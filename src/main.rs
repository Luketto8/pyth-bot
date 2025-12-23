use anyhow::{Context, anyhow};
use base64::Engine;
use clap::Parser;
use near_crypto::{InMemorySigner, SecretKey};
use near_gas::NearGas;
use near_jsonrpc_client::{
    JsonRpcClient,
    errors::JsonRpcError,
    methods::{
        query::{RpcQueryError, RpcQueryRequest},
        send_tx::RpcSendTransactionRequest,
        tx::{RpcTransactionError, RpcTransactionStatusRequest, TransactionInfo},
    },
};
use near_jsonrpc_primitives::types::query::QueryResponseKind;
use near_primitives::{
    action::Action,
    hash::CryptoHash,
    transaction::{SignedTransaction, Transaction, TransactionV0},
    types::{AccountId, BlockReference, Gas},
    views::{FinalExecutionStatus, QueryRequest, TxExecutionStatus},
};
use tracing::{debug, info, instrument, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

use serde::{Deserialize, ser::Serialize};
use serde_json::json;
use std::str::FromStr;

use std::time::Duration;
use tokio::time::Instant;

const MAX_POLL_INTERVAL: Duration = Duration::from_secs(5);
// /// Error types for RPC operations
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    /// Failed to query view method
    #[error("Failed to query view method: {0}")]
    ViewMethodError(#[from] JsonRpcError<RpcQueryError>),
    /// Failed to get access key data
    #[error("Failed to get access key data: {0}")]
    AccessKeyDataError(JsonRpcError<RpcQueryError>),
    /// Got wrong response kind from RPC
    #[error("Got wrong response kind from RPC: {0}")]
    WrongResponseKind(String),
    /// Failed to send transaction
    #[error("Failed to send transaction: {0}")]
    SendTransactionError(#[from] JsonRpcError<RpcTransactionError>),
    /// Failed to deserialize response
    #[error("Failed to deserialize response: {0}")]
    DeserializeError(#[from] serde_json::Error),
    /// Other error
    #[error(transparent)]
    Other(#[from] anyhow::Error),
    /// Timeout exceeded
    #[error("Timeout exceeded: {0}")]
    TimeoutError(u64, u64),
    /// No outcome for transaction
    #[error("No outcome for transaction: {0}")]
    NoOutcome(String),
}
pub type RpcResult<T = ()> = std::result::Result<T, RpcError>;

#[derive(Debug, Clone, Parser)]
#[command(
    author,
    version,
    about = "Fetch Pyth update off-chain and push on-chain"
)]
struct Args {
    /// Comma-separated 32-byte hex price feed ids (no 0x)
    #[arg(long, env = "IDS", value_delimiter = ',', required = true)]
    ids: Vec<String>,

    /// Hermes endpoint
    #[arg(
        long,
        default_value = "https://hermes-beta.pyth.network/v2/updates/price/latest"
    )]
    hermes: String,

    /// Oracle contract account id
    #[arg(long, default_value = "pyth-oracle.testnet")]
    oracle: String,

    /// Oracle method
    #[arg(long, default_value = "update_price_feeds")]
    method: String,

    /// NEAR RPC endpoint
    #[arg(long, default_value = "https://rpc.testnet.near.org")]
    rpc: String,

    /// Signer account id
    #[arg(long, env = "SIGNER_ID")]
    signer_id: String,

    /// Signer secret key (ed25519:...)
    #[arg(long, env = "SIGNER_SK")]
    signer_sk: String,

    /// Gas in Tgas
    #[arg(long, default_value_t = 300)]
    gas_tgas: u64,

    /// Deposit in yoctoNEAR (often 0 or 1)
    #[arg(long, default_value_t = 0)]
    deposit_yocto: u128,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(crate = "serde")]
struct HermesBinary {
    data: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(crate = "serde")]
struct HermesResp {
    binary: HermesBinary,
}

fn gas_from_tgas(t: u64) -> Gas {
    NearGas::from_tgas(t).as_gas()
}

#[allow(clippy::expect_used, reason = "We know the serialization will succeed")]
pub fn serialize_and_encode(data: impl Serialize) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .encode(serde_json::to_string(&data).expect("Failed to serialize data"))
        .into_bytes()
}

async fn fetch_prices(hermes: &str, ids: &[String]) -> anyhow::Result<String> {
    let client = reqwest::Client::new();
    let mut url = reqwest::Url::parse(hermes)?;
    {
        let mut qp = url.query_pairs_mut();
        for id in ids {
            qp.append_pair("ids[]", id);
        }
    }
    let resp: HermesResp = client
        .get(url.clone())
        .send()
        .await
        .context("request Hermes")?
        .error_for_status()
        .context("non-200 Hermes")?
        .json()
        .await
        .context("decode Hermes JSON")?;

    let out = resp.binary.data.join("");
    if out.is_empty() {
        return Err(anyhow!("Hermes payload empty"));
    }
    Ok(out)
}

#[instrument(skip(client), level = "debug")]
pub async fn get_access_key_data(
    client: &JsonRpcClient,
    signer: &InMemorySigner,
) -> RpcResult<(u64, CryptoHash)> {
    let access_key_query_response = client
        .call(RpcQueryRequest {
            block_reference: BlockReference::latest(),
            request: QueryRequest::ViewAccessKey {
                account_id: signer.account_id.clone(),
                public_key: signer.public_key().clone(),
            },
        })
        .await
        .map_err(RpcError::AccessKeyDataError)?;

    let nonce = match access_key_query_response.kind {
        QueryResponseKind::AccessKey(access_key) => access_key.nonce + 1,
        _ => {
            return Err(RpcError::WrongResponseKind(format!(
                "Expected AccessKey got {:?}",
                access_key_query_response.kind
            )));
        }
    };
    let block_hash = access_key_query_response.block_hash;

    Ok((nonce, block_hash))
}

fn create_transfer_tx(
    nonce: u64,
    block_hash: CryptoHash,
    receiver: AccountId,
    signer: &InMemorySigner,
    args: Vec<u8>,
) -> RpcResult<Transaction> {
    let function_call = near_primitives::action::FunctionCallAction {
        method_name: "update_price_feeds".to_string(),
        args,
        gas: gas_from_tgas(300),
        deposit: 0,
    };
    Ok(Transaction::V0(TransactionV0 {
        nonce,
        receiver_id: receiver,
        block_hash,
        signer_id: signer.account_id.clone(),
        public_key: signer.public_key().clone(),
        actions: vec![Action::FunctionCall(Box::new(function_call))],
    }))
}

pub async fn send_tx(
    client: &JsonRpcClient,
    signer: &InMemorySigner,
    timeout: u64,
    tx: Transaction,
) -> RpcResult<FinalExecutionStatus> {
    let (tx_hash, _size) = tx.get_hash_and_size();

    let called_at = Instant::now();
    let signature = signer.sign(tx_hash.as_ref());
    let deadline = called_at + Duration::from_secs(timeout);
    let result = match client
        .call(RpcSendTransactionRequest {
            signed_transaction: SignedTransaction::new(signature, tx),
            wait_until: TxExecutionStatus::Final,
        })
        .await
    {
        Ok(res) => res,
        Err(e) => {
            loop {
                if !matches!(e.handler_error(), Some(RpcTransactionError::TimeoutError)) {
                    return Err(e.into());
                }

                // Poll with exponential backoff
                let mut poll_interval = Duration::from_millis(500);

                loop {
                    if Instant::now() >= deadline {
                        return Err(RpcError::TimeoutError(
                            timeout,
                            called_at.elapsed().as_secs(),
                        ));
                    }

                    tokio::time::sleep(poll_interval).await;

                    // Exponential backoff
                    poll_interval = std::cmp::min(poll_interval * 2, MAX_POLL_INTERVAL);

                    let status = client
                        .call(RpcTransactionStatusRequest {
                            transaction_info: TransactionInfo::TransactionId {
                                sender_account_id: signer.account_id.clone(),
                                tx_hash,
                            },
                            wait_until: TxExecutionStatus::Final,
                        })
                        .await;

                    let Err(e) = status else {
                        break;
                    };

                    if !matches!(e.handler_error(), Some(RpcTransactionError::TimeoutError)) {
                        return Err(e.into());
                    }
                }
            }
        }
    };

    let Some(outcome) = result.final_execution_outcome else {
        return Err(RpcError::NoOutcome(tx_hash.to_string()));
    };

    Ok(outcome.into_outcome().status)
}

#[tokio::main]
async fn main() -> RpcResult<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let signer_id = AccountId::from_str(&args.signer_id)
        .context("parse signer account id")?;
    let sk = SecretKey::from_str(&args.signer_sk)
        .context("parse signer secret key")?;
    let signer = InMemorySigner::from_secret_key(signer_id.clone(), sk);
    let oracle = AccountId::from_str(&args.oracle)
        .context("parse oracle account id")?;
    let client = JsonRpcClient::connect(&args.rpc);

    info!("Starting pushing prices on-chain");

    loop {
        if let Err(e) = async {
            let joined_hex = fetch_prices(&args.hermes, &args.ids).await?;

            let call_args = json!({ "data": joined_hex });
            let args_bytes = serde_json::to_vec(&call_args).map_err(RpcError::DeserializeError)?; // or anyhow!(...) if you prefer
            info!("Prepared payload for update_price_feeds");

            let (nonce, block_hash) = get_access_key_data(&client, &signer).await?;
            debug!("nonce={}, block_hash={}", nonce, block_hash);

            let tx = create_transfer_tx(nonce, block_hash, oracle.clone(), &signer, args_bytes)?;

            match send_tx(&client, &signer, 60, tx).await {
                Ok(status) => match status {
                    FinalExecutionStatus::SuccessValue(_) => {
                        info!("Price update succeeded: {:?}", status);
                    }
                    other => {
                        warn!("Price update not successful: {:?}", other);
                    }
                },
                Err(e) => {
                    // Network or RPC error—log and continue next cycle
                    warn!("Broadcast failed: {e}");
                }
            }

            Ok::<_, RpcError>(())
        }
        .await
        {
            // Any error from the inner block is logged here
            warn!("Cycle error: {e:#}");
        }

        info!("Waiting 55 seconds before next update...");
        tokio::time::sleep(Duration::from_secs(55)).await;
    }
}
