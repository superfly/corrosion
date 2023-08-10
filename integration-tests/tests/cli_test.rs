use assert_cmd::Command;
use corro_tests::launch_test_agent;
use spawn::wait_for_all_pending_handles;
use tripwire::Tripwire;

#[test]
fn test_help() {
    let mut cmd = Command::cargo_bin("corrosion").unwrap();

    cmd.arg("--help").assert().success();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query() {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta = launch_test_agent(|conf| conf.build(), tripwire.clone())
        .await
        .unwrap();

    let mut cmd = Command::cargo_bin("corrosion").unwrap();

    let api_addr = ta.agent.api_addr();

    let expected = ta.agent.actor_id().as_simple().to_string().to_uppercase();

    let assert = cmd
        .arg("--api-addr")
        .arg(api_addr.to_string())
        .arg("query")
        .arg("SELECT hex(site_id) FROM crsql_site_id WHERE ordinal = 0")
        .assert();

    assert.success().stdout(format!("{expected}\n"));

    tripwire_tx.send(()).await.ok();
    tripwire_worker.await;
    wait_for_all_pending_handles().await;
}