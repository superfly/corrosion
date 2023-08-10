use assert_cmd::prelude::OutputAssertExt;
use corro_tests::launch_test_agent;
use escargot::CargoRun;
use once_cell::sync::Lazy;
use spawn::wait_for_all_pending_handles;
use tripwire::Tripwire;

static CORROSION_BIN: Lazy<CargoRun> = Lazy::new(|| {
    escargot::CargoBuild::new()
        .bin("corrosion")
        .current_release()
        // .current_target()
        .manifest_path("../Cargo.toml")
        .run()
        .unwrap()
});

#[test]
fn test_help() {
    let mut cmd = CORROSION_BIN.command();

    cmd.arg("--help").assert().success();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_query() {
    _ = tracing_subscriber::fmt::try_init();
    let (tripwire, tripwire_worker, tripwire_tx) = Tripwire::new_simple();
    let ta = launch_test_agent(|conf| conf.build(), tripwire.clone())
        .await
        .unwrap();

    let mut cmd = CORROSION_BIN.command();

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
