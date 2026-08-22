use super::*;
use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::time::timeout;

struct TestActor {
    runs: Arc<AtomicUsize>,
    first_run_panics: bool,
    first_run_exits: bool,
}

impl SupervisedActor for TestActor {
    type Error = Infallible;

    fn name(&self) -> &'static str {
        "test"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let runs = self.runs.clone();
        let first_run_panics = self.first_run_panics;
        let first_run_exits = self.first_run_exits;

        async move {
            let run = runs.fetch_add(1, Ordering::SeqCst);

            if run == 0 && first_run_panics {
                panic!("test actor panic");
            }

            if run == 0 && first_run_exits {
                return Ok(());
            }

            tripwire.await;
            Ok(())
        }
    }
}

struct StatefulTestActor {
    runs: Arc<AtomicUsize>,
    delivered: Arc<AtomicUsize>,
    receiver: tokio::sync::mpsc::Receiver<u8>,
}

impl SupervisedActor for StatefulTestActor {
    type Error = Infallible;

    fn name(&self) -> &'static str {
        "stateful-test"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let runs = self.runs.clone();
        let delivered = self.delivered.clone();
        let receiver = &mut self.receiver;

        async move {
            let run = runs.fetch_add(1, Ordering::SeqCst);
            let message = receiver.recv().await.expect("test inbox closed");

            if run == 0 {
                assert_eq!(message, 1);
                panic!("stateful actor panic");
            }

            assert_eq!(message, 2);
            delivered.fetch_add(1, Ordering::SeqCst);

            tripwire.await;
            Ok(())
        }
    }
}

struct EscalatingActor {
    runs: Arc<AtomicUsize>,
    escalations: Arc<AtomicUsize>,
    restarts: Arc<AtomicUsize>,
}

impl SupervisedActor for EscalatingActor {
    type Error = Infallible;

    fn name(&self) -> &'static str {
        "escalating-test"
    }

    fn run(&mut self, _tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let runs = self.runs.clone();

        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            panic!("escalating actor panic");
        }
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Panicked => RestartDirective::Escalate,
            RestartReason::Completed | RestartReason::Failed => RestartDirective::Restart,
        }
    }

    fn on_restart(&mut self, _reason: RestartReason) {
        self.restarts.fetch_add(1, Ordering::SeqCst);
    }

    fn on_escalate(&mut self, reason: RestartReason) {
        assert_eq!(reason, RestartReason::Panicked);
        self.escalations.fetch_add(1, Ordering::SeqCst);
    }
}

struct StopOnCompletionActor {
    runs: Arc<AtomicUsize>,
}

impl SupervisedActor for StopOnCompletionActor {
    type Error = Infallible;

    fn name(&self) -> &'static str {
        "stop-on-completion"
    }

    fn run(&mut self, _tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.runs.fetch_add(1, Ordering::SeqCst);

        async { Ok(()) }
    }

    fn restart_directive(&mut self, reason: RestartReason) -> RestartDirective {
        match reason {
            RestartReason::Completed => RestartDirective::Stop,
            RestartReason::Failed | RestartReason::Panicked => RestartDirective::Restart,
        }
    }
}

struct AlwaysPanickingActor {
    runs: Arc<AtomicUsize>,
    restarts: Arc<AtomicUsize>,
    escalations: Arc<AtomicUsize>,
}

impl SupervisedActor for AlwaysPanickingActor {
    type Error = Infallible;

    fn name(&self) -> &'static str {
        "always-panicking"
    }

    fn run(&mut self, _tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let runs = self.runs.clone();

        async move {
            runs.fetch_add(1, Ordering::SeqCst);
            panic!("always-panicking actor panic");
        }
    }

    fn on_restart(&mut self, reason: RestartReason) {
        assert_eq!(reason, RestartReason::Panicked);
        self.restarts.fetch_add(1, Ordering::SeqCst);
    }

    fn on_escalate(&mut self, reason: RestartReason) {
        assert_eq!(reason, RestartReason::Panicked);
        self.escalations.fetch_add(1, Ordering::SeqCst);
    }
}

struct StableResetActor {
    runs: Arc<AtomicUsize>,
}

impl SupervisedActor for StableResetActor {
    type Error = Infallible;

    fn name(&self) -> &'static str {
        "stable-reset"
    }

    fn run(&mut self, tripwire: Tripwire) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let runs = self.runs.clone();

        async move {
            match runs.fetch_add(1, Ordering::SeqCst) {
                0 => panic!("first short run"),
                1 => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    panic!("stable run ended");
                }
                _ => tripwire.await,
            }

            Ok(())
        }
    }
}

fn test_policy() -> RestartPolicy {
    RestartPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(10),
        Duration::from_secs(1),
    )
}

fn spawn_test_supervised<A>(actor: A, tripwire: Tripwire, policy: RestartPolicy) -> JoinHandle<()>
where
    A: SupervisedActor,
{
    spawn_supervised_with_escalation(actor, tripwire, policy, |_| {})
}

async fn wait_for_runs(runs: &AtomicUsize, expected: usize) {
    timeout(Duration::from_secs(1), async {
        while runs.load(Ordering::SeqCst) < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor did not restart");
}

async fn shutdown(
    tx: tokio::sync::mpsc::Sender<()>,
    worker: tokio::task::JoinHandle<()>,
    supervisor: JoinHandle<()>,
) {
    tx.send(()).await.expect("tripwire sender closed");
    worker.await.expect("tripwire worker panicked");
    supervisor.await.expect("supervisor panicked");
}

#[test]
fn restart_backoff_grows_and_caps() {
    let policy = RestartPolicy::new(
        Duration::from_millis(10),
        Duration::from_millis(40),
        Duration::from_secs(1),
    );

    let (delay, next) =
        restart_schedule(Duration::from_millis(10), policy, Duration::from_millis(1));

    assert_eq!(delay, Duration::from_millis(10));
    assert_eq!(next, Duration::from_millis(20));

    let (delay, next) = restart_schedule(next, policy, Duration::from_millis(1));

    assert_eq!(delay, Duration::from_millis(20));
    assert_eq!(next, Duration::from_millis(40));

    let (delay, next) = restart_schedule(next, policy, Duration::from_millis(1));

    assert_eq!(delay, Duration::from_millis(40));
    assert_eq!(next, Duration::from_millis(40));
}

#[test]
fn restart_backoff_resets_after_stable_run() {
    let policy = RestartPolicy::new(
        Duration::from_millis(10),
        Duration::from_millis(80),
        Duration::from_secs(1),
    );

    let (delay, next) = restart_schedule(Duration::from_millis(80), policy, Duration::from_secs(1));

    assert_eq!(delay, Duration::from_millis(10));
    assert_eq!(next, Duration::from_millis(20));
}

#[tokio::test]
async fn escalate_directive_does_not_restart_actor() {
    let runs = Arc::new(AtomicUsize::new(0));
    let escalations = Arc::new(AtomicUsize::new(0));
    let restarts = Arc::new(AtomicUsize::new(0));
    let node_escalations = Arc::new(AtomicUsize::new(0));
    let (tripwire, _worker, _tx) = Tripwire::new_simple();

    let node_escalations_for_handler = node_escalations.clone();
    let supervisor = spawn_supervised_with_escalation(
        EscalatingActor {
            runs: runs.clone(),
            escalations: escalations.clone(),
            restarts: restarts.clone(),
        },
        tripwire,
        test_policy(),
        move |_| {
            node_escalations_for_handler.fetch_add(1, Ordering::SeqCst);
        },
    );

    timeout(Duration::from_secs(1), supervisor)
        .await
        .expect("supervisor did not stop after escalation")
        .expect("supervisor panicked");

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(escalations.load(Ordering::SeqCst), 1);
    assert_eq!(node_escalations.load(Ordering::SeqCst), 1);
    assert_eq!(restarts.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn stop_directive_does_not_restart_actor() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (tripwire, _worker, _tx) = Tripwire::new_simple();

    let supervisor = spawn_test_supervised(
        StopOnCompletionActor { runs: runs.clone() },
        tripwire,
        test_policy(),
    );

    timeout(Duration::from_secs(1), supervisor)
        .await
        .expect("supervisor did not stop")
        .expect("supervisor panicked");

    assert_eq!(runs.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn restarts_actor_after_unexpected_exit() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (tripwire, worker, tx) = Tripwire::new_simple();
    let worker = tokio::spawn(worker);

    let supervisor = spawn_test_supervised(
        TestActor {
            runs: runs.clone(),
            first_run_panics: false,
            first_run_exits: true,
        },
        tripwire,
        test_policy(),
    );

    wait_for_runs(&runs, 2).await;
    shutdown(tx, worker, supervisor).await;

    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn restarts_actor_after_panic() {
    let runs = Arc::new(AtomicUsize::new(0));
    let (tripwire, worker, tx) = Tripwire::new_simple();
    let worker = tokio::spawn(worker);

    let supervisor = spawn_test_supervised(
        TestActor {
            runs: runs.clone(),
            first_run_panics: true,
            first_run_exits: false,
        },
        tripwire,
        test_policy(),
    );

    wait_for_runs(&runs, 2).await;
    shutdown(tx, worker, supervisor).await;

    assert_eq!(runs.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn preserves_actor_state_across_restart() {
    let runs = Arc::new(AtomicUsize::new(0));
    let delivered = Arc::new(AtomicUsize::new(0));
    let (inbox_tx, inbox_rx) = tokio::sync::mpsc::channel(2);

    inbox_tx
        .send(1)
        .await
        .expect("could not queue first message");
    inbox_tx
        .send(2)
        .await
        .expect("could not queue second message");

    let (tripwire, worker, tx) = Tripwire::new_simple();
    let worker = tokio::spawn(worker);

    let supervisor = spawn_test_supervised(
        StatefulTestActor {
            runs: runs.clone(),
            delivered: delivered.clone(),
            receiver: inbox_rx,
        },
        tripwire,
        test_policy(),
    );

    timeout(Duration::from_secs(1), async {
        while delivered.load(Ordering::SeqCst) < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("actor did not preserve its inbox across restart");

    shutdown(tx, worker, supervisor).await;

    assert_eq!(runs.load(Ordering::SeqCst), 2);
    assert_eq!(delivered.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shutdown_does_not_restart_or_escalate_actor() {
    let runs = Arc::new(AtomicUsize::new(0));
    let node_escalations = Arc::new(AtomicUsize::new(0));
    let (tripwire, worker, tx) = Tripwire::new_simple();
    let worker = tokio::spawn(worker);

    let node_escalations_for_handler = node_escalations.clone();
    let supervisor = spawn_supervised_with_escalation(
        TestActor {
            runs: runs.clone(),
            first_run_panics: false,
            first_run_exits: false,
        },
        tripwire,
        test_policy(),
        move |_| {
            node_escalations_for_handler.fetch_add(1, Ordering::SeqCst);
        },
    );

    wait_for_runs(&runs, 1).await;
    shutdown(tx, worker, supervisor).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(node_escalations.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn restart_limit_escalates_once() {
    let runs = Arc::new(AtomicUsize::new(0));
    let restarts = Arc::new(AtomicUsize::new(0));
    let actor_escalations = Arc::new(AtomicUsize::new(0));
    let node_escalations = Arc::new(AtomicUsize::new(0));
    let (tripwire, _worker, _tx) = Tripwire::new_simple();

    let node_escalations_for_handler = node_escalations.clone();
    let supervisor = spawn_supervised_with_escalation(
        AlwaysPanickingActor {
            runs: runs.clone(),
            restarts: restarts.clone(),
            escalations: actor_escalations.clone(),
        },
        tripwire,
        test_policy().with_max_restarts(2),
        move |_| {
            node_escalations_for_handler.fetch_add(1, Ordering::SeqCst);
        },
    );

    timeout(Duration::from_secs(1), supervisor)
        .await
        .expect("supervisor did not stop at restart limit")
        .expect("supervisor panicked");

    assert_eq!(runs.load(Ordering::SeqCst), 3);
    assert_eq!(restarts.load(Ordering::SeqCst), 2);
    assert_eq!(actor_escalations.load(Ordering::SeqCst), 1);
    assert_eq!(node_escalations.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stable_run_resets_restart_limit() {
    let runs = Arc::new(AtomicUsize::new(0));
    let node_escalations = Arc::new(AtomicUsize::new(0));
    let (tripwire, worker, tx) = Tripwire::new_simple();
    let worker = tokio::spawn(worker);

    let policy = RestartPolicy::new(
        Duration::from_millis(1),
        Duration::from_millis(10),
        Duration::from_millis(10),
    )
    .with_max_restarts(1);

    let node_escalations_for_handler = node_escalations.clone();
    let supervisor = spawn_supervised_with_escalation(
        StableResetActor { runs: runs.clone() },
        tripwire,
        policy,
        move |_| {
            node_escalations_for_handler.fetch_add(1, Ordering::SeqCst);
        },
    );

    wait_for_runs(&runs, 3).await;
    shutdown(tx, worker, supervisor).await;

    assert_eq!(runs.load(Ordering::SeqCst), 3);
    assert_eq!(node_escalations.load(Ordering::SeqCst), 0);
}
