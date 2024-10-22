use crate::agent::SplitPool;
use crate::pubsub::{
    unpack_columns, MatchCandidates, MatchableChange, MatcherCreated, MatcherError,
};
use crate::schema::Schema;
use async_trait::async_trait;
use corro_api_types::{Change, ColumnName, QueryEvent, SqliteValueRef, TableName};
use corro_base_types::CrsqlDbVersion;
use metrics::{counter, histogram};
use parking_lot::RwLock;
use rusqlite::Connection;
use spawn::spawn_counted;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::block_in_place;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tracing::{debug, error, info, trace, warn};
use tripwire::Tripwire;
use uuid::Uuid;

#[derive(Debug, Default, Clone)]
pub struct UpdatesManager(Arc<RwLock<InnerUpdatesManager>>);

pub trait Manager {
    fn trait_type(&self) -> String;
    fn get(&self, id: &Uuid) -> Option<Box<dyn Handle>>;
    fn remove(&self, id: &Uuid) -> Option<Box<dyn Handle + Send>>;
    fn get_handles(&self) -> BTreeMap<Uuid, Box<dyn Handle>>;
}

#[async_trait]
pub trait Handle {
    fn id(&self) -> Uuid;
    fn hash(&self) -> &str;
    fn cancelled(&self) -> WaitForCancellationFuture;
    fn filter_matchable_change(
        &self,
        candidates: &mut MatchCandidates,
        change: MatchableChange,
    ) -> bool;
    fn changes_tx(&self) -> mpsc::Sender<(MatchCandidates, CrsqlDbVersion)>;
    async fn cleanup(&self);
}

impl Manager for UpdatesManager {
    fn trait_type(&self) -> String {
        "updates".to_string()
    }

    fn get(&self, id: &Uuid) -> Option<Box<dyn Handle>> {
        self.0
            .read()
            .get(id)
            .map(|h| Box::new(h) as Box<dyn Handle>)
    }

    fn remove(&self, id: &Uuid) -> Option<Box<dyn Handle + Send>> {
        let mut inner = self.0.write();
        inner
            .remove(id)
            .map(|h| Box::new(h) as Box<dyn Handle + Send>)
    }

    fn get_handles(&self) -> BTreeMap<Uuid, Box<dyn Handle>> {
        let handles = { self.0.read().handles.clone() };
        handles
            .into_iter()
            .map(|(id, x)| (id, Box::new(x) as Box<dyn Handle>))
            .collect()
    }
}

#[async_trait]
impl Handle for UpdateHandle {
    fn id(&self) -> Uuid {
        self.inner.id
    }

    fn hash(&self) -> &str {
        &self.inner.name
    }

    fn cancelled(&self) -> WaitForCancellationFuture {
        self.inner.cancel.cancelled()
    }

    fn filter_matchable_change(
        &self,
        candidates: &mut MatchCandidates,
        change: MatchableChange,
    ) -> bool {
        if change.table.to_string() != self.inner.name {
            return false;
        }

        trace!("filtering change {change:?}");
        // don't double process the same pk
        if candidates
            .get(change.table)
            .map(|pks| pks.contains(change.pk))
            .unwrap_or_default()
        {
            trace!("already contained key");
            return false;
        }

        if let Some(v) = candidates.get_mut(change.table) {
            v.insert(change.pk.to_vec())
        } else {
            candidates.insert(change.table.clone(), [change.pk.to_vec()].into());
            true
        }
    }

    fn changes_tx(&self) -> mpsc::Sender<(MatchCandidates, CrsqlDbVersion)> {
        return self.inner.changes_tx.clone();
    }

    async fn cleanup(&self) {
        // self.inner.cancel.cancel();
        info!(sub_id = %self.inner.id, "Canceled subscription");
    }
}

const UPDATE_EVENT_CHANNEL_CAP: usize = 512;

#[derive(Debug, Default)]
struct InnerUpdatesManager {
    tables: BTreeMap<String, Uuid>,
    handles: BTreeMap<Uuid, UpdateHandle>,
}

impl InnerUpdatesManager {
    fn remove(&mut self, id: &Uuid) -> Option<UpdateHandle> {
        let handle = self.handles.remove(id)?;
        Some(handle)
    }

    fn get(&self, id: &Uuid) -> Option<UpdateHandle> {
        self.handles.get(id).cloned()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpdateError {
    #[error(transparent)]
    Lexer(#[from] sqlite3_parser::lexer::sql::Error),
    #[error("one statement is required for matching")]
    StatementRequired,
    #[error("unsupported statement")]
    UnsupportedStatement,
}

impl UpdatesManager {
    pub fn get(&self, table: &str) -> Option<UpdateHandle> {
        let id = self.0.read().tables.get(table).cloned();
        if let Some(id) = id {
            return self.0.read().handles.get(&id).cloned();
        }
        None
    }

    pub fn get_or_insert(
        &self,
        tbl_name: &str,
        schema: &Schema,
        _pool: &SplitPool,
        tripwire: Tripwire,
    ) -> Result<(UpdateHandle, Option<MatcherCreated>), MatcherError> {
        if let Some(handle) = self.get(tbl_name) {
            return Ok((handle, None));
        }

        let mut inner = self.0.write();
        let (evt_tx, evt_rx) = mpsc::channel(UPDATE_EVENT_CHANNEL_CAP);

        let id = Uuid::new_v4();
        let handle_res = UpdateHandle::create(id, tbl_name, schema, evt_tx, tripwire);

        let handle = match handle_res {
            Ok(handle) => handle,
            Err(e) => {
                return Err(e);
            }
        };

        inner.handles.insert(id, handle.clone());
        inner.tables.insert(tbl_name.to_string(), id);

        Ok((handle, Some(MatcherCreated { evt_rx })))
    }

    pub fn remove(&self, id: &Uuid) -> Option<UpdateHandle> {
        let mut inner = self.0.write();

        inner.remove(id)
    }
}

#[derive(Clone, Debug)]
pub struct UpdateHandle {
    inner: Arc<InnerUpdateHandle>,
}

#[derive(Clone, Debug)]
pub struct InnerUpdateHandle {
    id: Uuid,
    name: String,
    cancel: CancellationToken,
    changes_tx: mpsc::Sender<(MatchCandidates, CrsqlDbVersion)>,
}

impl UpdateHandle {
    pub fn id(&self) -> Uuid {
        return self.inner.id;
    }
    pub fn create(
        id: Uuid,
        tbl_name: &str,
        schema: &Schema,
        evt_tx: mpsc::Sender<QueryEvent>,
        tripwire: Tripwire,
    ) -> Result<UpdateHandle, MatcherError> {
        // check for existing handles
        match schema.tables.get(tbl_name) {
            Some(table) => {
                if table.pk.is_empty() {
                    return Err(MatcherError::MissingPrimaryKeys);
                }
            }
            None => return Err(MatcherError::TableNotFound(tbl_name.to_string())),
        };

        let cancel = CancellationToken::new();
        let (changes_tx, changes_rx) = mpsc::channel(20480);
        let handle = UpdateHandle {
            inner: Arc::new(InnerUpdateHandle {
                id,
                name: tbl_name.to_owned(),
                cancel: cancel.clone(),
                changes_tx,
            }),
        };
        spawn_counted(cmd_loop(id, cancel, evt_tx, changes_rx, tripwire));
        Ok(handle)
    }

    pub async fn cleanup(self) {
        self.inner.cancel.cancel();
        info!(sub_id = %self.inner.name, "Canceled subscription");
    }
}

fn handle_candidates(
    evt_tx: mpsc::Sender<QueryEvent>,
    candidates: MatchCandidates,
) -> Result<(), MatcherError> {
    if candidates.is_empty() {
        return Ok(());
    }

    trace!(
        "got some candidates! {:?}",
        candidates.keys().collect::<Vec<_>>()
    );

    for (table, pks) in candidates {
        let pks = pks
            .iter()
            .map(|pk| unpack_columns(pk))
            .collect::<Result<Vec<Vec<SqliteValueRef>>, _>>()?;

        for pk in pks {
            if let Err(e) = evt_tx.blocking_send(QueryEvent::Notify(
                table.clone(),
                pk.iter().map(|x| x.to_owned()).collect::<Vec<_>>(),
            )) {
                debug!("could not send back row to matcher sub sender: {e}");
                return Err(MatcherError::EventReceiverClosed);
            }
        }
    }

    return Ok(());
}

// async fn cmd_loop(
//     id: Uuid,
//     evt_rx: mpsc::Sender<QueryEvent>,
//     mut changes_rx: mpsc::Receiver<(MatchCandidates, CrsqlDbVersion)>,
//     mut tripwire: Tripwire,
// ) {
//     loop {
//         tokio::select! {
//             Some((candidates, _)) = changes_rx.recv() => {
//                 // todo(s): handle error properly
//                 if let Err(e) = block_in_place(|| handle_candidates(evt_rx.clone(), candidates)) {
//                     if !matches!(e, MatcherError::EventReceiverClosed) {
//                         error!(id = %id, "could not handle change: {e}");
//                     }
//                     break;
//                 }
//             }
//             _ = &mut tripwire => {
//                 trace!(sub_id = %id, "tripped cmd_loop, returning");
//                 // just return!
//                 return;
//             }
//         }
//     }
//     info!("changes channel closed! exiting.");
// }

async fn cmd_loop(
    id: Uuid,
    cancel: CancellationToken,
    evt_rx: mpsc::Sender<QueryEvent>,
    mut changes_rx: mpsc::Receiver<(MatchCandidates, CrsqlDbVersion)>,
    mut tripwire: Tripwire,
) {
    const PROCESS_CHANGES_THRESHOLD: usize = 1000;
    const PROCESS_BUFFER_DEADLINE: Duration = Duration::from_millis(600);

    info!(sub_id = %id, "Starting loop to run the subscription");
    // todo(somtochiama): do we need to have some state like MatcherRunning?

    let mut buf = MatchCandidates::new();
    let mut buf_count = 0;

    // max duration of aggregating candidates
    let process_changes_deadline = tokio::time::sleep(PROCESS_BUFFER_DEADLINE);
    tokio::pin!(process_changes_deadline);

    let mut process = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(sub_id = %id, "Acknowledged subscription cancellation, breaking loop.");
                break;
            }
            Some((candidates, _)) = changes_rx.recv() => {
                for (table, pks) in  candidates {
                    let buffed = buf.entry(table).or_default();
                    for pk in pks {
                        if buffed.insert(pk) {
                            buf_count += 1;
                        }
                    }
                }

                if buf_count >= PROCESS_CHANGES_THRESHOLD {
                   process = true
                }
            },
            _ = process_changes_deadline.as_mut() => {
                process_changes_deadline
                    .as_mut()
                    .reset((Instant::now() + PROCESS_BUFFER_DEADLINE).into());
                if buf_count != 0 {
                    process = true
                }
            },
            _ = &mut tripwire => {
                trace!(sub_id = %id, "tripped cmd_loop, returning");
                // just return!
                return;
            }
            else => {
                return;
            }
        }

        if process {
            let start = Instant::now();
            if let Err(e) =
                block_in_place(|| handle_candidates(evt_rx.clone(), std::mem::take(&mut buf)))
            {
                if !matches!(e, MatcherError::EventReceiverClosed) {
                    error!(sub_id = %id, "could not handle change: {e}");
                }
                break;
            }
            let elapsed = start.elapsed();

            histogram!("corro.updates.changes.processing.duration.seconds", "table" => id.to_string()).record(elapsed);

            buf_count = 0;

            // reset the deadline
            process_changes_deadline
                .as_mut()
                .reset((Instant::now() + PROCESS_BUFFER_DEADLINE).into());
        }
    }

    debug!(id = %id, "matcher loop is done");
}

pub fn match_changes(manager: &impl Manager, changes: &[Change], db_version: CrsqlDbVersion) {
    let trait_type = manager.trait_type();
    trace!(
        %db_version,
        "trying to match changes to {trait_type}, len: {}",
        changes.len()
    );
    if changes.is_empty() {
        return;
    }

    let handles = manager.get_handles();
    if handles.is_empty() {
        return;
    }

    for (id, handle) in handles.iter() {
        trace!(sub_id = %id, %db_version, "attempting to match changes to a subscription");
        let mut candidates = MatchCandidates::new();
        let mut match_count = 0;
        for change in changes.iter().map(MatchableChange::from) {
            if handle.filter_matchable_change(&mut candidates, change) {
                match_count += 1;
            }
        }

        // metrics...
        for (table, pks) in candidates.iter() {
            counter!("corro.subs.changes.updates.count", "table" => table.to_string())
                .increment(pks.len() as u64);
        }

        trace!(sub_id = %id, %db_version, "found {match_count} candidates");

        if let Err(e) = handle.changes_tx().try_send((candidates, db_version)) {
            error!(sub_id = %id, "could not send change candidates to subscription handler: {e}");
            match e {
                mpsc::error::TrySendError::Full(item) => {
                    warn!("channel is full, falling back to async send");

                    let changes_tx = handle.changes_tx().clone();
                    tokio::spawn(async move {
                        _ = changes_tx.send(item).await;
                    });
                }
                mpsc::error::TrySendError::Closed(_) => {
                    if let Some(handle) = manager.remove(id) {
                        tokio::spawn(async move {
                            handle.cleanup().await;
                        });
                    }
                }
            }
        }
    }
}

pub fn match_changes_from_db_version(
    manager: &impl Manager,
    conn: &Connection,
    db_version: CrsqlDbVersion,
) -> rusqlite::Result<()> {
    let handles = manager.get_handles();

    let mut candidates = handles
        .iter()
        .map(|(id, handle)| (id, (MatchCandidates::new(), handle)))
        .collect::<BTreeMap<_, _>>();

    {
        let mut prepped = conn.prepare_cached(
            r#"
        SELECT "table", pk, cid
            FROM crsql_changes
            WHERE db_version = ?
            ORDER BY seq ASC
        "#,
        )?;

        let rows = prepped.query_map([db_version], |row| {
            Ok((
                row.get::<_, TableName>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, ColumnName>(2)?,
            ))
        })?;

        for change_res in rows {
            let (table, pk, column) = change_res?;

            for (_id, (candidates, handle)) in candidates.iter_mut() {
                let change = MatchableChange {
                    table: &table,
                    pk: &pk,
                    column: &column,
                };
                handle.filter_matchable_change(candidates, change);
            }
        }
    }

    // metrics...
    for (id, (candidates, handle)) in candidates {
        let mut match_count = 0;

        for (table, pks) in candidates.iter() {
            let count = pks.len();
            match_count += count;
            counter!("corro.subs.changes.matched.count", "sql_hash" => handle.hash().clone(), "table" => table.to_string()).increment(count as u64);
        }

        trace!(sub_id = %id, %db_version, "found {match_count} candidates");

        if let Err(e) = handle.changes_tx().try_send((candidates, db_version)) {
            error!(sub_id = %id, "could not send change candidates to subscription handler: {e}");
            match e {
                mpsc::error::TrySendError::Full(item) => {
                    warn!("channel is full, falling back to async send");
                    let changes_tx = handle.changes_tx();
                    tokio::spawn(async move {
                        _ = changes_tx.send(item).await;
                    });
                }
                mpsc::error::TrySendError::Closed(_) => {
                    if let Some(handle) = manager.remove(id) {
                        tokio::spawn(async move {
                            handle.cleanup().await;
                        });
                    }
                }
            }
        }
    }

    Ok(())
}
