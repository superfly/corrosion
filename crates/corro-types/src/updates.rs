use crate::actor::ActorId;
use crate::agent::SplitPool;
use crate::broadcast::Changeset;
use crate::pubsub::{unpack_columns, MatchCandidates, MatchableChange, MatcherError};
use crate::schema::Schema;
use antithesis_sdk::assert_sometimes;
use async_trait::async_trait;
use corro_api_types::sqlite::ChangeType;
use corro_api_types::{ColumnName, NotifyEvent, TableName};
use corro_base_types::CrsqlDbVersion;
use indexmap::{map::Entry, IndexMap};
use metrics::{counter, histogram, Counter};
use parking_lot::RwLock;
use rusqlite::Connection;
use spawn::spawn_counted;
use std::collections::BTreeMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};
use tracing::{debug, error, info, trace, warn};
use tripwire::Tripwire;
use uuid::Uuid;

pub trait Manager<H> {
    fn trait_type(&self) -> String;
    fn get(&self, id: &Uuid) -> Option<H>;
    fn remove(&self, id: &Uuid) -> Option<H>;
    fn get_handles(&self) -> BTreeMap<Uuid, H>;
}

#[async_trait]
pub trait Handle {
    fn id(&self) -> Uuid;
    fn cancelled(&self) -> WaitForCancellationFuture<'_>;
    fn filter_matchable_change(
        &self,
        candidates: &mut MatchCandidates,
        change: MatchableChange,
    ) -> bool;
    fn changes_tx(&self) -> mpsc::Sender<MatchCandidates>;
    async fn cleanup(&self);
    fn get_counter(&self, table: &str) -> &HandleMetrics;
}

#[derive(Clone)]
pub struct HandleMetrics {
    pub matched_count: Counter,
}

impl std::fmt::Debug for HandleMetrics {
    fn fmt(&self, f: &mut Formatter) -> Result<(), std::fmt::Error> {
        f.debug_struct("Counter").finish_non_exhaustive()
    }
}

#[derive(Default, Debug, Clone)]
pub struct UpdatesManager(Arc<RwLock<InnerUpdatesManager>>);

impl Manager<UpdateHandle> for UpdatesManager {
    fn trait_type(&self) -> String {
        "updates".to_string()
    }

    fn get(&self, id: &Uuid) -> Option<UpdateHandle> {
        self.0.read().get(id)
    }

    fn remove(&self, id: &Uuid) -> Option<UpdateHandle> {
        let mut inner = self.0.write();
        inner.remove(id)
    }

    fn get_handles(&self) -> BTreeMap<Uuid, UpdateHandle> {
        self.0.read().handles.clone()
    }
}

#[async_trait]
impl Handle for UpdateHandle {
    fn id(&self) -> Uuid {
        self.inner.id
    }

    fn cancelled(&self) -> WaitForCancellationFuture<'_> {
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
            .map(|pks| pks.contains_key(change.pk))
            .unwrap_or_default()
        {
            trace!("already contained key");
            return false;
        }

        if let Some(v) = candidates.get_mut(change.table) {
            v.insert(change.pk.into(), change.cl).is_none()
        } else {
            candidates.insert(
                change.table.clone(),
                [(change.pk.to_vec(), change.cl)].into(),
            );
            true
        }
    }

    fn changes_tx(&self) -> mpsc::Sender<MatchCandidates> {
        self.inner.changes_tx.clone()
    }

    async fn cleanup(&self) {
        self.inner.cancel.cancel();
        info!(sub_id = %self.inner.id, "Canceled subscription");
    }

    fn get_counter(&self, table: &str) -> &HandleMetrics {
        // this should not happen
        if table != self.inner.name {
            warn!(update_tbl = %self.inner.name, "udpates handle get_counter method called for wrong table : {table}! This shouldn't happen!")
        }
        &self.inner.counters
    }
}

const UPDATE_EVENT_CHANNEL_CAP: usize = 512;

// tools to bootstrap a new notifier
pub struct UpdateCreated {
    pub evt_rx: mpsc::Receiver<NotifyEvent>,
}

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
    ) -> Result<(UpdateHandle, Option<UpdateCreated>), MatcherError> {
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

        Ok((handle, Some(UpdateCreated { evt_rx })))
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
    changes_tx: mpsc::Sender<MatchCandidates>,
    counters: HandleMetrics,
}

impl UpdateHandle {
    pub fn id(&self) -> Uuid {
        self.inner.id
    }

    pub fn create(
        id: Uuid,
        tbl_name: &str,
        schema: &Schema,
        evt_tx: mpsc::Sender<NotifyEvent>,
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
                counters: HandleMetrics {
                    matched_count: counter!("corro.updates.changes.matched.count", "table" => tbl_name.to_owned()),
                },
            }),
        };
        spawn_counted(batch_candidates(
            id,
            tbl_name.to_owned(),
            cancel,
            evt_tx,
            changes_rx,
            tripwire,
        ));
        Ok(handle)
    }

    pub async fn cleanup(self) {
        self.inner.cancel.cancel();
        info!(update_id = %self.inner.id, "Canceled update");
    }
}

type PendingChanges = IndexMap<(TableName, Vec<u8>), i64>;

// max number of distinct pks buffered for an updates client. When exceeded,
// the oldest pending changes are dropped in favor of newer ones.
const MAX_PENDING_CHANGES: usize = 3000;
const MAX_CACHE_ENTRIES: usize = 2000;
const KEEP_CACHE_ENTRIES: usize = 1000;

/// Merges candidates into the pending buffer, skipping changes older than what
/// we've already seen for a pk, and evicts the oldest entries once the buffer is full.
/// Returns the number of evicted entries.
fn buffer_candidates(
    buf: &mut PendingChanges,
    cl_cache: &mut PendingChanges,
    candidates: MatchCandidates,
) -> usize {
    for (table, pk_map) in candidates {
        for (pk, cl) in pk_map {
            match cl_cache.entry((table.clone(), pk.clone())) {
                Entry::Occupied(mut o) => {
                    if *o.get() > cl {
                        continue;
                    }
                    o.insert(cl);
                }
                Entry::Vacant(v) => {
                    v.insert(cl);
                }
            }

            buf.insert((table.clone(), pk), cl);
        }
    }

    if cl_cache.len() > MAX_CACHE_ENTRIES {
        *cl_cache = cl_cache.split_off(cl_cache.len() - KEEP_CACHE_ENTRIES);
    }

    let overflow = buf.len().saturating_sub(MAX_PENDING_CHANGES);
    buf.drain(..overflow);
    overflow
}

async fn batch_candidates(
    id: Uuid,
    table: String,
    cancel: CancellationToken,
    evt_tx: mpsc::Sender<NotifyEvent>,
    mut changes_rx: mpsc::Receiver<MatchCandidates>,
    mut tripwire: Tripwire,
) {
    const PROCESS_CHANGES_THRESHOLD: usize = 1000;
    const PROCESS_BUFFER_DEADLINE: Duration = Duration::from_millis(600);

    // small cache to ensure we don't send an older update when changes to the same pk
    // are made in quick succession. Changes aren't sent in order, and there's just one situation where this
    // can be problematic, when we have a delete before an update but those
    // get sent in reverse order. so the client might delete a key that's actually present in the db.
    //
    // (TODO: maybe we could send the cl to the client? or read from db?)
    let mut cl_cache = PendingChanges::new();

    info!(sub_id = %id, "Starting loop to receive candidates from updates");

    let dropped_count = counter!("corro.updates.changes.dropped.count", "table" => table);

    let mut buf = PendingChanges::new();

    // sending to the client happens in a separate task so we can keep draining
    // changes_rx (and drop old changes) while waiting on a slow receiver
    let mut send_task: Option<(Instant, JoinHandle<Result<(), MatcherError>>)> = None;

    // max duration of aggregating candidates
    let process_changes_deadline = tokio::time::sleep(PROCESS_BUFFER_DEADLINE);
    tokio::pin!(process_changes_deadline);

    let mut deadline_hit = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                info!(sub_id = %id, "Acknowledged updates cancellation, breaking loop.");
                break;
            }
            res = async { (&mut send_task.as_mut().unwrap().1).await }, if send_task.is_some() => {
                let (start, _) = send_task.take().unwrap();
                match res {
                    Ok(Ok(())) => {}
                    Ok(Err(MatcherError::EventReceiverClosed)) => break,
                    Ok(Err(e)) => {
                        error!(sub_id = %id, "could not handle change: {e}");
                        break;
                    }
                    Err(e) => {
                        error!(sub_id = %id, "updates send task failed: {e}");
                        break;
                    }
                }
                histogram!("corro.updates.changes.processing.duration.seconds", "table" => id.to_string()).record(start.elapsed());
            }
            Some(candidates) = changes_rx.recv() => {
                trace!(sub_id = %id, "updates got candidates: {candidates:?}");
                let dropped = buffer_candidates(&mut buf, &mut cl_cache, candidates);
                if dropped > 0 {
                    warn!(sub_id = %id, "updates buffer is full, dropped {dropped} oldest pending changes");
                    dropped_count.increment(dropped as u64);
                }
            },
            _ = process_changes_deadline.as_mut() => {
                process_changes_deadline
                    .as_mut()
                    .reset(Instant::now() + PROCESS_BUFFER_DEADLINE);
                deadline_hit = !buf.is_empty();
            },
            _ = &mut tripwire => {
                trace!(sub_id = %id, "tripped batch_candidates, returning");
                break;
            }
            else => {
                break;
            }
        }

        if send_task.is_none() && (deadline_hit || buf.len() >= PROCESS_CHANGES_THRESHOLD) {
            let changes = std::mem::take(&mut buf);
            let evt_tx = evt_tx.clone();
            let task = tokio::spawn(async move {
                for ((_, pk), cl) in changes {
                    let pk = unpack_columns(&pk)?
                        .iter()
                        .map(|x| x.to_owned())
                        .collect::<Vec<_>>();
                    let change_type = if cl % 2 == 0 {
                        ChangeType::Delete
                    } else {
                        ChangeType::Update
                    };
                    if let Err(e) = evt_tx.send(NotifyEvent::Notify(change_type, pk)).await {
                        debug!("could not send back row to matcher sub sender: {e}");
                        return Err(MatcherError::EventReceiverClosed);
                    }
                }
                Ok(())
            });
            send_task = Some((Instant::now(), task));
            deadline_hit = false;

            // reset the deadline
            process_changes_deadline
                .as_mut()
                .reset(Instant::now() + PROCESS_BUFFER_DEADLINE);
        }
    }

    if let Some((_, task)) = send_task {
        task.abort();
    }

    debug!(id = %id, "update loop is done");
}

pub fn match_changes<H>(manager: &impl Manager<H>, changes: &Changeset, db_version: CrsqlDbVersion)
where
    H: Handle + Send + 'static,
{
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

    assert_sometimes!(true, "Corrosion matches changes for updates");
    for (id, handle) in handles.iter() {
        trace!(sub_id = %id, %db_version, "attempting to match changes to a subscription");
        let mut candidates = MatchCandidates::new();
        let mut match_count = 0;
        for change in changes.matchable_changes() {
            if handle.filter_matchable_change(&mut candidates, change) {
                match_count += 1;
            }
        }

        // metrics...
        for (table, pks) in candidates.iter() {
            handle
                .get_counter(table)
                .matched_count
                .increment(pks.len() as u64);
        }

        trace!(sub_id = %id, %db_version, "found {match_count} candidates");

        if let Err(e) = handle.changes_tx().try_send(candidates) {
            error!(sub_id = %id, "could not send change candidates to {trait_type} handler: {e}");
            match e {
                mpsc::error::TrySendError::Full(item) => {
                    warn!("channel is full, falling back to async send");

                    let changes_tx = handle.changes_tx();
                    let id = *id;
                    tokio::spawn(async move {
                        if let Err(e) = changes_tx.send(item).await {
                            error!(sub_id = %id, "could not send match_change candidates to handler: {e}");
                        }
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

pub fn match_changes_from_db_version<H>(
    manager: &impl Manager<H>,
    conn: &Connection,
    db_version: CrsqlDbVersion,
    actor_id: ActorId,
) -> rusqlite::Result<()>
where
    H: Handle + Send + 'static,
{
    let handles = manager.get_handles();
    if handles.is_empty() {
        return Ok(());
    }

    let trait_type = manager.trait_type();
    let mut candidates = handles
        .iter()
        .map(|(id, handle)| (id, (MatchCandidates::new(), handle)))
        .collect::<BTreeMap<_, _>>();

    {
        let mut prepped = conn.prepare_cached(
            r#"
        SELECT "table", pk, cid, cl
            FROM crsql_changes
            WHERE db_version = ?
              AND site_id = ?
            ORDER BY seq ASC
        "#,
        )?;

        let rows = prepped.query_map((db_version, actor_id), |row| {
            Ok((
                row.get::<_, TableName>(0)?,
                row.get::<_, Vec<u8>>(1)?,
                row.get::<_, ColumnName>(2)?,
                row.get::<_, i64>(3)?,
            ))
        })?;

        for change_res in rows {
            let (table, pk, column, cl) = change_res?;

            for (_id, (candidates, handle)) in candidates.iter_mut() {
                let change = MatchableChange {
                    table: &table,
                    pk: &pk,
                    column: &column,
                    cl,
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
            handle
                .get_counter(table)
                .matched_count
                .increment(pks.len() as u64);
        }

        trace!(sub_id = %id, %db_version, "found {match_count} candidates");

        if let Err(e) = handle.changes_tx().try_send(candidates) {
            error!(sub_id = %id, "could not send change candidates to {trait_type} handler: {e}");
            match e {
                mpsc::error::TrySendError::Full(item) => {
                    warn!("channel is full, falling back to async send");
                    let changes_tx = handle.changes_tx();
                    let id = *id;
                    tokio::spawn(async move {
                        if let Err(e) = changes_tx.send(item).await {
                            error!(sub_id = %id, "could not send change candidates to handler: {e}");
                        }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates(table: &TableName, pks: impl IntoIterator<Item = (u32, i64)>) -> MatchCandidates {
        [(
            table.clone(),
            pks.into_iter()
                .map(|(pk, cl)| (pk.to_be_bytes().to_vec(), cl))
                .collect(),
        )]
        .into()
    }

    #[test]
    fn buffer_candidates_favors_newer_changes() {
        let table: TableName = "tests".into();
        let key = |pk: u32| (table.clone(), pk.to_be_bytes().to_vec());
        let mut buf = PendingChanges::new();
        let mut cl_cache = PendingChanges::new();

        let all = candidates(&table, (0..MAX_PENDING_CHANGES as u32).map(|pk| (pk, 1)));
        assert_eq!(buffer_candidates(&mut buf, &mut cl_cache, all), 0);

        // a newer cl replaces the buffered one, older cls are ignored
        let touch = candidates(&table, [(5, 3)]);
        assert_eq!(buffer_candidates(&mut buf, &mut cl_cache, touch), 0);
        let stale = candidates(&table, [(5, 2)]);
        assert_eq!(buffer_candidates(&mut buf, &mut cl_cache, stale), 0);
        assert_eq!(buf.get(&key(5)), Some(&3));

        // overflowing evicts the oldest pks first
        let new_pk = MAX_PENDING_CHANGES as u32;
        let extra = candidates(&table, [(new_pk, 1), (new_pk + 1, 1)]);
        assert_eq!(buffer_candidates(&mut buf, &mut cl_cache, extra), 2);

        assert_eq!(buf.len(), MAX_PENDING_CHANGES);
        assert!(!buf.contains_key(&key(0)));
        assert!(!buf.contains_key(&key(1)));
        assert!(buf.contains_key(&key(2)));
        assert_eq!(buf.get(&key(5)), Some(&3));
        assert_eq!(buf.last(), Some((&key(new_pk + 1), &1)));
        assert!(cl_cache.len() <= MAX_CACHE_ENTRIES);
    }
}
