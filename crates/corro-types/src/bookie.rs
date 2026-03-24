use std::{
    cmp,
    collections::{btree_map, BTreeMap, HashMap, HashSet},
    ops::{Deref, DerefMut, RangeInclusive},
    sync::Arc,
};

use antithesis_sdk::{assert_always, assert_unreachable};
use arc_swap::ArcSwap;
use papaya::{Guard, HashMap as PapayaHashMap};
use parking_lot::{ArcMutexGuard, Mutex, RawMutex};
use rangemap::{RangeInclusiveSet, StepLite};
use rusqlite::{named_params, Connection, OptionalExtension};
use serde::Serialize;
use serde_json::json;
use tracing::{debug, trace, warn};

use crate::{
    actor::ActorId,
    agent::PartialVersion,
    base::{CrsqlDbVersion, CrsqlDbVersionRange, CrsqlSeq, CrsqlSeqRange},
    broadcast::Timestamp,
    sqlite::unnest_param,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BookedVersions {
    actor_id: ActorId,
    pub partials: BTreeMap<CrsqlDbVersion, PartialVersion>,
    needed: RangeInclusiveSet<CrsqlDbVersion>,
    max: Option<CrsqlDbVersion>,
}

impl BookedVersions {
    pub fn needed(&self) -> &RangeInclusiveSet<CrsqlDbVersion> {
        &self.needed
    }

    pub fn insert_gaps(&mut self, db_versions: RangeInclusiveSet<CrsqlDbVersion>) {
        self.needed.extend(db_versions);
    }

    pub fn insert_db(
        &mut self,
        conn: &Connection, // usually a `Transaction`
        db_versions: RangeInclusiveSet<CrsqlDbVersion>,
    ) -> rusqlite::Result<()> {
        trace!("wants to insert into db {db_versions:?}");
        let mut changes = self.compute_gaps_change(db_versions);

        trace!(actor_id = %self.actor_id, "delete: {:?}", changes.remove_ranges);
        trace!(actor_id = %self.actor_id, "new: {:?}", changes.insert_set);

        // those are actual ranges we had stored and will change, remove them from the DB
        {
            let remove_ranges = std::mem::take(&mut changes.remove_ranges);
            let actors = unnest_param(remove_ranges.iter().map(|_| self.actor_id));
            let starts = unnest_param(remove_ranges.iter().map(|r| r.start()));
            let ends = unnest_param(remove_ranges.iter().map(|r| r.end()));
            // TODO: use returning to discover which ranges were actually deleted
            let count = conn
                .prepare_cached(
                    "
                    DELETE FROM __corro_bookkeeping_gaps WHERE (actor_id, start, end) 
                    IN (SELECT value0, value1, value2 FROM unnest(:actors, :starts, :ends))
                    ",
                )?
                .execute(named_params! {
                    ":actors": actors,
                    ":starts": starts,
                    ":ends": ends,
                })?;
            if count != remove_ranges.len() {
                warn!(actor_id = %self.actor_id, "did not delete some gaps from db: {remove_ranges:?}");
                let details: serde_json::Value = json!({"count": count, "ranges": remove_ranges});
                assert_unreachable!("ineffective deletion of gaps in-db", &details);
            }

            for range in remove_ranges {
                self.needed.remove(range);
            }
        }

        {
            let insert_set = std::mem::take(&mut changes.insert_set);
            let actors = unnest_param(insert_set.iter().map(|_| self.actor_id));
            let starts = unnest_param(insert_set.iter().map(|r| r.start()));
            let ends = unnest_param(insert_set.iter().map(|r| r.end()));
            debug!(actor_id = %self.actor_id, "inserting {insert_set:?}");
            // TODO: use returning to discover which ranges were actually inserted
            let count = conn
                .prepare_cached(
                    "
                    INSERT OR IGNORE INTO __corro_bookkeeping_gaps (actor_id, start, end) 
                    SELECT value0, value1, value2 FROM unnest(:actors, :starts, :ends)
                    ",
                )?
                .execute(named_params! {
                    ":actors": actors,
                    ":starts": starts,
                    ":ends": ends,
                })?;
            if count != insert_set.len() {
                warn!(actor_id = %self.actor_id, "did not insert some gaps into db: {insert_set:?}");

                let existing: Vec<(ActorId, CrsqlDbVersion, CrsqlDbVersion)> = conn
                    .prepare_cached(
                        "
                    SELECT actor_id, start, end FROM __corro_bookkeeping_gaps 
                    WHERE (actor_id, start) 
                    IN (SELECT value0, value1 FROM unnest(:actors, :starts))",
                    )?
                    .query_map(
                        named_params! {
                            ":actors": actors,
                            ":starts": starts,
                        },
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?;

                warn!("already had gaps entries! existing: {existing:?}");
                let details: serde_json::Value =
                    json!({"count": count, "insert_set": insert_set, "existing": existing});
                assert_unreachable!("ineffective insertion of gaps in-db", &details);
                return Err(rusqlite::Error::ModuleError(
                    "Gaps entries already present in DB".to_string(),
                ));
            }

            for range in insert_set {
                self.needed.insert(range);
            }
        }

        self.max = changes.max.take();

        Ok(())
    }

    fn compute_gaps_change(&self, versions: RangeInclusiveSet<CrsqlDbVersion>) -> GapsChanges {
        trace!("needed: {:?}", self.needed);

        let mut changes = GapsChanges {
            // set as the current max
            max: self.max,

            insert_set: Default::default(),
            remove_ranges: Default::default(),
        };

        for versions in versions.clone() {
            // only update the max if it's bigger
            changes.max = cmp::max(changes.max, Some(*versions.end()));

            let overlapping_ranges = compute_overlapping_ranges(&self.needed, &versions);
            changes.insert_set.extend(overlapping_ranges.clone());
            changes.remove_ranges.extend(overlapping_ranges.clone());

            // either a max or 0
            // TODO: figure out if we want to use 0 instead of None in the struct by default
            let current_max = self.max.unwrap_or_default();

            // check if there's a gap created between our current max and the start version we just inserted
            let gap_start = current_max + 1;
            if gap_start < *versions.start() {
                let range = gap_start..=*versions.start();
                trace!("inserting gap between max + 1 and start: {range:?}");
                changes.insert_set.insert(range.clone());
                for range in self.needed.overlapping(&range) {
                    changes.insert_set.insert(range.clone());
                    changes.remove_ranges.insert(range.clone());
                }
            }
        }

        for versions in versions {
            // we now know the applied versions
            changes.insert_set.remove(versions.clone());
        }

        changes
    }
}

fn compute_overlapping_ranges<T: Ord + Clone + StepLite + Into<u64>>(
    check_ranges: &RangeInclusiveSet<T>,
    range: &RangeInclusive<T>,
) -> RangeInclusiveSet<T> {
    let mut overlapping_ranges = RangeInclusiveSet::new();

    // iterate all partially or fully overlapping ranges
    for range in check_ranges.overlapping(range) {
        overlapping_ranges.insert(range.clone());
    }

    // check if there's a previous range with an end version = start version - 1
    let start_u64: u64 = range.start().clone().into();
    if start_u64 > 0 {
        let start = range.start().sub_one();
        if let Some(range) = check_ranges.get(&start) {
            overlapping_ranges.insert(range.clone());
        }
    }

    // check if there's a next range with an start version = end version + 1
    let end = range.end().add_one();
    if let Some(range) = check_ranges.get(&end) {
        overlapping_ranges.insert(range.clone());
    }

    overlapping_ranges
}

impl BookedVersions {
    pub fn new(actor_id: ActorId) -> Self {
        Self {
            actor_id,
            partials: Default::default(),
            needed: Default::default(),
            max: Default::default(),
        }
    }

    pub fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    pub fn from_conn(conn: &Connection, actor_id: ActorId) -> rusqlite::Result<Self> {
        trace!("from_conn");
        let mut bv = BookedVersions::new(actor_id);

        // fetch the biggest version we know, a partial version might override
        // this below
        bv.max = conn
            .prepare_cached("SELECT db_version FROM crsql_db_versions WHERE site_id = ?")?
            .query_row([actor_id], |row| row.get(0))
            .optional()?;

        {
            // fetch known partial sequences
            let mut prepped = conn.prepare_cached(
            "SELECT db_version, start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping WHERE site_id = ?",
            )?;
            let mut rows = prepped.query([actor_id])?;

            loop {
                let row = rows.next()?;
                match row {
                    None => break,
                    Some(row) => {
                        let version = row.get(0)?;
                        // NOTE: use normal insert logic to have a consistent behavior
                        bv.insert_partial(
                            version,
                            PartialVersion {
                                seqs: RangeInclusiveSet::from_iter(vec![row.get(1)?..=row.get(2)?]),
                                last_seq: row.get(3)?,
                                ts: row.get(4)?,
                            },
                        );
                    }
                }
            }
        }

        {
            // fetch the sync's needed version gaps
            let mut prepped = conn.prepare_cached(
                "SELECT start, end FROM __corro_bookkeeping_gaps WHERE actor_id = ?",
            )?;
            let mut rows = prepped.query([actor_id])?;

            loop {
                let row = rows.next()?;
                match row {
                    None => break,
                    Some(row) => {
                        let start_v = row.get(0)?;
                        let end_v = row.get(1)?;

                        // TODO: don't do this manually...
                        bv.needed.insert(start_v..=end_v);
                        // max for booked versions shouldn't come from gaps
                        if Some(end_v) > bv.max {
                            warn!(%actor_id, %start_v, %end_v, max = ?bv.max, "max for actor is less than gap");
                        }
                    }
                }
            }
        }

        Ok(bv)
    }

    pub fn load_all_from_conn(conn: &Connection) -> rusqlite::Result<HashMap<ActorId, Self>> {
        let mut map: HashMap<ActorId, Self> = HashMap::new();

        // Collect all known site_ids: remote actors we have applied changes from,
        // plus any we have buffered partial changes for, plus ourselves.
        {
            let mut prepped = conn.prepare(
                "SELECT site_id FROM crsql_site_id
                        UNION
                    SELECT DISTINCT site_id FROM __corro_seq_bookkeeping
                        UNION 
                    SELECT DISTINCT actor_id FROM __corro_bookkeeping_gaps",
            )?;
            let rows = prepped.query_map([], |row| row.get(0))?;
            for actor_id in rows {
                let actor_id: ActorId = actor_id?;
                map.entry(actor_id).or_insert_with(|| Self::new(actor_id));
            }
        }

        // Populate max from crsql_db_versions
        {
            let mut prepped = conn.prepare("SELECT site_id, db_version FROM crsql_db_versions")?;
            let rows = prepped.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
            for row in rows {
                let (actor_id, db_version): (ActorId, CrsqlDbVersion) = row?;
                if let Some(bv) = map.get_mut(&actor_id) {
                    bv.max = Some(db_version);
                }
            }
        }

        // Populate partials from __corro_seq_bookkeeping
        {
            let mut prepped = conn.prepare(
                "SELECT site_id, db_version, start_seq, end_seq, last_seq, ts FROM __corro_seq_bookkeeping",
            )?;
            let rows = prepped.query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            })?;
            for row in rows {
                let (actor_id, version, start_seq, end_seq, last_seq, ts): (
                    ActorId,
                    CrsqlDbVersion,
                    _,
                    _,
                    _,
                    _,
                ) = row?;
                if let Some(bv) = map.get_mut(&actor_id) {
                    bv.insert_partial(
                        version,
                        PartialVersion {
                            seqs: RangeInclusiveSet::from_iter(vec![start_seq..=end_seq]),
                            last_seq,
                            ts,
                        },
                    );
                }
            }
        }

        // Populate needed gaps from __corro_bookkeeping_gaps
        {
            let mut prepped =
                conn.prepare("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps")?;
            let rows = prepped.query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?;
            for row in rows {
                let (actor_id, start_v, end_v): (ActorId, CrsqlDbVersion, CrsqlDbVersion) = row?;
                if let Some(bv) = map.get_mut(&actor_id) {
                    bv.needed.insert(start_v..=end_v);
                    if Some(end_v) > bv.max {
                        warn!(%actor_id, %start_v, %end_v, max = ?bv.max, "max for actor is less than gap");
                    }
                }
            }
        }

        Ok(map)
    }

    pub fn contains_version(&self, version: &CrsqlDbVersion) -> bool {
        // corrosion knows about a version if...

        // it's not in the list of needed versions
        !self.needed.iter().any(|range| range.start() <= version && version <= range.end()) &&
        // and the last known version is bigger than the requested version
        self.max.unwrap_or_default() >= *version
        // we don't need to look at partials because if we have a partial
        // then it fulfills the previous conditions
    }

    pub fn get_partial(&self, version: &CrsqlDbVersion) -> Option<&PartialVersion> {
        self.partials.get(version)
    }

    pub fn contains(&self, version: CrsqlDbVersion, seqs: Option<CrsqlSeqRange>) -> bool {
        self.contains_version(&version)
            && seqs
                .map(|mut check_seqs| match self.partials.get(&version) {
                    Some(partial) => check_seqs.all(|seq| partial.seqs.contains(&seq)),
                    // if `contains_version` is true but we don't have a partial version,
                    // then we must have it as a fully applied or cleared version
                    None => true,
                })
                // if we have partials for the version but seqs are empty
                // then a partial version has been cleared and we haven't seen it yet
                .unwrap_or(!self.partials.contains_key(&version))
    }

    #[inline]
    pub fn contains_all(
        &self,
        versions: impl Into<CrsqlDbVersionRange>,
        seqs: Option<CrsqlSeqRange>,
    ) -> bool {
        versions.into().all(|version| self.contains(version, seqs))
    }

    pub fn last(&self) -> Option<CrsqlDbVersion> {
        self.max
    }

    /// Removes partials for the versions
    pub fn clear_partials(
        &mut self,
        conn: &Connection,
        versions: RangeInclusiveSet<CrsqlDbVersion>,
    ) -> rusqlite::Result<()> {
        let remove_versions = versions
            .into_iter()
            .flat_map(CrsqlDbVersionRange::from)
            .filter(|v| self.partials.contains_key(v))
            .collect::<Vec<_>>();

        let deleted = conn.prepare_cached(
            "DELETE FROM __corro_seq_bookkeeping WHERE site_id = ? AND db_version IN (SELECT value0 FROM unnest(:versions))",
        )?
        .execute((self.actor_id, unnest_param(&remove_versions)))?;

        if deleted != remove_versions.len() {
            warn!(actor_id = %self.actor_id, "ineffective deletion of partials in-db: {remove_versions:?} count: {deleted}");
            let details: serde_json::Value = json!({"count": deleted, "expected": remove_versions.len(), "versions": remove_versions});
            assert_unreachable!("ineffective deletion of partials in-db", &details);
        }

        for version in remove_versions {
            self.partials.remove(&version);
        }

        Ok(())
    }

    /// Merges new partial data into the in-memory `partials` map only.
    pub fn insert_partial(
        &mut self,
        version: CrsqlDbVersion,
        partial: PartialVersion,
    ) -> PartialVersion {
        debug!(actor_id = %self.actor_id, "insert partial {version:?}");

        match self.partials.entry(version) {
            btree_map::Entry::Vacant(entry) => {
                self.max = cmp::max(self.max, Some(version));
                entry.insert(partial).clone()
            }
            btree_map::Entry::Occupied(mut entry) => {
                let got = entry.get_mut();
                let details = json!({"version": version, "current": got.last_seq, "received": partial.last_seq});
                assert_always!(
                    got.last_seq == partial.last_seq,
                    "last_seq matches for partial version",
                    &details
                );
                got.seqs.extend(partial.seqs);
                got.clone()
            }
        }
    }

    /// Compute what `__corro_seq_bookkeeping` rows need to change based on the
    /// current in-memory `partials` state and the new partial data to merge.
    fn compute_partials_change(
        &self,
        version: CrsqlDbVersion,
        partial: &PartialVersion,
    ) -> PartialChanges {
        let mut changes = PartialChanges {
            remove_seqs: RangeInclusiveSet::new(),
            insert_seqs: RangeInclusiveSet::new(),
        };

        if let Some(existing) = self.partials.get(&version) {
            for seq in partial.seqs.iter() {
                let ranges = compute_overlapping_ranges(&existing.seqs, seq);
                changes.insert_seqs.extend(ranges.clone());
                changes.remove_seqs.extend(ranges.clone());
            }
        }

        // ensure that all the new seqs are inserted
        for range in partial.seqs.iter() {
            changes.insert_seqs.insert(range.clone());
        }

        changes
    }

    /// Update `__corro_seq_bookkeeping` and in-memory `partials`
    /// based on new partial data, similar to `insert_db` for gaps.
    ///
    /// Returns the list of versions that became complete after merging.
    pub fn insert_partials_db(
        &mut self,
        conn: &Connection,
        partials: BTreeMap<CrsqlDbVersion, PartialVersion>,
    ) -> rusqlite::Result<Vec<CrsqlDbVersion>> {
        #[derive(Default, Serialize)]
        struct PartialParams {
            actors: Vec<ActorId>,
            versions: Vec<CrsqlDbVersion>,
            start_seqs: Vec<CrsqlSeq>,
            end_seqs: Vec<CrsqlSeq>,

            // only used in insert cases
            last_seqs: Vec<CrsqlSeq>,
            timestamps: Vec<Timestamp>,
        }

        let mut remove_params = PartialParams::default();
        let mut insert_params = PartialParams::default();
        for (version, partial) in partials.iter() {
            let last_seq = partial.last_seq;
            let ts = partial.ts;
            let changes = self.compute_partials_change(*version, partial);
            debug!(actor_id = %self.actor_id, "computed partials change for version {version:?}: {changes:?}");
            if !changes.remove_seqs.is_empty() {
                remove_params
                    .actors
                    .extend(changes.remove_seqs.iter().map(|_| self.actor_id));
                remove_params
                    .versions
                    .extend(changes.remove_seqs.iter().map(|_| *version));
                remove_params
                    .start_seqs
                    .extend(changes.remove_seqs.iter().map(|s| s.start()));
                remove_params
                    .end_seqs
                    .extend(changes.remove_seqs.iter().map(|s| s.end()));
            }

            // we should always have something to insert
            let details =
                json!({"actor_id": self.actor_id, "version": version, "last_seq": last_seq});
            assert_always!(
                !changes.insert_seqs.is_empty(),
                "insert_seqs should not be empty",
                &details
            );
            insert_params
                .actors
                .extend(changes.insert_seqs.iter().map(|_| self.actor_id));
            insert_params
                .versions
                .extend(changes.insert_seqs.iter().map(|_| version));
            insert_params
                .start_seqs
                .extend(changes.insert_seqs.iter().map(|s| s.start()));
            insert_params
                .end_seqs
                .extend(changes.insert_seqs.iter().map(|s| s.end()));
            insert_params
                .last_seqs
                .extend(changes.insert_seqs.iter().map(|_| last_seq));
            insert_params
                .timestamps
                .extend(changes.insert_seqs.iter().map(|_| ts));
        }

        trace!(actor_id = %self.actor_id, "partials delete: {:?}", remove_params.actors.len());
        trace!(actor_id = %self.actor_id, "partials insert: {:?}", insert_params.actors.len());

        // delete old seq bookkeeping rows
        if !remove_params.actors.is_empty() {
            let actors = unnest_param(remove_params.actors.iter());
            let versions = unnest_param(remove_params.versions.iter());
            let start_seqs = unnest_param(remove_params.start_seqs.iter());
            let end_seqs = unnest_param(remove_params.end_seqs.iter());

            let deleted = conn.prepare_cached(
                "DELETE FROM __corro_seq_bookkeeping
                 WHERE (site_id, db_version, start_seq, end_seq)
                 IN (SELECT value0, value1, value2, value3 FROM unnest(:actors, :versions, :start_seqs, :end_seqs))",
            )?
            .execute(named_params! {
                ":actors": actors,
                ":versions": versions,
                ":start_seqs": start_seqs,
                ":end_seqs": end_seqs,
            })?;

            if deleted != remove_params.actors.len() {
                warn!(actor_id = %self.actor_id, "did not delete some seq bookkeeping rows from db: deleted: {deleted}, expected: {}", remove_params.actors.len());
                let details: serde_json::Value = json!({"count": deleted, "expected": remove_params.actors.len(), "params": remove_params});
                assert_unreachable!(
                    "ineffective deletion of seq bookkeeping rows in-db",
                    &details
                );
            }
        }

        // insert new merged seq bookkeeping rows
        if !insert_params.actors.is_empty() {
            let actors = unnest_param(insert_params.actors.iter());
            let versions = unnest_param(insert_params.versions.iter());
            let start_seqs = unnest_param(insert_params.start_seqs.iter());
            let end_seqs = unnest_param(insert_params.end_seqs.iter());
            let last_seqs = unnest_param(insert_params.last_seqs.iter());
            let timestamps = unnest_param(insert_params.timestamps.iter());

            conn.prepare_cached(
                "INSERT OR REPLACE INTO __corro_seq_bookkeeping
                    (site_id, db_version, start_seq, end_seq, last_seq, ts)
                 SELECT value0, value1, value2, value3, value4, value5
                 FROM unnest(:actors, :versions, :start_seqs, :end_seqs, :last_seqs, :timestamps)",
            )?
            .execute(named_params! {
                ":actors": actors,
                ":versions": versions,
                ":start_seqs": start_seqs,
                ":end_seqs": end_seqs,
                ":last_seqs": last_seqs,
                ":timestamps": timestamps,
            })?;
        }

        let mut completed = Vec::new();
        for (version, partial) in partials {
            let partial = self.insert_partial(version, partial);
            if partial.is_complete() {
                completed.push(version);
            }
        }

        Ok(completed)
    }
}

#[derive(Debug)]
pub struct PartialChanges {
    remove_seqs: RangeInclusiveSet<CrsqlSeq>,
    insert_seqs: RangeInclusiveSet<CrsqlSeq>,
}

#[derive(Debug)]
pub struct GapsChanges {
    max: Option<CrsqlDbVersion>,
    insert_set: RangeInclusiveSet<CrsqlDbVersion>,
    remove_ranges: HashSet<RangeInclusive<CrsqlDbVersion>>,
}

#[derive(Clone)]
pub struct Booked {
    versions: Arc<ArcSwap<BookedVersions>>,
}

pub struct BookedWriteTx {
    versions: Arc<ArcSwap<BookedVersions>>,
    working: BookedVersions,
}

impl BookedWriteTx {
    // Publishes the working copy into ArcSwap so it's visible by readers.
    // The external BookieWriteGuard controls lock lifetime.
    pub fn commit(self) {
        self.versions.store(Arc::new(self.working));
    }
}

impl Deref for BookedWriteTx {
    type Target = BookedVersions;

    fn deref(&self) -> &Self::Target {
        &self.working
    }
}

impl DerefMut for BookedWriteTx {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.working
    }
}

pub struct BookieWriteGuard {
    _writer_guard: ArcMutexGuard<RawMutex, ()>,
}

impl BookieWriteGuard {
    fn new(writer_guard: ArcMutexGuard<RawMutex, ()>) -> BookieWriteGuard {
        BookieWriteGuard {
            _writer_guard: writer_guard,
        }
    }

    pub fn write_tx(&self, booked: &Booked) -> BookedWriteTx {
        booked.write_tx()
    }
}

impl Booked {
    pub fn new(versions: BookedVersions) -> Self {
        Self {
            versions: Arc::new(ArcSwap::from_pointee(versions)),
        }
    }

    pub fn read(
        &self,
    ) -> arc_swap::Guard<Arc<BookedVersions>, arc_swap::strategy::DefaultStrategy> {
        self.versions.load()
    }

    fn write_tx(&self) -> BookedWriteTx {
        let current = self.versions.load_full();

        BookedWriteTx {
            versions: self.versions.clone(),
            working: (*current).clone(),
        }
    }
}

#[derive(Clone)]
pub struct Bookie {
    map: Arc<PapayaHashMap<ActorId, Booked>>,
    writer: Arc<Mutex<()>>,
}

impl Bookie {
    // WARNING: this method blocks the current thread while waiting for the lock.
    // In async contexts this can starve runtime workers if used outside block_in_place/spawn_blocking.
    pub fn write_lock_blocking(&self) -> BookieWriteGuard {
        BookieWriteGuard::new(self.writer.lock_arc())
    }

    pub fn new(map: HashMap<ActorId, BookedVersions>) -> Self {
        let papaya_map = PapayaHashMap::new();
        {
            let pinned_map = papaya_map.pin();
            for (actor_id, booked_versions) in map {
                pinned_map.insert(actor_id, Booked::new(booked_versions));
            }
        }

        Self {
            map: Arc::new(papaya_map),
            writer: Arc::new(Mutex::new(())),
        }
    }

    pub fn get(&self, actor_id: &ActorId) -> Option<Booked> {
        self.map.pin().get(actor_id).cloned()
    }

    pub fn owned_guard(&self) -> impl Guard + '_ {
        self.map.owned_guard()
    }

    pub fn iter<'guard>(
        &self,
        guard: &'guard impl Guard,
    ) -> impl Iterator<Item = (&'guard ActorId, &'guard Booked)> {
        self.map.iter(guard)
    }

    pub fn ensure(&self, actor_id: ActorId) -> Booked {
        self.map
            .pin()
            .get_or_insert(actor_id, Booked::new(BookedVersions::new(actor_id)))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::migrate;
    use crate::base::{dbvr, dbvri};
    use crate::sqlite::{setup_conn, CrConn};
    use rangemap::range_inclusive_set;
    use rusqlite::params;

    #[test]
    fn test_booked_insert_db() -> rusqlite::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let conn = new_conn()?;

        let actor_id = ActorId::default();
        let mut bv = BookedVersions::new(actor_id);

        let mut all = RangeInclusiveSet::new();

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(1, 20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(1, 10)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // try from an empty state again
        let mut bv = BookedVersions::new(actor_id);
        let mut all = RangeInclusiveSet::new();

        // create 2:=3 gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(1, 1), dbvri!(4, 4)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(2, 3)])?;

        // fill gap
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(3, 3), dbvri!(2, 2)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // try from an empty state again
        let mut bv = BookedVersions::new(actor_id);
        let mut all = RangeInclusiveSet::new();

        // insert a non-1 first version
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(5, 20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(1, 4)])?;

        // insert a further change that does not overlap a gap
        insert_everywhere(&conn, &mut bv, &mut all, range_inclusive_set![dbvri!(6, 7)])?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(1, 4)])?;

        // insert a further change that does overlap a gap
        insert_everywhere(&conn, &mut bv, &mut all, range_inclusive_set![dbvri!(3, 7)])?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(1, 2)])?;

        insert_everywhere(&conn, &mut bv, &mut all, range_inclusive_set![dbvri!(1, 2)])?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(25, 25)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(21, 24)])?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(30, 35)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(21, 24), dbvr!(26, 29)])?;

        // NOTE: overlapping partially from the end

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(19, 22)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(23, 24), dbvr!(26, 29)])?;

        // NOTE: overlapping partially from the start

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(24, 25)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(23, 23), dbvr!(26, 29)])?;

        // NOTE: overlapping 2 ranges

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(23, 27)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(28, 29)])?;

        // NOTE: ineffective insert of already known ranges

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(1, 20)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(28, 29)])?;

        // NOTE: overlapping no ranges, but encompassing a full range

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(27, 30)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![])?;

        // NOTE: touching multiple ranges, partially

        // create gap 36..=39
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(40, 45)],
        )?;
        // create gap 46..=49
        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(50, 55)],
        )?;

        insert_everywhere(
            &conn,
            &mut bv,
            &mut all,
            range_inclusive_set![dbvri!(38, 47)],
        )?;
        expect_gaps(&conn, &bv, &all, vec![dbvr!(36, 37), dbvr!(48, 49)])?;

        // test loading a bv from the conn, they should be identical!
        let mut bv2 = BookedVersions::from_conn(&conn, actor_id)?;
        // manually set the last version because there's nothing in `__corro_bookkeeping`
        bv2.max = Some(CrsqlDbVersion(55));

        assert_eq!(bv, bv2);

        Ok(())
    }

    #[test]
    fn test_load_all_from_conn() -> rusqlite::Result<()> {
        _ = tracing_subscriber::fmt::try_init();

        let conn = new_conn()?;
        // The local node's own site_id is stored at ordinal=0 in crsql_site_id.
        let local_actor_id: ActorId = conn.query_row(
            "SELECT site_id FROM crsql_site_id WHERE ordinal = 0",
            [],
            |row| row.get(0),
        )?;

        // Create a cr-sqlite tracked table and insert a row so that crsql_db_versions
        // records a db_version for the local actor. This lets us verify that max is loaded.
        conn.execute_batch(
            "CREATE TABLE test_dummy (id INTEGER NOT NULL PRIMARY KEY, val TEXT);
             SELECT crsql_as_crr('test_dummy');",
        )?;
        conn.execute("INSERT INTO test_dummy (id, val) VALUES (1, 'hello')", [])?;
        let expected_max: CrsqlDbVersion = conn.query_row(
            "SELECT db_version FROM crsql_db_versions WHERE site_id = crsql_site_id()",
            [],
            |row| row.get(0),
        )?;

        // A remote actor known only via a bookkeeping gap (versions 3..=5 are missing).
        let gap_actor_id = ActorId(uuid::Uuid::new_v4());
        conn.execute(
            "INSERT INTO __corro_bookkeeping_gaps (actor_id, start, end) VALUES (?, ?, ?)",
            params![gap_actor_id, CrsqlDbVersion(3), CrsqlDbVersion(5)],
        )?;

        // A remote actor known only via a partial seq entry.
        let partial_actor_id = ActorId(uuid::Uuid::new_v4());
        conn.execute(
            "INSERT INTO __corro_seq_bookkeeping (site_id, db_version, start_seq, end_seq, last_seq, ts) VALUES (?, ?, ?, ?, ?, ?)",
            params![partial_actor_id, CrsqlDbVersion(1), 0i64, 4i64, 9i64, Timestamp::zero()],
        )?;

        let map = BookedVersions::load_all_from_conn(&conn)?;

        assert!(
            map.contains_key(&local_actor_id),
            "load_all_from_conn must include the local actor (ordinal=0 in crsql_site_id), but it was missing"
        );
        assert_eq!(
            map[&local_actor_id].max,
            Some(expected_max),
            "local actor's max db_version must be loaded from crsql_db_versions"
        );
        assert!(
            map.contains_key(&gap_actor_id),
            "load_all_from_conn must include actors known via __corro_bookkeeping_gaps"
        );
        assert!(
            map.contains_key(&partial_actor_id),
            "load_all_from_conn must include actors known via __corro_seq_bookkeeping"
        );

        Ok(())
    }

    fn insert_everywhere(
        conn: &Connection,
        bv: &mut BookedVersions,
        all_versions: &mut RangeInclusiveSet<CrsqlDbVersion>,
        versions: RangeInclusiveSet<CrsqlDbVersion>,
    ) -> rusqlite::Result<()> {
        all_versions.extend(versions.clone());
        bv.insert_db(conn, versions)
    }

    fn expect_gaps(
        conn: &Connection,
        bv: &BookedVersions,
        all_versions: &RangeInclusiveSet<CrsqlDbVersion>,
        expected: Vec<CrsqlDbVersionRange>,
    ) -> rusqlite::Result<()> {
        let gaps: Vec<(ActorId, CrsqlDbVersion, CrsqlDbVersion)> = conn
            .prepare_cached("SELECT actor_id, start, end FROM __corro_bookkeeping_gaps")?
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;

        assert_eq!(
            gaps,
            expected
                .clone()
                .into_iter()
                .map(|expected| (bv.actor_id, expected.start(), expected.end()))
                .collect::<Vec<_>>()
        );

        for range in all_versions.iter() {
            assert!(bv.contains_all(range.clone(), None));
        }

        for range in expected {
            for v in range {
                assert!(!bv.contains(v, None), "expected not to contain {v}");
                assert!(bv.needed.contains(&v), "expected needed to contain {v:?}");
            }
        }

        assert_eq!(
            bv.max,
            all_versions.iter().next_back().map(|range| *range.end()),
            "expected last version not to increment"
        );

        Ok(())
    }

    fn new_conn() -> rusqlite::Result<CrConn> {
        let mut conn = CrConn::init(Connection::open_in_memory()?)?;
        setup_conn(&conn)?;
        let clock = Arc::new(uhlc::HLC::default());
        migrate(clock, &mut conn)?;
        Ok(conn)
    }

    fn check_version_partials(
        conn: &Connection,
        bv: &BookedVersions,
        all: &BTreeMap<CrsqlDbVersion, (CrsqlSeq, RangeInclusiveSet<CrsqlSeq>)>,
    ) -> rusqlite::Result<()> {
        for (version, (last_seq, expected_seqs)) in all.iter() {
            let bookie_partial = bv.partials.get(version).unwrap();
            assert_eq!(bookie_partial.last_seq, *last_seq, "last_seq mismatch");
            assert_eq!(&bookie_partial.seqs, expected_seqs, "partial seqs mismatch");

            let db_rows = conn.prepare_cached(
                "SELECT start_seq, end_seq FROM __corro_seq_bookkeeping WHERE site_id = ? AND db_version = ? and last_seq = ?",
            )?
            .query_map((bv.actor_id(), version, last_seq), |row| {
                Ok(row.get(0)?..=row.get(1)?)
            })?
            .collect::<Result<RangeInclusiveSet<CrsqlSeq>, _>>()?;
            assert_eq!(&db_rows, expected_seqs, "DB seq bookkeeping mismatch");
        }

        Ok(())
    }

    #[test]
    fn test_booked_insert_partials_db() -> rusqlite::Result<()> {
        _ = tracing_subscriber::fmt::try_init();
        let conn = new_conn()?;
        let actor_id = ActorId(uuid::Uuid::new_v4());
        let mut bv = BookedVersions::new(actor_id);

        let mut all = BTreeMap::new();

        let (v1, last_seq_1) = (CrsqlDbVersion(1), CrsqlSeq(5));
        let (v2, last_seq_2) = (CrsqlDbVersion(2), CrsqlSeq(9));
        let (v3, last_seq_3) = (CrsqlDbVersion(3), CrsqlSeq(20));
        let (v4, last_seq_4) = (CrsqlDbVersion(4), CrsqlSeq(9));

        // empty input is a no-op
        let complete_seqs = bv.insert_partials_db(&conn, BTreeMap::new())?;
        assert!(complete_seqs.is_empty());
        assert!(bv.partials.is_empty());

        let complete_seqs = insert_partials_all(
            &conn,
            &mut all,
            &mut bv,
            vec![
                (v1, last_seq_1, [0..=5].into()),        // already complete
                (v2, last_seq_2, [1..=3, 6..=8].into()), // insert middle seqs
                (v3, last_seq_3, [0..=0].into()),        // insert only the first seq
                (v4, last_seq_4, [9..=9].into()),        // insert only the last seq
            ],
        )?;

        assert_eq!(complete_seqs, vec![v1]);
        assert!(bv.partials.get(&v1).unwrap().is_complete());
        check_version_partials(&conn, &bv, &all)?;

        let complete_seqs = insert_partials_all(
            &conn,
            &mut all,
            &mut bv,
            vec![
                (v2, last_seq_2, [2..=7].into()), // overlapping two ranges
                (v3, last_seq_3, [1..=1].into()), // start - 1 overlaps
                (v4, last_seq_4, [6..=8].into()), // end + 1 overlaps
            ],
        )?;
        assert!(complete_seqs.is_empty());
        check_version_partials(&conn, &bv, &all)?;

        let complete_seqs = insert_partials_all(
            &conn,
            &mut all,
            &mut bv,
            vec![
                (v2, last_seq_2, [3..=8].into()), // already received seqs
                (v3, last_seq_3, [2..=5, 19..=20, 22..=22].into()), // disjoint seqs
                (v4, last_seq_4, [7..=7].into()), // smaller range
            ],
        )?;
        assert!(complete_seqs.is_empty());
        check_version_partials(&conn, &bv, &all)?;

        let complete_seqs = insert_partials_all(
            &conn,
            &mut all,
            &mut bv,
            vec![
                (v2, last_seq_2, [1..=8].into()), // insert exact range
                (v3, last_seq_3, [1..=1].into()), // insert range that'd connect two existing ranges
            ],
        )?;
        assert!(complete_seqs.is_empty());
        check_version_partials(&conn, &bv, &all)?;

        // complete v2
        let complete_seqs = insert_partials_all(
            &conn,
            &mut all,
            &mut bv,
            vec![
                (v2, last_seq_2, [0..=7, 9..=9].into()),
                (v3, last_seq_3, [4..=11, 14..=17, 19..=20].into()),
            ],
        )?;
        assert!(!complete_seqs.is_empty());
        check_version_partials(&conn, &bv, &all)?;

        // clear complete partial from memory and DB
        bv.clear_partials(&conn, RangeInclusiveSet::from([v1..=v2]))?;
        assert!(!check_seq_partials_exists(&conn, &bv, v1)?);
        assert!(!check_seq_partials_exists(&conn, &bv, v2)?);

        // roundtrip: loading from DB matches in-memory state
        let bv2 = BookedVersions::from_conn(&conn, actor_id)?;
        assert_eq!(bv.partials, bv2.partials);

        Ok(())
    }

    fn insert_partials_all(
        conn: &Connection,
        all: &mut BTreeMap<CrsqlDbVersion, (CrsqlSeq, RangeInclusiveSet<CrsqlSeq>)>,
        bv: &mut BookedVersions,
        partials: Vec<(CrsqlDbVersion, CrsqlSeq, RangeInclusiveSet<u64>)>,
    ) -> rusqlite::Result<Vec<CrsqlDbVersion>> {
        let mut partials_map = BTreeMap::new();
        for (version, last_seq, seqs) in partials {
            let seqs_set: RangeInclusiveSet<CrsqlSeq> = seqs
                .into_iter()
                .map(|s| CrsqlSeq(*s.start())..=CrsqlSeq(*s.end()))
                .collect();
            all.entry(version)
                .or_insert((last_seq, RangeInclusiveSet::new()))
                .1
                .extend(seqs_set.clone());

            partials_map.insert(
                version,
                PartialVersion {
                    seqs: seqs_set,
                    last_seq,
                    ts: Timestamp::zero(),
                },
            );
        }

        let complete = bv.insert_partials_db(conn, partials_map)?;
        Ok(complete)
    }

    fn check_seq_partials_exists(
        conn: &Connection,
        bv: &BookedVersions,
        version: CrsqlDbVersion,
    ) -> rusqlite::Result<bool> {
        let db_row = conn.prepare_cached(
            "SELECT EXISTS(SELECT 1 FROM __corro_seq_bookkeeping WHERE site_id = ? AND db_version = ?)",
        )?
        .query_row((bv.actor_id(), version), |row| {
            row.get(0)
        })?;
        Ok(db_row || bv.get_partial(&version).is_some())
    }
}
