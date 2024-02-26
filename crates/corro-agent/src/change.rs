use crate::agent::util;
use corro_types::{
    actor::ActorId,
    agent::{Agent, Bookie, ChangeError, CurrentVersion, KnownDbVersion, PartialVersion},
    base::{CrsqlSeq, CrsqlDbVersion},
    broadcast::{ChangeSource, ChangeV1, Changeset},
};
use itertools::Itertools;
use metrics::{counter, histogram};
use rangemap::RangeInclusiveMap;
use rusqlite::{named_params, Transaction};
use std::{
    cmp,
    collections::{BTreeMap, HashSet},
    time::Instant,
};
use tokio::task::block_in_place;
use tracing::{debug, error, trace, warn};

#[tracing::instrument(skip(agent, bookie, changes), err)]
pub async fn process_multiple_changes(
    agent: Agent,
    bookie: Bookie,
    changes: Vec<(ChangeV1, ChangeSource, Instant)>,
) -> Result<(), ChangeError> {
    let start = Instant::now();
    counter!("corro.agent.changes.processing.started").increment(changes.len() as u64);
    debug!(self_actor_id = %agent.actor_id(), "processing multiple changes, len: {}", changes.iter().map(|(change, _, _)| cmp::max(change.len(), 1)).sum::<usize>());

    let mut seen = HashSet::new();
    let mut unknown_changes = Vec::with_capacity(changes.len());
    for (change, src, queued_at) in changes {
        histogram!("corro.agent.changes.queued.seconds").record(queued_at.elapsed());
        let versions = change.versions();
        let seqs = change.seqs();
        if !seen.insert((change.actor_id, versions, seqs.cloned())) {
            continue;
        }
        if bookie
            .write(format!(
                "process_multiple_changes(ensure):{}",
                change.actor_id.as_simple()
            ))
            .await
            .ensure(change.actor_id)
            .read(format!(
                "process_multiple_changes(contains?):{}",
                change.actor_id.as_simple()
            ))
            .await
            .contains_all(change.versions(), change.seqs())
        {
            continue;
        }

        unknown_changes.push((change, src));
    }

    unknown_changes.sort_by_key(|(change, _src)| change.actor_id);

    let mut conn = agent.pool().write_normal().await?;

    let changesets = block_in_place(|| {
        let start = Instant::now();
        let tx = conn
            .immediate_transaction()
            .map_err(|source| ChangeError::Rusqlite {
                source,
                actor_id: None,
                version: None,
            })?;

        let mut knowns: BTreeMap<ActorId, Vec<_>> = BTreeMap::new();
        let mut changesets = vec![];

        let mut last_db_version = None;

        for (actor_id, changes) in unknown_changes
            .into_iter()
            .group_by(|(change, _src)| change.actor_id)
            .into_iter()
        {
            // get a lock on the actor id's booked writer if we didn't already
            {
                let booked = {
                    bookie
                        .blocking_write(format!(
                            "process_multiple_changes(for_actor_blocking):{}",
                            actor_id.as_simple()
                        ))
                        .ensure(actor_id)
                };
                let booked_write = booked.blocking_write(format!(
                    "process_multiple_changes(booked writer):{}",
                    actor_id.as_simple()
                ));

                let mut seen = RangeInclusiveMap::new();

                for (change, src) in changes {
                    trace!("handling a single changeset: {change:?}");
                    let seqs = change.seqs();
                    if booked_write.contains_all(change.versions(), change.seqs()) {
                        trace!(
                            "previously unknown versions are now deemed known, aborting inserts"
                        );
                        continue;
                    }

                    let versions = change.versions();

                    // check if we've seen this version here...
                    if versions.clone().all(|version| match seqs {
                        Some(check_seqs) => match seen.get(&version) {
                            Some(known) => match known {
                                KnownDbVersion::Partial(PartialVersion { seqs, .. }) => {
                                    check_seqs.clone().all(|seq| seqs.contains(&seq))
                                }
                                KnownDbVersion::Current { .. } | KnownDbVersion::Cleared => true,
                            },
                            None => false,
                        },
                        None => seen.contains_key(&version),
                    }) {
                        continue;
                    }

                    // optimizing this, insert later!
                    let known = if change.is_complete() && change.is_empty() {
                        // we never want to block here
                        if let Err(e) = agent.tx_empty().try_send((actor_id, change.versions())) {
                            error!("could not send empty changed versions into channel: {e}");
                        }

                        KnownDbVersion::Cleared
                    } else {
                        if let Some(seqs) = change.seqs() {
                            if seqs.end() < seqs.start() {
                                warn!(%actor_id, versions = ?change.versions(), "received an invalid change, seqs start is greater than seqs end: {seqs:?}");
                                continue;
                            }
                        }

                        let (known, versions) = match process_single_version(
                            &agent,
                            &tx,
                            last_db_version,
                            change,
                        ) {
                            Ok((known, changeset)) => {
                                let versions = changeset.versions();
                                if let KnownDbVersion::Current(CurrentVersion {
                                    db_version, ..
                                }) = &known
                                {
                                    last_db_version = Some(*db_version);
                                    changesets.push((actor_id, changeset, *db_version, src));
                                }
                                (known, versions)
                            }
                            Err(e) => {
                                error!(%actor_id, ?versions, "could not process single change: {e}");
                                continue;
                            }
                        };
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "got known to insert: {known:?}");
                        known
                    };

                    seen.insert(versions.clone(), known.clone());
                    knowns.entry(actor_id).or_default().push((versions, known));
                }
            }
        }

        let mut count = 0;

        for (actor_id, knowns) in knowns.iter_mut() {
            debug!(%actor_id, self_actor_id = %agent.actor_id(), "processing {} knowns", knowns.len());
            for (versions, known) in knowns.iter_mut() {
                match known {
                    KnownDbVersion::Partial { .. } => {
                        continue;
                    }
                    KnownDbVersion::Current(CurrentVersion {
                        db_version,
                        last_seq,
                        ts,
                    }) => {
                        count += 1;
                        let version = versions.start();
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), %version, "inserting bookkeeping row db_version: {db_version}, ts: {ts:?}");
                        tx.prepare_cached("
                            INSERT INTO __corro_bookkeeping ( actor_id,  start_version,  db_version,  last_seq,  ts)
                                                    VALUES  (:actor_id, :start_version, :db_version, :last_seq, :ts);").map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(*actor_id), version: Some(*version)})?
                            .execute(named_params!{
                                ":actor_id": actor_id,
                                ":start_version": *version,
                                ":db_version": *db_version,
                                ":last_seq": *last_seq,
                                ":ts": *ts
                            }).map_err(|source| ChangeError::Rusqlite{source, actor_id: Some(*actor_id), version: Some(*version)})?;
                    }
                    KnownDbVersion::Cleared => {
                        debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserting CLEARED bookkeeping");
                        if let Err(e) = agent.tx_empty().try_send((*actor_id, versions.clone())) {
                            error!("could not schedule version to be cleared: {e}");
                        }
                    }
                }
                debug!(%actor_id, self_actor_id = %agent.actor_id(), ?versions, "inserted bookkeeping row");
            }
        }

        debug!("inserted {count} new changesets");

        tx.commit().map_err(|source| ChangeError::Rusqlite {
            source,
            actor_id: None,
            version: None,
        })?;

        for (_, changeset, _, _) in changesets.iter() {
            if let Some(ts) = changeset.ts() {
                let dur = (agent.clock().new_timestamp().get_time() - ts.0).to_duration();
                histogram!("corro.agent.changes.commit.lag.seconds").record(dur);
            }
        }

        debug!("committed {count} changes in {:?}", start.elapsed());

        for (actor_id, knowns) in knowns {
            let booked = {
                bookie
                    .blocking_write(format!(
                        "process_multiple_changes(for_actor_blocking):{}",
                        actor_id.as_simple()
                    ))
                    .ensure(actor_id)
            };
            let mut booked_write = booked.blocking_write(format!(
                "process_multiple_changes(booked writer, post commit):{}",
                actor_id.as_simple()
            ));

            for (versions, known) in knowns {
                let version = *versions.start();
                // this merges partial version seqs
                if let Some(PartialVersion { seqs, last_seq, .. }) =
                    booked_write.insert_many(versions, known)
                {
                    let full_seqs_range = CrsqlSeq(0)..=last_seq;
                    let gaps_count = seqs.gaps(&full_seqs_range).count();
                    if gaps_count == 0 {
                        // if we have no gaps, then we can schedule applying all these changes.
                        debug!(%actor_id, %version, "we now have all versions, notifying for background jobber to insert buffered changes! seqs: {seqs:?}, expected full seqs: {full_seqs_range:?}");
                        let tx_apply = agent.tx_apply().clone();
                        tokio::spawn(async move {
                            if let Err(e) = tx_apply.send((actor_id, version)).await {
                                error!("could not send trigger for applying fully buffered changes later: {e}");
                            }
                        });
                    } else {
                        debug!(%actor_id, %version, "still have {gaps_count} gaps in partially buffered seqs");
                    }
                }
            }
        }

        Ok::<_, ChangeError>(changesets)
    })?;

    for (_actor_id, changeset, db_version, _src) in changesets {
        agent
            .subs_manager()
            .match_changes(changeset.changes(), db_version);
    }

    histogram!("corro.agent.changes.processing.time.seconds").record(start.elapsed());

    Ok(())
}

#[tracing::instrument(skip_all, err)]
pub fn process_single_version(
    agent: &Agent,
    trans: &Transaction,
    last_db_version: Option<CrsqlDbVersion>,
    change: ChangeV1,
) -> rusqlite::Result<(KnownDbVersion, Changeset)> {
    let ChangeV1 {
        actor_id,
        changeset,
    } = change;

    let versions = changeset.versions();

    let (known, changeset) = if changeset.is_complete() {
        let (known, changeset) = util::process_complete_version(
            trans,
            actor_id,
            last_db_version,
            versions,
            changeset
                .into_parts()
                .expect("no changeset parts, this shouldn't be happening!"),
        )?;

        if util::check_buffered_meta_to_clear(trans, actor_id, changeset.versions())? {
            if let Err(e) = agent
                .tx_clear_buf()
                .try_send((actor_id, changeset.versions()))
            {
                error!("could not schedule buffered meta clear: {e}");
            }
        }

        (known, changeset)
    } else {
        let parts = changeset.into_parts().unwrap();
        let known = util::process_incomplete_version(trans, actor_id, &parts)?;

        (known, parts.into())
    };

    Ok((known, changeset))
}
