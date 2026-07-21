//! zstd compression for broadcast (and, without dictionaries, sync) wire
//! payloads. See `ZstdDicts` for how dictionary-aware compression works.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    num::NonZeroU32,
    time::Instant,
};

use metrics::{counter, histogram};
use speedy::{Readable, Writable};
use tracing::warn;

use crate::broadcast::ChangeV1;

#[derive(Debug, thiserror::Error)]
pub enum CompressError {
    #[error(transparent)]
    Speedy(#[from] speedy::Error),
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Cheaply checks whether `prefix` (a dictionary candidate's leading bytes)
/// is zstd's dictionary magic number, i.e. looks like an actual trained
/// dictionary rather than some unrelated file. Meant to be checked against
/// just the first few bytes of a candidate file, before reading the whole
/// thing -- not a substitute for the dictionary-id check in
/// `ZstdDicts::new`, since a conformant dictionary can still have no id.
pub fn looks_like_zstd_dict(prefix: &[u8]) -> bool {
    prefix
        .get(..4)
        .map(|prefix| {
            u32::from_le_bytes(prefix.try_into().unwrap()) == zstd::zstd_safe::MAGIC_DICTIONARY
        })
        .unwrap_or(false)
}

/// Trained zstd dictionaries for broadcast compression. `encoder` is the
/// single, explicitly configured dictionary used to compress outgoing
/// broadcasts. `decoders` indexes every known dictionary (the encoder one,
/// plus any extra ones loaded from a scanned directory) by its embedded
/// zstd dictionary ID, so we can keep decoding broadcasts from peers still
/// on an older (or newer) dictionary.
pub struct ZstdDicts {
    encoder: zstd::dict::EncoderDictionary<'static>,
    decoders: HashMap<NonZeroU32, zstd::dict::DecoderDictionary<'static>>,
}

impl ZstdDicts {
    /// `encoder_bytes` is used both to compress outgoing broadcasts and to
    /// decode frames that reference its ID. `extra_decoder_bytes` are
    /// additional dictionaries usable only for decoding.
    pub fn new(encoder_bytes: &[u8], level: i32, extra_decoder_bytes: Vec<Vec<u8>>) -> Self {
        let mut decoders = HashMap::new();

        match zstd::zstd_safe::get_dict_id_from_dict(encoder_bytes) {
            Some(id) => {
                decoders.insert(id, zstd::dict::DecoderDictionary::copy(encoder_bytes));
            }
            None => {
                warn!(
                    "configured compression dict has no embedded dictionary id \
                     (was it trained with `zstd --train`?); broadcasts \
                     compressed with it can't be decoded by dictionary-aware \
                     nodes, including this one"
                );
            }
        }

        for bytes in extra_decoder_bytes {
            match zstd::zstd_safe::get_dict_id_from_dict(&bytes) {
                Some(id) => {
                    decoders
                        .entry(id)
                        .or_insert_with(|| zstd::dict::DecoderDictionary::copy(&bytes));
                }
                None => {
                    warn!("skipping candidate compression dict with no embedded dictionary id");
                }
            }
        }

        Self {
            encoder: zstd::dict::EncoderDictionary::copy(encoder_bytes, level),
            decoders,
        }
    }
}

fn encode_all_with_dict(data: &[u8], level: i32, dicts: Option<&ZstdDicts>) -> io::Result<Vec<u8>> {
    match dicts {
        Some(dicts) => {
            let mut buf = Vec::new();
            let mut encoder =
                zstd::stream::write::Encoder::with_prepared_dictionary(&mut buf, &dicts.encoder)?;
            encoder.write_all(data)?;
            encoder.finish()?;
            Ok(buf)
        }
        None => zstd::stream::encode_all(data, level),
    }
}

/// Decodes `data`, using whichever known dictionary (peeked from compressed bytes) its embedded
/// dictionary ID points to.
fn decode_all_with_dict(data: &[u8], dicts: Option<&ZstdDicts>) -> io::Result<Vec<u8>> {
    let Some(dicts) = dicts else {
        return zstd::stream::decode_all(data);
    };
    match zstd::zstd_safe::get_dict_id_from_frame(data) {
        None => zstd::stream::decode_all(data),
        Some(id) => match dicts.decoders.get(&id) {
            Some(decoder) => {
                let mut decoder_reader =
                    zstd::stream::read::Decoder::with_prepared_dictionary(data, decoder)?;
                let mut buf = Vec::new();
                decoder_reader.read_to_end(&mut buf)?;
                Ok(buf)
            }
            None => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("no known compression dict for id {id}"),
            )),
        },
    }
}

pub fn compress_change(
    change: &ChangeV1,
    level: i32,
    dicts: Option<&ZstdDicts>,
) -> Result<Vec<u8>, CompressError> {
    let encoded = change.write_to_vec()?;
    Ok(encode_all_with_dict(&encoded, level, dicts)?)
}

pub fn decompress_change(
    data: &[u8],
    traffic: &'static str,
    dicts: Option<&ZstdDicts>,
) -> Result<ChangeV1, CompressError> {
    let start = Instant::now();
    let decompressed = match decode_all_with_dict(data, dicts) {
        Ok(bytes) => bytes,
        Err(e) => {
            counter!("corro.decompression.errors.total", "traffic" => traffic).increment(1);
            return Err(e.into());
        }
    };
    histogram!("corro.decompression.time.seconds").record(start.elapsed());
    if decompressed.len() > data.len() {
        counter!("corro.decompression.bytes.extra")
            .increment((decompressed.len() - data.len()) as u64);
    }
    match ChangeV1::read_from_buffer(&decompressed) {
        Ok(change) => Ok(change),
        Err(e) => {
            counter!("corro.decompression.errors.total", "traffic" => traffic).increment(1);
            Err(e.into())
        }
    }
}

/// Result of attempting to compress a change for the wire.
pub enum WireCompression {
    /// Payload shrank; send the compressed bytes.
    Compressed(Vec<u8>),
    /// Compression didn't help; send the original change.
    Uncompressed,
}

/// Try to compress a `ChangeV1` for the wire. Records compression metrics under
/// `traffic` (`"broadcast"` or `"sync"`).
pub fn try_compress_change_for_wire(
    change: &ChangeV1,
    traffic: &'static str,
    level: i32,
    dicts: Option<&ZstdDicts>,
) -> Result<WireCompression, CompressError> {
    let raw_len = change.write_to_vec()?.len();

    let start = Instant::now();
    counter!("corro.compression.attempts.total", "traffic" => traffic).increment(1);
    counter!("corro.compression.bytes.raw.total", "traffic" => traffic).increment(raw_len as u64);

    match compress_change(change, level, dicts)? {
        compressed if compressed.len() < raw_len => {
            let saved_bytes = raw_len - compressed.len();

            histogram!("corro.compression.time.seconds").record(start.elapsed());
            counter!("corro.compression.used.total", "traffic" => traffic).increment(1);
            counter!("corro.compression.bytes.saved", "traffic" => traffic)
                .increment(saved_bytes as u64);
            Ok(WireCompression::Compressed(compressed))
        }
        _ => {
            counter!("corro.compression.useless", "traffic" => traffic).increment(raw_len as u64);
            Ok(WireCompression::Uncompressed)
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;
    use crate::{
        actor::ActorId,
        base::{CrsqlDbVersion, CrsqlSeq},
        broadcast::{BroadcastDecodeError, BroadcastV1, Changeset, Timestamp},
        change::Change,
    };
    use corro_api_types::SqliteValue;
    use corro_base_types::CrsqlSeqRange;

    fn change_with_rows(n: usize) -> ChangeV1 {
        let actor_id = ActorId(Uuid::new_v4());
        let changes = (0..n)
            .map(|i| Change {
                table: "test_table".into(),
                pk: format!("pk-{i}").into_bytes(),
                cid: "some_column".into(),
                val: SqliteValue::Text("a fairly repetitive value".into()),
                col_version: 1,
                db_version: CrsqlDbVersion(1),
                seq: CrsqlSeq(i as u64),
                site_id: actor_id.to_bytes(),
                cl: 1,
            })
            .collect::<Vec<_>>();

        ChangeV1 {
            actor_id,
            changeset: Changeset::Full {
                version: CrsqlDbVersion(1),
                changes,
                seqs: CrsqlSeqRange::new(CrsqlSeq(0), CrsqlSeq(n.max(1) as u64 - 1)),
                last_seq: CrsqlSeq(n.max(1) as u64 - 1),
                ts: Timestamp::zero(),
            },
        }
    }

    #[test]
    fn test_looks_like_zstd_dict() {
        let samples: Vec<Vec<u8>> = (1..200)
            .map(|n| change_with_rows(n).write_to_vec().unwrap())
            .collect();
        let trained = zstd::dict::from_samples(&samples, 16 * 1024).unwrap();

        assert!(looks_like_zstd_dict(&trained));
        assert!(!looks_like_zstd_dict(b"not a dictionary"));
        assert!(!looks_like_zstd_dict(b""));
        assert!(!looks_like_zstd_dict(&[0u8; 3]));
    }

    #[test]
    fn test_compress_change_compresses_and_roundtrips() {
        let change = change_with_rows(200);
        let raw_len = change.write_to_vec().unwrap().len();
        let bcast = BroadcastV1::Change(change.clone()).compress_for_wire(3, None);

        let BroadcastV1::CompressedChange(compressed) = bcast else {
            panic!("expected a large, repetitive change to compress");
        };
        let compressed_len = compressed.len();

        assert!(
            compressed_len < raw_len,
            "compressed size ({compressed_len}) should be smaller than raw ({raw_len})"
        );

        let ratio = (1.0 - compressed_len as f64 / raw_len as f64) * 100.0;
        eprintln!(
            "broadcast change compression: raw={raw_len}B compressed={compressed_len}B ratio={ratio:.1}%"
        );

        let decoded = BroadcastV1::CompressedChange(compressed)
            .into_change(None)
            .unwrap();
        assert_eq!(decoded, change);
    }

    fn trained_dict_bytes() -> Vec<u8> {
        let samples: Vec<Vec<u8>> = (1..200)
            .map(|n| change_with_rows(n).write_to_vec().unwrap())
            .collect();
        zstd::dict::from_samples(&samples, 16 * 1024).unwrap()
    }

    #[test]
    fn test_compress_change_with_dict_roundtrips() {
        let change = change_with_rows(50);
        let dicts = ZstdDicts::new(&trained_dict_bytes(), 3, vec![]);

        let bcast = BroadcastV1::Change(change.clone()).compress_for_wire(3, Some(&dicts));
        let BroadcastV1::CompressedChange(compressed) = bcast else {
            panic!("expected change to compress with a dict");
        };

        let decoded = BroadcastV1::CompressedChange(compressed)
            .into_change(Some(&dicts))
            .unwrap();
        assert_eq!(decoded, change);
    }

    #[test]
    fn test_decode_with_older_dict_via_directory_scan() {
        // Simulate a rotation: encode with the "old" dict, but the decoder
        // was configured with a "new" primary dict and only knows the old
        // one via its extra (directory-scanned) dictionaries.
        let old_dict_bytes = trained_dict_bytes();
        let new_dict_bytes = {
            let samples: Vec<Vec<u8>> = (200..400)
                .map(|n| change_with_rows(n).write_to_vec().unwrap())
                .collect();
            zstd::dict::from_samples(&samples, 16 * 1024).unwrap()
        };

        let encoder_dicts = ZstdDicts::new(&old_dict_bytes, 3, vec![]);
        let decoder_dicts = ZstdDicts::new(&new_dict_bytes, 3, vec![old_dict_bytes]);

        let change = change_with_rows(50);
        let bcast = BroadcastV1::Change(change.clone()).compress_for_wire(3, Some(&encoder_dicts));
        let BroadcastV1::CompressedChange(compressed) = bcast else {
            panic!("expected change to compress with a dict");
        };

        let decoded = BroadcastV1::CompressedChange(compressed)
            .into_change(Some(&decoder_dicts))
            .unwrap();
        assert_eq!(decoded, change);
    }

    #[test]
    fn test_decode_fails_for_unknown_dict_id() {
        let old_dict_bytes = trained_dict_bytes();
        let encoder_dicts = ZstdDicts::new(&old_dict_bytes, 3, vec![]);
        // decoder doesn't know about `old_dict_bytes` at all
        let new_dict_bytes = {
            let samples: Vec<Vec<u8>> = (200..400)
                .map(|n| change_with_rows(n).write_to_vec().unwrap())
                .collect();
            zstd::dict::from_samples(&samples, 16 * 1024).unwrap()
        };
        let decoder_dicts = ZstdDicts::new(&new_dict_bytes, 3, vec![]);

        let change = change_with_rows(50);
        let bcast = BroadcastV1::Change(change.clone()).compress_for_wire(3, Some(&encoder_dicts));
        let BroadcastV1::CompressedChange(compressed) = bcast else {
            panic!("expected change to compress with a dict");
        };

        let err = BroadcastV1::CompressedChange(compressed)
            .into_change(Some(&decoder_dicts))
            .unwrap_err();
        assert!(matches!(err, BroadcastDecodeError::Compress(_)));
    }
}
