// The sha of the commit that this version of crsqlite was built from.
pub const SHA: &str = match core::option_env!("CRSQLITE_COMMIT_SHA") {
    Some(sha) => sha,
    None => "unknown",
};
