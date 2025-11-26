//! Reimpl of PgWireMessageServerCodec from <https://github.com/sunng87/pgwire/blob/a12c5f28aef8ada3e13a387da9f9dd5a70cfea41/src/tokio/server.rs>,
//! the implementation was public, but was made (accidentally?) private in <https://github.com/sunng87/pgwire/pull/214>

use pgwire::{
    api::ClientInfo,
    tokio::server::PgWireMessageServerCodec,
    types::{format::FormatOptions, FromSqlText},
};
use postgres_types::Type;
use tokio_util::codec;
use tracing::debug;

pub(super) trait SetState {
    fn set_state(&mut self, state: pgwire::api::PgWireConnectionState);
}

impl<T: 'static, S> SetState for codec::Framed<T, PgWireMessageServerCodec<S>> {
    fn set_state(&mut self, state: pgwire::api::PgWireConnectionState) {
        self.codec_mut().client_info.set_state(state);
    }
}

impl<L, R> SetState for tokio_util::either::Either<L, R>
where
    L: SetState,
    R: SetState,
{
    fn set_state(&mut self, state: pgwire::api::PgWireConnectionState) {
        match self {
            Self::Left(l) => l.set_state(state),
            Self::Right(r) => r.set_state(state),
        }
    }
}

pub trait VecFromSqlText: Sized {
    fn from_vec_sql_text(
        ty: &Type,
        input: &[u8],
        format: &FormatOptions,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>>;
}

// Re-implementation of the ToSqlText trait from pg_wire to make it generic over different types.
// Implemented as a macro in pgwire
// https://github.com/sunng87/pgwire/blob/6cbce9d444cc86a01d992f6b35f84c024f10ceda/src/types/from_sql_text.rs#L402
impl<T> VecFromSqlText for Vec<T>
where
    for<'a> T: FromSqlText<'a>,
{
    fn from_vec_sql_text(
        ty: &Type,
        input: &[u8],
        format: &FormatOptions,
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        // PostgreSQL array text format: {elem1,elem2,elem3}
        // Remove the outer braces
        let input_str = std::str::from_utf8(input)?;

        if input_str.is_empty() {
            return Ok(Vec::new());
        }

        // Check if it's an array format
        if !input_str.starts_with('{') || !input_str.ends_with('}') {
            return Err(format!(
                "Invalid array format: must start with '{{' and end with '}}', input: {input_str}"
            )
            .into());
        }

        let inner = &input_str[1..input_str.len() - 1];

        if inner.is_empty() {
            return Ok(Vec::new());
        }

        let elements = extract_array_elements(inner)?
            .into_iter()
            .map(|s| s.into_bytes());
        let mut result = Vec::new();

        for element_str in elements {
            let element = T::from_sql_text(ty, &element_str, format)?;
            result.push(element);
        }

        Ok(result)
    }
}

// Helper function to extract array elements
// https://github.com/sunng87/pgwire/blob/6cbce9d444cc86a01d992f6b35f84c024f10ceda/src/types/from_sql_text.rs#L402
fn extract_array_elements(
    input: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Sync + Send>> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let mut elements = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut escape_next = false;
    let mut seen_content = false; // helpful for tracking when the last element is an empty string

    for ch in input.chars() {
        match ch {
            '\\' if !escape_next => {
                escape_next = true;
            }
            '"' if !escape_next => {
                in_quotes = !in_quotes;
                // we have seen a new element surrounded by quotes
                if !in_quotes {
                    seen_content = true;
                }
                // Don't include the quotes in the output
            }
            '{' if !in_quotes && !escape_next => {
                return Err("Nested arrays are not supported".into());
            }
            '}' if !in_quotes && !escape_next => {
                return Err("Nested arrays are not supported".into());
            }
            ',' if !in_quotes && !escape_next => {
                // End of current element
                if !current.trim().eq_ignore_ascii_case("NULL") {
                    elements.push(std::mem::take(&mut current));
                    seen_content = false;
                }
            }
            _ => {
                current.push(ch);
                escape_next = false;
                seen_content = true;
            }
        }
    }

    // Process the last element
    if seen_content && !current.trim().eq_ignore_ascii_case("NULL") {
        elements.push(current);
    }

    debug!(
        "extracted elements: {elements:?} from input: {input}, lenght: {}",
        elements.len()
    );
    Ok(elements)
}
