use time::PlainDateTime;

fn fmt_desc() -> &'static time::format_description::OwnedFormatItem {
    static mut FMT: time::format_description::OwnedFormatItem =
        time::format_description::OwnedFormatItem::Component(
            time::format_description::Component::End(
                time::format_description::modifier::End::default(),
            ),
        );
    static INIT: std::sync::Once = std::sync::Once::new();

    unsafe {
        INIT.call_once(|| {
            // pgwire doesn't support time, only chrono, which is unmaintained, so for now we manually implement this
            // until either time is supported, or jiff 1.0 is:
            //
            // - https://github.com/sunng87/pgwire/pull/491
            // - https://github.com/rust-postgres/rust-postgres/pull/1405
            // - https://github.com/rusqlite/rusqlite/pull/1963
            //
            // https://github.com/sunng87/pgwire/blob/c01c5cf69b326cc1004aae17af4fda877001f3bb/src/types/format/date_style.rs#L95
            FMT = time::format_description::parse_strftime_owned("%Y-%m-%d %H:%M:%S%.6f")
                .expect("failed to parse strftime string");
        });
        #[allow(static_mut_refs)]
        &FMT
    }
}

pub(super) fn from_sql(
    typ: &postgres_types::Type,
    val: &[u8],
    format: crate::FormatCode,
) -> Result<PlainDateTime, crate::ToParamError<time::Error>> {
    match format {
        crate::FormatCode::Text => {
            let s = std::str::from_utf8(val)?;
            PlainDateTime::parse(s, fmt_desc())
                .map_err(|err| crate::ToParamError::Parse(err.into()))
        }
        crate::FormatCode::Binary => {
            use postgres_types::FromSql;
            PlainDateTime::from_sql(typ, val).map_err(crate::ToParamError::<time::Error>::FromSql)
        }
    }
}

pub(super) struct OurDateTime(PlainDateTime);

impl std::fmt::Debug for OurDateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl pgwire::types::ToSqlText for OurDateTime {
    fn to_sql_text(
        &self,
        ty: &pgwire::api::Type,
        out: &mut bytes::BytesMut,
        _format_options: &pgwire::types::format::FormatOptions,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        assert!(
            matches!(ty, &pgwire::api::Type::TIMESTAMP),
            "this is the only timestamp type corro-pg uses"
        );

        use bytes::BufMut;
        self.0.format_into(&mut out.writer(), fmt_desc())?;
        Ok(postgres_types::IsNull::No)
    }
}

impl rusqlite::types::FromSql for OurDateTime {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        PlainDateTime::column_result(value).map(Self)
    }
}

impl postgres_types::ToSql for OurDateTime {
    fn to_sql(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>>
    where
        Self: Sized,
    {
        self.0.to_sql(ty, out)
    }

    fn to_sql_checked(
        &self,
        ty: &postgres_types::Type,
        out: &mut bytes::BytesMut,
    ) -> Result<postgres_types::IsNull, Box<dyn std::error::Error + Sync + Send>> {
        self.0.to_sql_checked(ty, out)
    }

    fn accepts(ty: &postgres_types::Type) -> bool
    where
        Self: Sized,
    {
        PlainDateTime::accepts(ty)
    }
}
