//! Contains extensions that make working with the Postgres protocol simpler or more efficient.

use crate::proto::{ConnectionCodec, FormatCode, ProtocolError, RowDescription};
use bytes::{BufMut, BytesMut};
use time::{Date, PrimitiveDateTime};
use tokio_util::codec::Encoder;

/// Supports batched rows for e.g. returning portal result sets.
///
/// NB: this struct only performs limited validation of column consistency across rows.
pub struct DataRowBatch {
    format_code: FormatCode,
    num_cols: usize,
    num_rows: usize,
    data: BytesMut,
    row: BytesMut,
}

impl DataRowBatch {
    /// Creates a new row batch using the given format code, requiring a certain number of columns per row.
    pub fn new(format_code: FormatCode, num_cols: usize) -> Self {
        Self {
            format_code,
            num_cols,
            num_rows: 0,
            data: BytesMut::new(),
            row: BytesMut::new(),
        }
    }

    /// Creates a [DataRowBatch] from the given [RowDescription].
    pub fn from_row_desc(desc: &RowDescription) -> Self {
        Self::new(desc.format_code, desc.fields.len())
    }

    /// Starts writing a new row.
    ///
    /// Returns a [DataRowWriter] that is responsible for the actual value encoding.
    pub fn create_row(&mut self) -> DataRowWriter {
        self.num_rows += 1;
        DataRowWriter::new(self)
    }

    /// Returns the number of rows currently written to this batch.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }
}

macro_rules! primitive_write {
    ($name: ident, $type: ident) => {
        #[allow(missing_docs)]
        pub fn $name(&mut self, val: $type) {
            match self.parent.format_code {
                FormatCode::Text => self.write_value(&val.to_string().into_bytes()),
                FormatCode::Binary => self.write_value(&val.to_be_bytes()),
            };
        }
    };
}

/// Temporarily leased from a [DataRowBatch] to encode a single row.
pub struct DataRowWriter<'a> {
    current_col: usize,
    parent: &'a mut DataRowBatch,
}

impl<'a> DataRowWriter<'a> {
    fn new(parent: &'a mut DataRowBatch) -> Self {
        parent.row.put_i16(parent.num_cols as i16);
        Self {
            current_col: 0,
            parent,
        }
    }

    fn write_value(&mut self, data: &[u8]) {
        self.current_col += 1;
        self.parent.row.put_i32(data.len() as i32);
        self.parent.row.put_slice(data);
    }

    /// Writes a null value for the next column.
    pub fn write_null(&mut self) {
        self.current_col += 1;
        self.parent.row.put_i32(-1);
    }

    /// Writes a string value for the next column.
    pub fn write_string(&mut self, val: &str) {
        self.write_value(val.as_bytes());
    }

    /// Writes a bool value for the next column.
    pub fn write_bool(&mut self, val: bool) {
        match self.parent.format_code {
            FormatCode::Text => self.write_value(if val { "t" } else { "f" }.as_bytes()),
            FormatCode::Binary => {
                self.current_col += 1;
                self.parent.row.put_u8(val as u8);
            }
        };
    }

    fn pg_date_epoch() -> Date {
        Date::from_calendar_date(2000, time::Month::January, 1)
            .expect("failed to create pg date epoch")
    }

    fn pg_timestamp_epoch() -> PrimitiveDateTime {
        Self::pg_date_epoch()
            .with_hms(0, 0, 0)
            .expect("failed to create pg timestamp epoch")
    }

    /// Writes a date value for the next column.
    pub fn write_date(&mut self, val: Date) {
        match self.parent.format_code {
            FormatCode::Binary => {
                self.write_int4((val - Self::pg_date_epoch()).whole_days() as i32)
            }
            FormatCode::Text => self.write_string(&val.to_string()),
        }
    }

    /// Writes a timestamp value for the next column.
    pub fn write_timestamp(&mut self, val: PrimitiveDateTime) {
        match self.parent.format_code {
            FormatCode::Binary => {
                self.write_int8((val - Self::pg_timestamp_epoch()).whole_microseconds() as i64);
            }
            FormatCode::Text => self.write_string(&val.to_string()),
        }
    }

    primitive_write!(write_int2, i16);
    primitive_write!(write_int4, i32);
    primitive_write!(write_int8, i64);
    primitive_write!(write_float4, f32);
    primitive_write!(write_float8, f64);
}

impl<'a> Drop for DataRowWriter<'a> {
    fn drop(&mut self) {
        assert_eq!(
            self.parent.num_cols, self.current_col,
            "dropped a row writer with an invalid number of columns"
        );

        self.parent.data.put_u8(b'D');
        self.parent.data.put_i32((self.parent.row.len() + 4) as i32);
        self.parent.data.extend(self.parent.row.split());
    }
}

impl Encoder<DataRowBatch> for ConnectionCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: DataRowBatch, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.extend(item.data);
        Ok(())
    }
}
