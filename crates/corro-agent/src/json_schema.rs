use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use sqlite3_parser::ast::{ColumnConstraint, ColumnDefinition};

pub fn col_is_not_null(def: &ColumnDefinition) -> bool {
    def.constraints
        .iter()
        .find_map(|named| match named.constraint {
            ColumnConstraint::NotNull { nullable, .. } => Some(!nullable),
            _ => None,
        })
        .unwrap_or(false)
}

pub fn add_prop(sub_schema: &mut JsonSchema, def: &mut ColumnDefinition) {
    let col_name = quoted_string::strip_dquotes(&def.col_name.0)
        .map(str::to_owned)
        .unwrap_or_else(|| def.col_name.to_string());

    let mut prop = JsonSchema::default();
    let parsed = parse_col(def);

    match parsed {
        ParsedCol::Data { ty, required } => {
            prop.ty = ty;

            if required {
                sub_schema.required.push(col_name.clone());
            }
        }
    }

    sub_schema.properties.insert(col_name.clone(), prop);
}

pub enum ParsedCol {
    Data { ty: JsonSchemaType, required: bool },
}

pub fn parse_col(def: &ColumnDefinition) -> ParsedCol {
    match def
        .col_type
        .as_ref()
        .map(|t| t.name.to_ascii_uppercase())
        .as_deref()
    {
        // https://www.sqlite.org/datatype3.html#determination_of_column_affinity

        // JSON is supported by SQLite, but it's setup as TEXT
        Some("JSON") => ParsedCol::Data {
            ty: JsonSchemaType::Object,
            required: col_is_not_null(def),
        },

        // 1. If the declared type contains the string "INT" then it is assigned INTEGER affinity.
        Some(s) if s.contains("INT") => ParsedCol::Data {
            ty: JsonSchemaType::Integer,
            required: col_is_not_null(def),
        },

        // 2. If the declared type of the column contains any of the strings "CHAR", "CLOB", or "TEXT" then that column has TEXT affinity. Notice that the type VARCHAR contains the string "CHAR" and is thus assigned TEXT affinity.
        Some(s) if s.contains("CHAR") | s.contains("CLOB") | s.contains("TEXT") => {
            ParsedCol::Data {
                ty: JsonSchemaType::String,
                required: col_is_not_null(def),
            }
        }

        // 3. If the declared type for a column contains the string "BLOB" or if no type is specified then the column has affinity BLOB.
        Some(s) if s.contains("BLOB") => ParsedCol::Data {
            ty: JsonSchemaType::Array,
            required: col_is_not_null(def),
        },
        None => ParsedCol::Data {
            ty: JsonSchemaType::Array,
            required: col_is_not_null(def),
        },

        // 4. If the declared type for a column contains any of the strings "REAL", "FLOA", or "DOUB" then the column has REAL affinity.
        Some(s) if s.contains("REAL") || s.contains("FLOA") || s.contains("DOUB") || s == "ANY" => {
            ParsedCol::Data {
                ty: JsonSchemaType::Number,
                required: col_is_not_null(def),
            }
        }

        // 5. Otherwise, the affinity is NUMERIC.
        Some(s) => {
            println!("unhandled column type: '{s}', defaulting to NUMERIC as per Sqlite's docs");
            ParsedCol::Data {
                ty: JsonSchemaType::Number,
                required: col_is_not_null(def),
            }
        }
    }
}

type JsonProperties = HashMap<String, JsonSchema>;

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JsonSchema {
    #[serde(rename = "$schema", skip_serializing_if = "Option::is_none")]
    schema: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    title: Option<String>,
    #[serde(rename = "$id", default, skip_serializing_if = "Option::is_none")]
    id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(rename = "type")]
    ty: JsonSchemaType,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    properties: JsonProperties,
    #[serde(rename = "$defs", default, skip_serializing_if = "HashMap::is_empty")]
    defs: JsonProperties,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    required: Vec<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    default: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    max_length: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    format: Option<JsonStringFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    additional_properties: Option<bool>,
}

impl JsonSchema {
    pub fn new() -> Self {
        Self {
            schema: Some(default_schema()),
            ..Default::default()
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum JsonStringFormat {
    DateTime,
    Time,
    Date,
    Duration,
    Email,
    IdnEmail,
    Hostname,
    IdnHostname,
    Ipv4,
    Ipv6,
    Uuid,
    Uri,
    UriReference,
    Iri,
    IriReference,
    UriTemplate,
    JsonPointer,
    RelativeJsonPointer,
    Regex,
}

fn default_schema() -> String {
    "https://json-schema.org/draft/2020-12/schema".into()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonSchemaType {
    Null,
    Boolean,
    Object,
    Array,
    Number,
    String,
    Integer,
}

impl Default for JsonSchemaType {
    fn default() -> Self {
        JsonSchemaType::Object
    }
}
