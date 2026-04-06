use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::SortOrder;

/// Field type for RediSearch schema definitions.
pub enum FieldType {
    /// Full-text searchable field.
    Text,
    /// Numeric range-queryable field.
    Numeric,
    /// Tag field for exact-match filtering.
    Tag,
    /// Geo-spatial field (longitude, latitude).
    Geo,
    /// Vector similarity field.
    Vector,
}

impl FieldType {
    fn as_str(&self) -> &str {
        match self {
            FieldType::Text => "TEXT",
            FieldType::Numeric => "NUMERIC",
            FieldType::Tag => "TAG",
            FieldType::Geo => "GEO",
            FieldType::Vector => "VECTOR",
        }
    }
}

/// A field definition for a RediSearch schema.
pub struct SchemaField {
    /// Field name.
    pub name: String,
    /// Field type.
    pub field_type: FieldType,
    /// Whether the field is sortable.
    pub sortable: bool,
    /// Whether to skip indexing this field.
    pub noindex: bool,
}

/// Data structure type for FT.CREATE.
pub enum OnType {
    /// Index HASH keys.
    Hash,
    /// Index JSON keys.
    Json,
}

/// FT.CREATE index \[ON HASH|JSON\] \[PREFIX count prefix ...\] SCHEMA field type ...
///
/// Creates a new search index with the given schema. Uses a builder pattern
/// for constructing the index definition.
pub struct FtCreate {
    index: String,
    on_type: Option<OnType>,
    prefixes: Vec<String>,
    fields: Vec<SchemaField>,
}

impl FtCreate {
    pub fn new(index: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            on_type: None,
            prefixes: Vec::new(),
            fields: Vec::new(),
        }
    }

    /// Index HASH keys.
    pub fn on_hash(mut self) -> Self {
        self.on_type = Some(OnType::Hash);
        self
    }

    /// Index JSON keys.
    pub fn on_json(mut self) -> Self {
        self.on_type = Some(OnType::Json);
        self
    }

    /// Add a key prefix filter.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefixes.push(prefix.into());
        self
    }

    /// Add a field to the schema.
    pub fn field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.push(SchemaField {
            name: name.into(),
            field_type,
            sortable: false,
            noindex: false,
        });
        self
    }

    /// Add a sortable field to the schema.
    pub fn sortable_field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.push(SchemaField {
            name: name.into(),
            field_type,
            sortable: true,
            noindex: false,
        });
        self
    }

    /// Add a field with full options.
    pub fn schema_field(mut self, field: SchemaField) -> Self {
        self.fields.push(field);
        self
    }
}

impl Command for FtCreate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FT.CREATE"), bulk(self.index.as_str())];

        if let Some(on_type) = &self.on_type {
            args.push(bulk("ON"));
            match on_type {
                OnType::Hash => args.push(bulk("HASH")),
                OnType::Json => args.push(bulk("JSON")),
            }
        }

        if !self.prefixes.is_empty() {
            args.push(bulk("PREFIX"));
            args.push(bulk(self.prefixes.len().to_string()));
            for prefix in &self.prefixes {
                args.push(bulk(prefix.as_str()));
            }
        }

        args.push(bulk("SCHEMA"));
        for field in &self.fields {
            args.push(bulk(field.name.as_str()));
            args.push(bulk(field.field_type.as_str()));
            if field.sortable {
                args.push(bulk("SORTABLE"));
            }
            if field.noindex {
                args.push(bulk("NOINDEX"));
            }
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.CREATE"
    }
}

/// FT.DROPINDEX index \[DD\]
///
/// Deletes a search index. With `DD`, also deletes the indexed documents.
pub struct FtDropIndex {
    index: String,
    dd: bool,
}

impl FtDropIndex {
    pub fn new(index: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            dd: false,
        }
    }

    /// Also delete the indexed documents.
    pub fn dd(mut self) -> Self {
        self.dd = true;
        self
    }
}

impl Command for FtDropIndex {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FT.DROPINDEX"), bulk(self.index.as_str())];
        if self.dd {
            args.push(bulk("DD"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.DROPINDEX"
    }
}

/// FT.ALTER index SCHEMA ADD field type ...
///
/// Adds new fields to an existing index schema.
pub struct FtAlter {
    index: String,
    fields: Vec<SchemaField>,
}

impl FtAlter {
    pub fn new(index: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            fields: Vec::new(),
        }
    }

    /// Add a field to the schema.
    pub fn field(mut self, name: impl Into<String>, field_type: FieldType) -> Self {
        self.fields.push(SchemaField {
            name: name.into(),
            field_type,
            sortable: false,
            noindex: false,
        });
        self
    }

    /// Add a field with full options.
    pub fn schema_field(mut self, field: SchemaField) -> Self {
        self.fields.push(field);
        self
    }
}

impl Command for FtAlter {
    type Response = ();

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.ALTER"),
            bulk(self.index.as_str()),
            bulk("SCHEMA"),
            bulk("ADD"),
        ];
        for field in &self.fields {
            args.push(bulk(field.name.as_str()));
            args.push(bulk(field.field_type.as_str()));
            if field.sortable {
                args.push(bulk("SORTABLE"));
            }
            if field.noindex {
                args.push(bulk("NOINDEX"));
            }
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.ALTER"
    }
}

/// FT.INFO index
///
/// Returns information and statistics about a search index. The response is
/// a complex nested structure returned as a raw `Frame`.
pub struct FtInfo {
    index: String,
}

impl FtInfo {
    pub fn new(index: impl Into<String>) -> Self {
        Self {
            index: index.into(),
        }
    }
}

impl Command for FtInfo {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FT.INFO"), bulk(self.index.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.INFO"
    }
}

/// FT._LIST
///
/// Returns a list of all existing search index names.
pub struct FtList;

impl FtList {
    pub fn new() -> Self {
        Self
    }
}

impl Command for FtList {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FT._LIST")])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => Ok(data),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT._LIST"
    }
}

/// FT.SEARCH index query \[LIMIT offset num\] \[RETURN count field ...\]
/// \[SORTBY field ASC|DESC\] \[NOCONTENT\] \[VERBATIM\] \[WITHSCORES\]
///
/// Searches the index with the given query. Uses a builder pattern for
/// optional parameters. Returns a raw `Frame` containing the result count
/// and document array.
pub struct FtSearch {
    index: String,
    query: String,
    limit_offset: Option<u64>,
    limit_num: Option<u64>,
    return_fields: Vec<String>,
    sortby: Option<(String, SortOrder)>,
    nocontent: bool,
    verbatim: bool,
    withscores: bool,
}

impl FtSearch {
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            limit_offset: None,
            limit_num: None,
            return_fields: Vec::new(),
            sortby: None,
            nocontent: false,
            verbatim: false,
            withscores: false,
        }
    }

    /// Set the LIMIT clause with offset and number of results.
    pub fn limit(mut self, offset: u64, num: u64) -> Self {
        self.limit_offset = Some(offset);
        self.limit_num = Some(num);
        self
    }

    /// Set the fields to return.
    pub fn return_fields(mut self, fields: &[impl AsRef<str>]) -> Self {
        self.return_fields = fields.iter().map(|f| f.as_ref().to_string()).collect();
        self
    }

    /// Sort results by a field.
    pub fn sortby(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.sortby = Some((field.into(), order));
        self
    }

    /// Return only document IDs, not content.
    pub fn nocontent(mut self) -> Self {
        self.nocontent = true;
        self
    }

    /// Do not try to use stemming for query expansion.
    pub fn verbatim(mut self) -> Self {
        self.verbatim = true;
        self
    }

    /// Include scores in the results.
    pub fn withscores(mut self) -> Self {
        self.withscores = true;
        self
    }
}

impl Command for FtSearch {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.SEARCH"),
            bulk(self.index.as_str()),
            bulk(self.query.as_str()),
        ];

        if self.nocontent {
            args.push(bulk("NOCONTENT"));
        }
        if self.verbatim {
            args.push(bulk("VERBATIM"));
        }
        if self.withscores {
            args.push(bulk("WITHSCORES"));
        }

        if let Some(offset) = self.limit_offset {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            if let Some(num) = self.limit_num {
                args.push(bulk(num.to_string()));
            }
        }

        if !self.return_fields.is_empty() {
            args.push(bulk("RETURN"));
            args.push(bulk(self.return_fields.len().to_string()));
            for field in &self.return_fields {
                args.push(bulk(field.as_str()));
            }
        }

        if let Some((field, order)) = &self.sortby {
            args.push(bulk("SORTBY"));
            args.push(bulk(field.as_str()));
            match order {
                SortOrder::Asc => args.push(bulk("ASC")),
                SortOrder::Desc => args.push(bulk("DESC")),
            }
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.SEARCH"
    }
}

/// FT.AGGREGATE index query \[GROUPBY nargs property ...\]
/// \[REDUCE func nargs arg ...\] \[SORTBY nargs property ASC|DESC ...\]
/// \[LIMIT offset num\] \[APPLY expr AS alias\]
///
/// Runs an aggregation query against the index. Returns a raw `Frame`.
pub struct FtAggregate {
    index: String,
    query: String,
    groupby: Vec<String>,
    reduce: Vec<(String, Vec<String>, Option<String>)>,
    sortby: Vec<(String, SortOrder)>,
    limit_offset: Option<u64>,
    limit_num: Option<u64>,
    apply: Vec<(String, String)>,
}

impl FtAggregate {
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            groupby: Vec::new(),
            reduce: Vec::new(),
            sortby: Vec::new(),
            limit_offset: None,
            limit_num: None,
            apply: Vec::new(),
        }
    }

    /// Add a GROUPBY property.
    pub fn groupby(mut self, properties: &[impl AsRef<str>]) -> Self {
        self.groupby = properties.iter().map(|p| p.as_ref().to_string()).collect();
        self
    }

    /// Add a REDUCE function with arguments and optional alias.
    pub fn reduce(
        mut self,
        func: impl Into<String>,
        args: &[impl AsRef<str>],
        alias: Option<impl Into<String>>,
    ) -> Self {
        self.reduce.push((
            func.into(),
            args.iter().map(|a| a.as_ref().to_string()).collect(),
            alias.map(Into::into),
        ));
        self
    }

    /// Add a SORTBY field with order.
    pub fn sortby(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.sortby.push((field.into(), order));
        self
    }

    /// Set the LIMIT clause.
    pub fn limit(mut self, offset: u64, num: u64) -> Self {
        self.limit_offset = Some(offset);
        self.limit_num = Some(num);
        self
    }

    /// Add an APPLY expression with an alias.
    pub fn apply(mut self, expr: impl Into<String>, alias: impl Into<String>) -> Self {
        self.apply.push((expr.into(), alias.into()));
        self
    }
}

impl Command for FtAggregate {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.AGGREGATE"),
            bulk(self.index.as_str()),
            bulk(self.query.as_str()),
        ];

        if !self.groupby.is_empty() {
            args.push(bulk("GROUPBY"));
            args.push(bulk(self.groupby.len().to_string()));
            for prop in &self.groupby {
                args.push(bulk(prop.as_str()));
            }

            for (func, func_args, alias) in &self.reduce {
                args.push(bulk("REDUCE"));
                args.push(bulk(func.as_str()));
                args.push(bulk(func_args.len().to_string()));
                for arg in func_args {
                    args.push(bulk(arg.as_str()));
                }
                if let Some(alias) = alias {
                    args.push(bulk("AS"));
                    args.push(bulk(alias.as_str()));
                }
            }
        }

        if !self.sortby.is_empty() {
            args.push(bulk("SORTBY"));
            // nargs = 2 * number of fields (field + order)
            args.push(bulk((self.sortby.len() * 2).to_string()));
            for (field, order) in &self.sortby {
                args.push(bulk(field.as_str()));
                match order {
                    SortOrder::Asc => args.push(bulk("ASC")),
                    SortOrder::Desc => args.push(bulk("DESC")),
                }
            }
        }

        for (expr, alias) in &self.apply {
            args.push(bulk("APPLY"));
            args.push(bulk(expr.as_str()));
            args.push(bulk("AS"));
            args.push(bulk(alias.as_str()));
        }

        if let Some(offset) = self.limit_offset {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            if let Some(num) = self.limit_num {
                args.push(bulk(num.to_string()));
            }
        }

        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.AGGREGATE"
    }
}

/// FT.ALIASADD alias index
///
/// Adds an alias to a search index.
pub struct FtAliasAdd {
    alias: String,
    index: String,
}

impl FtAliasAdd {
    pub fn new(alias: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            index: index.into(),
        }
    }
}

impl Command for FtAliasAdd {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.ALIASADD"),
            bulk(self.alias.as_str()),
            bulk(self.index.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.ALIASADD"
    }
}

/// FT.ALIASDEL alias
///
/// Removes an alias from a search index.
pub struct FtAliasDel {
    alias: String,
}

impl FtAliasDel {
    pub fn new(alias: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
        }
    }
}

impl Command for FtAliasDel {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![bulk("FT.ALIASDEL"), bulk(self.alias.as_str())])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.ALIASDEL"
    }
}

/// FT.ALIASUPDATE alias index
///
/// Updates an alias to point to a different search index.
pub struct FtAliasUpdate {
    alias: String,
    index: String,
}

impl FtAliasUpdate {
    pub fn new(alias: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            alias: alias.into(),
            index: index.into(),
        }
    }
}

impl Command for FtAliasUpdate {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.ALIASUPDATE"),
            bulk(self.alias.as_str()),
            bulk(self.index.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::SimpleString(s) if &s[..] == b"OK" => Ok(()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "OK",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.ALIASUPDATE"
    }
}
