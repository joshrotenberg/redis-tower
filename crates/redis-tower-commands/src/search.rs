use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

use crate::SortOrder;

/// Field type for RediSearch schema definitions.
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
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
#[derive(Clone)]
pub struct FtCreate {
    index: String,
    on_type: Option<OnType>,
    prefixes: Vec<String>,
    fields: Vec<SchemaField>,
}

impl FtCreate {
    /// Create a new [`FtCreate`] command.
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
#[derive(Clone)]
pub struct FtDropIndex {
    index: String,
    dd: bool,
}

impl FtDropIndex {
    /// Create a new [`FtDropIndex`] command.
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
#[derive(Clone)]
pub struct FtAlter {
    index: String,
    fields: Vec<SchemaField>,
}

impl FtAlter {
    /// Create a new [`FtAlter`] command.
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
#[derive(Clone)]
pub struct FtInfo {
    index: String,
}

impl FtInfo {
    /// Create a new [`FtInfo`] command.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// FT._LIST
///
/// Returns a list of all existing search index names.
#[derive(Clone)]
pub struct FtList;

impl FtList {
    /// Create a new [`FtList`] command.
    pub fn new() -> Self {
        Self
    }
}

impl Default for FtList {
    fn default() -> Self {
        Self::new()
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
#[derive(Clone)]
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
    /// Create a new [`FtSearch`] command.
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

    fn idempotent(&self) -> bool {
        true
    }
}

/// FT.AGGREGATE index query \[GROUPBY nargs property ...\]
/// \[REDUCE func nargs arg ...\] \[SORTBY nargs property ASC|DESC ...\]
/// \[LIMIT offset num\] \[APPLY expr AS alias\]
///
/// Runs an aggregation query against the index. Returns a raw `Frame`.
#[derive(Clone)]
pub struct FtAggregate {
    index: String,
    query: String,
    groupby: Vec<String>,
    reduce: Vec<(String, Vec<String>, Option<String>)>,
    sortby: Vec<(String, SortOrder)>,
    limit_offset: Option<u64>,
    limit_num: Option<u64>,
    apply: Vec<(String, String)>,
    with_cursor: bool,
    cursor_count: Option<u64>,
    cursor_maxidle: Option<u64>,
}

impl FtAggregate {
    /// Create a new [`FtAggregate`] command.
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
            with_cursor: false,
            cursor_count: None,
            cursor_maxidle: None,
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

    /// Request a cursor for incremental result retrieval (`WITHCURSOR`).
    ///
    /// Read subsequent batches with [`FtCursorRead`] and release the cursor
    /// with [`FtCursorDel`].
    pub fn with_cursor(mut self) -> Self {
        self.with_cursor = true;
        self
    }

    /// Request a cursor and set its batch size (`WITHCURSOR COUNT n`).
    pub fn with_cursor_count(mut self, count: u64) -> Self {
        self.with_cursor = true;
        self.cursor_count = Some(count);
        self
    }

    /// Set the cursor's idle timeout in milliseconds (`MAXIDLE ms`).
    ///
    /// Implies `WITHCURSOR`.
    pub fn cursor_maxidle(mut self, maxidle: u64) -> Self {
        self.with_cursor = true;
        self.cursor_maxidle = Some(maxidle);
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

        if self.with_cursor {
            args.push(bulk("WITHCURSOR"));
            if let Some(count) = self.cursor_count {
                args.push(bulk("COUNT"));
                args.push(bulk(count.to_string()));
            }
            if let Some(maxidle) = self.cursor_maxidle {
                args.push(bulk("MAXIDLE"));
                args.push(bulk(maxidle.to_string()));
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

/// FT.CURSOR READ index cursor_id \[COUNT n\]
///
/// Reads the next batch of results from a cursor created by an
/// [`FtAggregate`] with `WITHCURSOR`. Returns the same raw `Frame` shape as
/// `FT.AGGREGATE`: an array of `[results, next_cursor_id]`. A `next_cursor_id`
/// of `0` indicates the cursor is exhausted.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::FtCursorRead;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let batch = conn.execute(FtCursorRead::new("idx", 42).count(100)).await?;
/// # let _ = batch;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FtCursorRead {
    index: String,
    cursor_id: u64,
    count: Option<u64>,
}

impl FtCursorRead {
    /// Create a new [`FtCursorRead`] command.
    pub fn new(index: impl Into<String>, cursor_id: u64) -> Self {
        Self {
            index: index.into(),
            cursor_id,
            count: None,
        }
    }

    /// Set the number of results to read in this batch (`COUNT n`).
    pub fn count(mut self, count: u64) -> Self {
        self.count = Some(count);
        self
    }
}

impl Command for FtCursorRead {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.CURSOR"),
            bulk("READ"),
            bulk(self.index.as_str()),
            bulk(self.cursor_id.to_string()),
        ];
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.CURSOR"
    }
}

/// FT.CURSOR DEL index cursor_id
///
/// Releases a cursor created by an [`FtAggregate`] with `WITHCURSOR`. Returns
/// `Ok(())` on success.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::FtCursorDel;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// conn.execute(FtCursorDel::new("idx", 42)).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FtCursorDel {
    index: String,
    cursor_id: u64,
}

impl FtCursorDel {
    /// Create a new [`FtCursorDel`] command.
    pub fn new(index: impl Into<String>, cursor_id: u64) -> Self {
        Self {
            index: index.into(),
            cursor_id,
        }
    }
}

impl Command for FtCursorDel {
    type Response = ();

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.CURSOR"),
            bulk("DEL"),
            bulk(self.index.as_str()),
            bulk(self.cursor_id.to_string()),
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
        "FT.CURSOR"
    }
}

/// FT.ALIASADD alias index
///
/// Adds an alias to a search index.
#[derive(Clone)]
pub struct FtAliasAdd {
    alias: String,
    index: String,
}

impl FtAliasAdd {
    /// Create a new [`FtAliasAdd`] command.
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
#[derive(Clone)]
pub struct FtAliasDel {
    alias: String,
}

impl FtAliasDel {
    /// Create a new [`FtAliasDel`] command.
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
#[derive(Clone)]
pub struct FtAliasUpdate {
    alias: String,
    index: String,
}

impl FtAliasUpdate {
    /// Create a new [`FtAliasUpdate`] command.
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

/// FT.EXPLAIN index query \[DIALECT dialect\]
///
/// Returns the textual execution plan for a search query.
#[derive(Clone)]
pub struct FtExplain {
    index: String,
    query: String,
    dialect: Option<u64>,
}

impl FtExplain {
    /// Create an `FT.EXPLAIN` command for `query`.
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            dialect: None,
        }
    }

    /// Select the query dialect used to parse the query.
    pub fn dialect(mut self, dialect: u64) -> Self {
        self.dialect = Some(dialect);
        self
    }
}

impl Command for FtExplain {
    type Response = String;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.EXPLAIN"),
            bulk(self.index.as_str()),
            bulk(self.query.as_str()),
        ];
        if let Some(dialect) = self.dialect {
            args.push(bulk("DIALECT"));
            args.push(bulk(dialect.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        parse_text(frame, "bulk string containing a query execution plan")
    }

    fn name(&self) -> &str {
        "FT.EXPLAIN"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// FT.EXPLAINCLI index query \[DIALECT dialect\]
///
/// Returns the execution plan as separate, CLI-friendly lines.
#[derive(Clone)]
pub struct FtExplainCli {
    index: String,
    query: String,
    dialect: Option<u64>,
}

impl FtExplainCli {
    /// Create an `FT.EXPLAINCLI` command for `query`.
    pub fn new(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query: query.into(),
            dialect: None,
        }
    }

    /// Select the query dialect used to parse the query.
    pub fn dialect(mut self, dialect: u64) -> Self {
        self.dialect = Some(dialect);
        self
    }
}

impl Command for FtExplainCli {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.EXPLAINCLI"),
            bulk(self.index.as_str()),
            bulk(self.query.as_str()),
        ];
        if let Some(dialect) = self.dialect {
            args.push(bulk("DIALECT"));
            args.push(bulk(dialect.to_string()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|frame| parse_text(frame, "bulk string containing an execution-plan line"))
                .collect(),
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array of execution-plan lines",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.EXPLAINCLI"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

fn parse_text(frame: Frame, expected: &'static str) -> Result<String, RedisError> {
    match frame {
        Frame::BulkString(Some(data))
        | Frame::SimpleString(data)
        | Frame::VerbatimString(_, data) => Ok(String::from_utf8_lossy(&data).into_owned()),
        other => Err(RedisError::UnexpectedResponse {
            expected,
            actual: format!("{other:?}"),
        }),
    }
}

/// K-nearest-neighbor options for [`FtHybrid`].
#[derive(Clone)]
pub struct FtHybridKnn {
    k: u64,
    ef_runtime: Option<u64>,
    shard_k_ratio: Option<f64>,
}

impl FtHybridKnn {
    /// Create a KNN query returning the nearest `k` vector candidates.
    pub fn new(k: u64) -> Self {
        Self {
            k,
            ef_runtime: None,
            shard_k_ratio: None,
        }
    }

    /// Set the runtime search breadth.
    pub fn ef_runtime(mut self, ef_runtime: u64) -> Self {
        self.ef_runtime = Some(ef_runtime);
        self
    }

    /// Set the per-shard candidate ratio used by clustered search.
    ///
    /// This option requires Redis 8.6.1 or later.
    pub fn shard_k_ratio(mut self, shard_k_ratio: f64) -> Self {
        self.shard_k_ratio = Some(shard_k_ratio);
        self
    }

    fn append_args(&self, args: &mut Vec<Frame>) {
        let count = 2
            + usize::from(self.ef_runtime.is_some()) * 2
            + usize::from(self.shard_k_ratio.is_some()) * 2;
        args.push(bulk("KNN"));
        args.push(bulk(count.to_string()));
        args.push(bulk("K"));
        args.push(bulk(self.k.to_string()));
        if let Some(ef_runtime) = self.ef_runtime {
            args.push(bulk("EF_RUNTIME"));
            args.push(bulk(ef_runtime.to_string()));
        }
        if let Some(shard_k_ratio) = self.shard_k_ratio {
            args.push(bulk("SHARD_K_RATIO"));
            args.push(bulk(shard_k_ratio.to_string()));
        }
    }
}

/// Vector-range options for [`FtHybrid`].
#[derive(Clone)]
pub struct FtHybridRange {
    radius: f64,
    epsilon: Option<f64>,
}

impl FtHybridRange {
    /// Create a vector-range query with the maximum distance `radius`.
    pub fn new(radius: f64) -> Self {
        Self {
            radius,
            epsilon: None,
        }
    }

    /// Set the range-query approximation factor.
    pub fn epsilon(mut self, epsilon: f64) -> Self {
        self.epsilon = Some(epsilon);
        self
    }

    fn append_args(&self, args: &mut Vec<Frame>) {
        let count = 2 + usize::from(self.epsilon.is_some()) * 2;
        args.push(bulk("RANGE"));
        args.push(bulk(count.to_string()));
        args.push(bulk("RADIUS"));
        args.push(bulk(self.radius.to_string()));
        if let Some(epsilon) = self.epsilon {
            args.push(bulk("EPSILON"));
            args.push(bulk(epsilon.to_string()));
        }
    }
}

#[derive(Clone)]
enum FtHybridVectorQuery {
    Default,
    Knn(FtHybridKnn),
    Range(FtHybridRange),
}

/// Reciprocal-rank-fusion options for [`FtHybrid`].
#[derive(Clone, Default)]
pub struct FtHybridRrf {
    constant: Option<f64>,
    window: Option<u64>,
    score_alias: Option<String>,
}

impl FtHybridRrf {
    /// Create an RRF configuration using Redis defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the RRF rank constant.
    pub fn constant(mut self, constant: f64) -> Self {
        self.constant = Some(constant);
        self
    }

    /// Set the number of candidates considered from each sub-query.
    pub fn window(mut self, window: u64) -> Self {
        self.window = Some(window);
        self
    }

    /// Alias the combined score for later pipeline operations.
    pub fn yield_score_as(mut self, alias: impl Into<String>) -> Self {
        self.score_alias = Some(alias.into());
        self
    }

    fn append_args(&self, args: &mut Vec<Frame>) {
        let count = usize::from(self.constant.is_some()) * 2
            + usize::from(self.window.is_some()) * 2
            + usize::from(self.score_alias.is_some()) * 2;
        if count == 0 {
            // RRF with all defaults is FT.HYBRID's implicit combine mode.
            return;
        }
        args.push(bulk("COMBINE"));
        args.push(bulk("RRF"));
        args.push(bulk(count.to_string()));
        if let Some(constant) = self.constant {
            args.push(bulk("CONSTANT"));
            args.push(bulk(constant.to_string()));
        }
        if let Some(window) = self.window {
            args.push(bulk("WINDOW"));
            args.push(bulk(window.to_string()));
        }
        if let Some(alias) = &self.score_alias {
            args.push(bulk("YIELD_SCORE_AS"));
            args.push(bulk(alias.as_str()));
        }
    }
}

/// Linear score-fusion options for [`FtHybrid`].
#[derive(Clone)]
pub struct FtHybridLinear {
    alpha: f64,
    beta: f64,
    window: Option<u64>,
    score_alias: Option<String>,
}

impl FtHybridLinear {
    /// Create a linear fusion with text weight `alpha` and vector weight `beta`.
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self {
            alpha,
            beta,
            window: None,
            score_alias: None,
        }
    }

    /// Set the number of candidates considered from each sub-query.
    pub fn window(mut self, window: u64) -> Self {
        self.window = Some(window);
        self
    }

    /// Alias the combined score for later pipeline operations.
    pub fn yield_score_as(mut self, alias: impl Into<String>) -> Self {
        self.score_alias = Some(alias.into());
        self
    }

    fn append_args(&self, args: &mut Vec<Frame>) {
        let count = 4
            + usize::from(self.window.is_some()) * 2
            + usize::from(self.score_alias.is_some()) * 2;
        args.push(bulk("COMBINE"));
        args.push(bulk("LINEAR"));
        args.push(bulk(count.to_string()));
        args.push(bulk("ALPHA"));
        args.push(bulk(self.alpha.to_string()));
        args.push(bulk("BETA"));
        args.push(bulk(self.beta.to_string()));
        if let Some(window) = self.window {
            args.push(bulk("WINDOW"));
            args.push(bulk(window.to_string()));
        }
        if let Some(alias) = &self.score_alias {
            args.push(bulk("YIELD_SCORE_AS"));
            args.push(bulk(alias.as_str()));
        }
    }
}

#[derive(Clone)]
enum FtHybridCombine {
    Default,
    Rrf(FtHybridRrf),
    Linear(FtHybridLinear),
}

#[derive(Clone)]
enum FtHybridLoad {
    None,
    All,
    Fields(Vec<(String, Option<String>)>),
}

/// FT.HYBRID index SEARCH query VSIM @vector_field $vector_param ...
///
/// Combines text and vector similarity search. The constructor binds the
/// required vector parameter to a binary-safe value; additional parameters and
/// pipeline operations can be added with the builder methods.
///
/// The serialized command intentionally uses the released Redis 8.4.4 grammar
/// without the later optional sub-query count, so it works across Redis 8.4.4
/// and newer.
///
/// Pipeline field expressions are serialized exactly as supplied. When a
/// method expects a field or previously yielded score alias, include Redis's
/// required `@` (or JSONPath `$`) sigil.
///
/// Returns the protocol-native response as a raw [`Frame`]: RESP2 uses an array
/// while RESP3 uses a map.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use bytes::Bytes;
/// use redis_tower_commands::{FtHybrid, FtHybridKnn};
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
/// let query_vector = Bytes::from_static(&[0, 0, 0, 0, 0, 0, 0, 0]);
/// let results = conn
///     .execute(
///         FtHybrid::new("products", "laptop", "embedding", "query_vec", query_vector)
///             .knn(FtHybridKnn::new(10))
///             .limit(0, 10),
///     )
///     .await?;
/// # let _ = results;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FtHybrid {
    index: String,
    search_query: String,
    vector_field: String,
    vector_param: String,
    vector: Bytes,
    scorer: Option<String>,
    search_score_alias: Option<String>,
    vector_query: FtHybridVectorQuery,
    vector_filter: Option<String>,
    vector_score_alias: Option<String>,
    combine: FtHybridCombine,
    limit: Option<(u64, u64)>,
    sortby: Option<(String, SortOrder)>,
    nosort: bool,
    load: FtHybridLoad,
    filter: Option<String>,
    params: Vec<(String, Bytes)>,
    timeout: Option<u64>,
}

impl FtHybrid {
    /// Create a hybrid query and bind its required vector parameter.
    pub fn new(
        index: impl Into<String>,
        search_query: impl Into<String>,
        vector_field: impl Into<String>,
        vector_param: impl Into<String>,
        vector: impl Into<Bytes>,
    ) -> Self {
        Self {
            index: index.into(),
            search_query: search_query.into(),
            vector_field: strip_sigil(vector_field.into(), '@'),
            vector_param: strip_sigil(vector_param.into(), '$'),
            vector: vector.into(),
            scorer: None,
            search_score_alias: None,
            vector_query: FtHybridVectorQuery::Default,
            vector_filter: None,
            vector_score_alias: None,
            combine: FtHybridCombine::Default,
            limit: None,
            sortby: None,
            nosort: false,
            load: FtHybridLoad::None,
            filter: None,
            params: Vec::new(),
            timeout: None,
        }
    }

    /// Select the text-search scorer.
    pub fn scorer(mut self, scorer: impl Into<String>) -> Self {
        self.scorer = Some(scorer.into());
        self
    }

    /// Alias the text-search score.
    pub fn search_score_as(mut self, alias: impl Into<String>) -> Self {
        self.search_score_alias = Some(alias.into());
        self
    }

    /// Configure K-nearest-neighbor vector search.
    pub fn knn(mut self, knn: FtHybridKnn) -> Self {
        self.vector_query = FtHybridVectorQuery::Knn(knn);
        self
    }

    /// Configure vector-range search.
    pub fn range(mut self, range: FtHybridRange) -> Self {
        self.vector_query = FtHybridVectorQuery::Range(range);
        self
    }

    /// Apply a pre-filter to the vector sub-query.
    pub fn vector_filter(mut self, filter: impl Into<String>) -> Self {
        self.vector_filter = Some(filter.into());
        self
    }

    /// Alias the vector distance score.
    ///
    /// This uses the standalone alias position supported by Redis 8.4.4 and
    /// newer.
    pub fn vector_score_as(mut self, alias: impl Into<String>) -> Self {
        self.vector_score_alias = Some(alias.into());
        self
    }

    /// Configure reciprocal-rank fusion.
    pub fn rrf(mut self, rrf: FtHybridRrf) -> Self {
        self.combine = FtHybridCombine::Rrf(rrf);
        self
    }

    /// Configure linear score fusion.
    pub fn linear(mut self, linear: FtHybridLinear) -> Self {
        self.combine = FtHybridCombine::Linear(linear);
        self
    }

    /// Limit the returned result window.
    pub fn limit(mut self, offset: u64, num: u64) -> Self {
        self.limit = Some((offset, num));
        self
    }

    /// Sort results by a pipeline field or yielded score alias.
    ///
    /// The field is serialized verbatim; include Redis's required `@` (or
    /// JSONPath `$`) sigil, for example `@combined_score`.
    pub fn sortby(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.sortby = Some((field.into(), order));
        self.nosort = false;
        self
    }

    /// Preserve pipeline order rather than sorting the final results.
    pub fn nosort(mut self) -> Self {
        self.sortby = None;
        self.nosort = true;
        self
    }

    /// Load a field into each result.
    ///
    /// The field is serialized verbatim; include Redis's required `@` (or
    /// JSONPath `$`) sigil.
    pub fn load_field(mut self, field: impl Into<String>) -> Self {
        self.load_fields_mut().push((field.into(), None));
        self
    }

    /// Load a field under an alias.
    ///
    /// The field is serialized verbatim; include Redis's required `@` (or
    /// JSONPath `$`) sigil.
    pub fn load_field_as(mut self, field: impl Into<String>, alias: impl Into<String>) -> Self {
        self.load_fields_mut()
            .push((field.into(), Some(alias.into())));
        self
    }

    /// Load all document fields.
    pub fn load_all(mut self) -> Self {
        self.load = FtHybridLoad::All;
        self
    }

    /// Apply a post-combination pipeline filter.
    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Bind an additional binary-safe query parameter.
    ///
    /// A name matching the required vector parameter replaces its value.
    pub fn param(mut self, name: impl Into<String>, value: impl Into<Bytes>) -> Self {
        let name = strip_sigil(name.into(), '$');
        let value = value.into();
        if name == self.vector_param {
            self.vector = value;
        } else if let Some((_, existing)) = self
            .params
            .iter_mut()
            .find(|(existing, _)| existing == &name)
        {
            *existing = value;
        } else {
            self.params.push((name, value));
        }
        self
    }

    /// Set the query timeout in milliseconds.
    pub fn timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }

    fn load_fields_mut(&mut self) -> &mut Vec<(String, Option<String>)> {
        if !matches!(self.load, FtHybridLoad::Fields(_)) {
            self.load = FtHybridLoad::Fields(Vec::new());
        }
        match &mut self.load {
            FtHybridLoad::Fields(fields) => fields,
            FtHybridLoad::None | FtHybridLoad::All => unreachable!(),
        }
    }

    fn append_query_args(&self, args: &mut Vec<Frame>) {
        args.push(bulk("SEARCH"));
        args.push(bulk(self.search_query.as_str()));
        if let Some(scorer) = &self.scorer {
            args.push(bulk("SCORER"));
            args.push(bulk(scorer.as_str()));
        }
        if let Some(alias) = &self.search_score_alias {
            args.push(bulk("YIELD_SCORE_AS"));
            args.push(bulk(alias.as_str()));
        }

        args.push(bulk("VSIM"));
        args.push(bulk(format!("@{}", self.vector_field)));
        args.push(bulk(format!("${}", self.vector_param)));
        match &self.vector_query {
            FtHybridVectorQuery::Default => {}
            FtHybridVectorQuery::Knn(knn) => knn.append_args(args),
            FtHybridVectorQuery::Range(range) => range.append_args(args),
        }
        if let Some(filter) = &self.vector_filter {
            // The uncounted FILTER form is accepted by Redis 8.4.4 and remains
            // supported by newer versions for backward compatibility.
            args.push(bulk("FILTER"));
            args.push(bulk(filter.as_str()));
        }
        if let Some(alias) = &self.vector_score_alias {
            args.push(bulk("YIELD_SCORE_AS"));
            args.push(bulk(alias.as_str()));
        }
        match &self.combine {
            FtHybridCombine::Default => {}
            FtHybridCombine::Rrf(rrf) => rrf.append_args(args),
            FtHybridCombine::Linear(linear) => linear.append_args(args),
        }

        if let Some((offset, num)) = self.limit {
            args.push(bulk("LIMIT"));
            args.push(bulk(offset.to_string()));
            args.push(bulk(num.to_string()));
        }
        if let Some((field, order)) = &self.sortby {
            args.push(bulk("SORTBY"));
            args.push(bulk("2"));
            args.push(bulk(field.as_str()));
            match order {
                SortOrder::Asc => args.push(bulk("ASC")),
                SortOrder::Desc => args.push(bulk("DESC")),
            }
        } else if self.nosort {
            args.push(bulk("NOSORT"));
        }

        match &self.load {
            FtHybridLoad::None => {}
            FtHybridLoad::All => {
                args.push(bulk("LOAD"));
                args.push(bulk("*"));
            }
            FtHybridLoad::Fields(fields) if !fields.is_empty() => {
                // Redis counts serialized LOAD arguments, so `@field AS alias`
                // contributes three rather than one.
                let count = fields
                    .iter()
                    .map(|(_, alias)| if alias.is_some() { 3 } else { 1 })
                    .sum::<usize>();
                args.push(bulk("LOAD"));
                args.push(bulk(count.to_string()));
                for (field, alias) in fields {
                    args.push(bulk(field.as_str()));
                    if let Some(alias) = alias {
                        args.push(bulk("AS"));
                        args.push(bulk(alias.as_str()));
                    }
                }
            }
            FtHybridLoad::Fields(_) => {}
        }

        if let Some(filter) = &self.filter {
            args.push(bulk("FILTER"));
            args.push(bulk(filter.as_str()));
        }

        args.push(bulk("PARAMS"));
        args.push(bulk(((self.params.len() + 1) * 2).to_string()));
        args.push(bulk(self.vector_param.as_str()));
        args.push(bulk(self.vector.as_ref()));
        for (name, value) in &self.params {
            args.push(bulk(name.as_str()));
            args.push(bulk(value.as_ref()));
        }

        if let Some(timeout) = self.timeout {
            args.push(bulk("TIMEOUT"));
            args.push(bulk(timeout.to_string()));
        }
    }
}

fn strip_sigil(value: String, sigil: char) -> String {
    value.strip_prefix(sigil).unwrap_or(&value).to_string()
}

impl Command for FtHybrid {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("FT.HYBRID"), bulk(self.index.as_str())];
        self.append_query_args(&mut args);
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.HYBRID"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// The query mode profiled by [`FtProfile`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FtProfileType {
    /// Profile an `FT.SEARCH` query.
    Search,
    /// Profile an `FT.HYBRID` query.
    Hybrid,
    /// Profile an `FT.AGGREGATE` query.
    Aggregate,
}

impl FtProfileType {
    fn as_str(self) -> &'static str {
        match self {
            FtProfileType::Search => "SEARCH",
            FtProfileType::Hybrid => "HYBRID",
            FtProfileType::Aggregate => "AGGREGATE",
        }
    }
}

#[derive(Clone)]
enum FtProfileQuery {
    Text(String),
    Hybrid(Box<FtHybrid>),
}

/// FT.PROFILE index SEARCH|HYBRID|AGGREGATE \[LIMITED\] QUERY query
///
/// Runs a `SEARCH`, `HYBRID`, or `AGGREGATE` query and returns both its results
/// and a detailed execution profile. The reply is a complex nested structure
/// returned as a raw [`Frame`]. Hybrid profiling requires Redis 8.8 or later.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::FtProfile;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let profile = conn
///     .execute(FtProfile::search("idx", "hello world").limited())
///     .await?;
/// # let _ = profile;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FtProfile {
    index: String,
    query_type: FtProfileType,
    limited: bool,
    query: FtProfileQuery,
}

impl FtProfile {
    /// Profile an `FT.SEARCH` query.
    pub fn search(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self::new(index, FtProfileType::Search, query)
    }

    /// Profile an `FT.AGGREGATE` query.
    pub fn aggregate(index: impl Into<String>, query: impl Into<String>) -> Self {
        Self::new(index, FtProfileType::Aggregate, query)
    }

    /// Profile an [`FtHybrid`] query.
    ///
    /// Redis profiles the same typed query tail that [`FtHybrid`] would send,
    /// including its binary parameters and pipeline options.
    pub fn hybrid(query: FtHybrid) -> Self {
        Self {
            index: query.index.clone(),
            query_type: FtProfileType::Hybrid,
            limited: false,
            query: FtProfileQuery::Hybrid(Box::new(query)),
        }
    }

    fn new(index: impl Into<String>, query_type: FtProfileType, query: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            query_type,
            limited: false,
            query: FtProfileQuery::Text(query.into()),
        }
    }

    /// Add the `LIMITED` flag, which removes per-record details from the
    /// reply-shadowing profile to reduce its size.
    pub fn limited(mut self) -> Self {
        self.limited = true;
        self
    }
}

impl Command for FtProfile {
    type Response = Frame;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("FT.PROFILE"),
            bulk(self.index.as_str()),
            bulk(self.query_type.as_str()),
        ];
        if self.limited {
            args.push(bulk("LIMITED"));
        }
        args.push(bulk("QUERY"));
        match &self.query {
            FtProfileQuery::Text(query) => args.push(bulk(query.as_str())),
            FtProfileQuery::Hybrid(query) => query.append_query_args(&mut args),
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        Ok(frame)
    }

    fn name(&self) -> &str {
        "FT.PROFILE"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

/// FT.TAGVALS index field_name
///
/// Returns the distinct set of indexed values for a `TAG` field. The values
/// are returned as an array of strings.
///
/// # Example
///
/// ```no_run
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// use redis_tower_commands::FtTagVals;
/// use redis_tower_core::RedisConnection;
///
/// let mut conn = RedisConnection::connect("127.0.0.1:6379").await?;
///
/// let cities = conn.execute(FtTagVals::new("idx", "city")).await?;
/// # let _ = cities;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct FtTagVals {
    index: String,
    field: String,
}

impl FtTagVals {
    /// Create a new [`FtTagVals`] command.
    pub fn new(index: impl Into<String>, field: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            field: field.into(),
        }
    }
}

impl Command for FtTagVals {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        array(vec![
            bulk("FT.TAGVALS"),
            bulk(self.index.as_str()),
            bulk(self.field.as_str()),
        ])
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) | Frame::Set(frames) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) | Frame::SimpleString(data) => {
                        Ok(String::from_utf8_lossy(&data).into_owned())
                    }
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string",
                        actual: format!("{other:?}"),
                    }),
                })
                .collect(),
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "FT.TAGVALS"
    }

    fn idempotent(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_core::Command;
    use redis_tower_protocol::helpers::{array, bulk};

    #[test]
    fn idempotency_flags() {
        // Read-only commands are safe to retry.
        assert!(FtSearch::new("idx", "*").idempotent());
        // Mutating commands keep the default (false).
        assert!(!FtCreate::new("idx").idempotent());
    }

    #[test]
    fn ft_aggregate_with_cursor_count_to_frame() {
        let cmd = FtAggregate::new("idx", "*").with_cursor_count(100);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.AGGREGATE"),
                bulk("idx"),
                bulk("*"),
                bulk("WITHCURSOR"),
                bulk("COUNT"),
                bulk("100"),
            ])
        );
    }

    #[test]
    fn ft_aggregate_with_cursor_maxidle_to_frame() {
        let cmd = FtAggregate::new("idx", "*")
            .with_cursor()
            .cursor_maxidle(5000);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.AGGREGATE"),
                bulk("idx"),
                bulk("*"),
                bulk("WITHCURSOR"),
                bulk("MAXIDLE"),
                bulk("5000"),
            ])
        );
    }

    #[test]
    fn ft_aggregate_without_cursor_unchanged() {
        let cmd = FtAggregate::new("idx", "*");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("FT.AGGREGATE"), bulk("idx"), bulk("*")])
        );
    }

    #[test]
    fn ft_cursor_read_to_frame() {
        let cmd = FtCursorRead::new("idx", 42).count(100);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.CURSOR"),
                bulk("READ"),
                bulk("idx"),
                bulk("42"),
                bulk("COUNT"),
                bulk("100"),
            ])
        );
    }

    #[test]
    fn ft_cursor_read_no_count_to_frame() {
        let cmd = FtCursorRead::new("idx", 7);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.CURSOR"),
                bulk("READ"),
                bulk("idx"),
                bulk("7")
            ])
        );
    }

    #[test]
    fn ft_cursor_del_to_frame() {
        let cmd = FtCursorDel::new("idx", 42);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.CURSOR"),
                bulk("DEL"),
                bulk("idx"),
                bulk("42")
            ])
        );
    }

    #[test]
    fn ft_explain_to_frame_and_parse_response() {
        let cmd = FtExplain::new("idx", "hello world").dialect(2);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.EXPLAIN"),
                bulk("idx"),
                bulk("hello world"),
                bulk("DIALECT"),
                bulk("2"),
            ])
        );
        assert_eq!(
            cmd.parse_response(bulk("INTERSECT {\n  hello\n  world\n}"))
                .unwrap(),
            "INTERSECT {\n  hello\n  world\n}"
        );
        assert_eq!(
            cmd.parse_response(Frame::VerbatimString(
                Bytes::from_static(b"txt"),
                Bytes::from_static(b"hello")
            ))
            .unwrap(),
            "hello"
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn ft_explain_cli_to_frame_and_parse_response() {
        let cmd = FtExplainCli::new("idx", "hello").dialect(4);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.EXPLAINCLI"),
                bulk("idx"),
                bulk("hello"),
                bulk("DIALECT"),
                bulk("4"),
            ])
        );
        assert_eq!(
            cmd.parse_response(array(vec![
                bulk("UNION {"),
                Frame::SimpleString(Bytes::from_static(b"  hello")),
                Frame::VerbatimString(Bytes::from_static(b"txt"), Bytes::from_static(b"}")),
            ]))
            .unwrap(),
            vec!["UNION {", "  hello", "}"]
        );
        assert_eq!(
            cmd.parse_response(Frame::Array(None)).unwrap(),
            Vec::<String>::new()
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn ft_hybrid_minimal_uses_redis_8_4_grammar() {
        let vector = Bytes::from_static(&[0, 0, 128, 63]);
        let cmd = FtHybrid::new("idx", "laptop", "@embedding", "$query_vec", vector.clone());
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.HYBRID"),
                bulk("idx"),
                bulk("SEARCH"),
                bulk("laptop"),
                bulk("VSIM"),
                bulk("@embedding"),
                bulk("$query_vec"),
                bulk("PARAMS"),
                bulk("2"),
                bulk("query_vec"),
                bulk(vector),
            ])
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn ft_hybrid_knn_rrf_pipeline_counts_tokens() {
        let vector = Bytes::from_static(&[0, 0, 128, 63]);
        let cmd = FtHybrid::new("idx", "laptop", "embedding", "query_vec", vector.clone())
            .scorer("BM25")
            .search_score_as("text_score")
            .knn(FtHybridKnn::new(5).ef_runtime(100).shard_k_ratio(1.5))
            .vector_filter("@category:{tech}")
            .vector_score_as("vector_score")
            .rrf(
                FtHybridRrf::new()
                    .constant(50.0)
                    .window(20)
                    .yield_score_as("combined_score"),
            )
            .limit(0, 10)
            .sortby("@combined_score", SortOrder::Desc)
            .load_field("@title")
            .load_field_as("@category", "category")
            .filter("@price > 10")
            .param("$brand", "redis")
            .timeout(1000);
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.HYBRID"),
                bulk("idx"),
                bulk("SEARCH"),
                bulk("laptop"),
                bulk("SCORER"),
                bulk("BM25"),
                bulk("YIELD_SCORE_AS"),
                bulk("text_score"),
                bulk("VSIM"),
                bulk("@embedding"),
                bulk("$query_vec"),
                bulk("KNN"),
                bulk("6"),
                bulk("K"),
                bulk("5"),
                bulk("EF_RUNTIME"),
                bulk("100"),
                bulk("SHARD_K_RATIO"),
                bulk("1.5"),
                bulk("FILTER"),
                bulk("@category:{tech}"),
                bulk("YIELD_SCORE_AS"),
                bulk("vector_score"),
                bulk("COMBINE"),
                bulk("RRF"),
                bulk("6"),
                bulk("CONSTANT"),
                bulk("50"),
                bulk("WINDOW"),
                bulk("20"),
                bulk("YIELD_SCORE_AS"),
                bulk("combined_score"),
                bulk("LIMIT"),
                bulk("0"),
                bulk("10"),
                bulk("SORTBY"),
                bulk("2"),
                bulk("@combined_score"),
                bulk("DESC"),
                bulk("LOAD"),
                bulk("4"),
                bulk("@title"),
                bulk("@category"),
                bulk("AS"),
                bulk("category"),
                bulk("FILTER"),
                bulk("@price > 10"),
                bulk("PARAMS"),
                bulk("4"),
                bulk("query_vec"),
                bulk(vector),
                bulk("brand"),
                bulk("redis"),
                bulk("TIMEOUT"),
                bulk("1000"),
            ])
        );
    }

    #[test]
    fn ft_hybrid_range_linear_load_all_and_nosort() {
        let vector = Bytes::from_static(&[0_u8; 8]);
        let cmd = FtHybrid::new("idx", "*", "embedding", "query_vec", vector.clone())
            .range(FtHybridRange::new(0.8).epsilon(0.1))
            .linear(
                FtHybridLinear::new(0.3, 0.7)
                    .window(40)
                    .yield_score_as("combined_score"),
            )
            .nosort()
            .load_all();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.HYBRID"),
                bulk("idx"),
                bulk("SEARCH"),
                bulk("*"),
                bulk("VSIM"),
                bulk("@embedding"),
                bulk("$query_vec"),
                bulk("RANGE"),
                bulk("4"),
                bulk("RADIUS"),
                bulk("0.8"),
                bulk("EPSILON"),
                bulk("0.1"),
                bulk("COMBINE"),
                bulk("LINEAR"),
                bulk("8"),
                bulk("ALPHA"),
                bulk("0.3"),
                bulk("BETA"),
                bulk("0.7"),
                bulk("WINDOW"),
                bulk("40"),
                bulk("YIELD_SCORE_AS"),
                bulk("combined_score"),
                bulk("NOSORT"),
                bulk("LOAD"),
                bulk("*"),
                bulk("PARAMS"),
                bulk("2"),
                bulk("query_vec"),
                bulk(vector),
            ])
        );
    }

    #[test]
    fn ft_profile_search_to_frame() {
        let cmd = FtProfile::search("idx", "hello");
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.PROFILE"),
                bulk("idx"),
                bulk("SEARCH"),
                bulk("QUERY"),
                bulk("hello"),
            ])
        );
    }

    #[test]
    fn ft_profile_aggregate_limited_to_frame() {
        let cmd = FtProfile::aggregate("idx", "*").limited();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.PROFILE"),
                bulk("idx"),
                bulk("AGGREGATE"),
                bulk("LIMITED"),
                bulk("QUERY"),
                bulk("*"),
            ])
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn ft_profile_hybrid_reuses_typed_query_tail() {
        let vector = Bytes::from_static(&[0, 0, 128, 63]);
        let hybrid = FtHybrid::new("idx", "hello", "embedding", "query_vec", vector.clone())
            .knn(FtHybridKnn::new(3))
            .limit(0, 2);
        let cmd = FtProfile::hybrid(hybrid).limited();
        assert_eq!(
            cmd.to_frame(),
            array(vec![
                bulk("FT.PROFILE"),
                bulk("idx"),
                bulk("HYBRID"),
                bulk("LIMITED"),
                bulk("QUERY"),
                bulk("SEARCH"),
                bulk("hello"),
                bulk("VSIM"),
                bulk("@embedding"),
                bulk("$query_vec"),
                bulk("KNN"),
                bulk("2"),
                bulk("K"),
                bulk("3"),
                bulk("LIMIT"),
                bulk("0"),
                bulk("2"),
                bulk("PARAMS"),
                bulk("2"),
                bulk("query_vec"),
                bulk(vector),
            ])
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn ft_tagvals_to_frame() {
        let cmd = FtTagVals::new("idx", "city");
        assert_eq!(
            cmd.to_frame(),
            array(vec![bulk("FT.TAGVALS"), bulk("idx"), bulk("city")])
        );
        assert!(cmd.idempotent());
    }

    #[test]
    fn ft_tagvals_parse_response() {
        let cmd = FtTagVals::new("idx", "city");
        let reply = array(vec![bulk("london"), bulk("paris")]);
        assert_eq!(
            cmd.parse_response(reply).unwrap(),
            vec!["london".to_string(), "paris".to_string()]
        );
        assert_eq!(
            cmd.parse_response(Frame::Array(None)).unwrap(),
            Vec::<String>::new()
        );
    }
}
