use bytes::Bytes;
use redis_tower_core::{Command, Frame, RedisError};
use redis_tower_protocol::helpers::{array, bulk};

/// Unit of distance for geospatial commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnit {
    /// Meters
    Meters,
    /// Kilometers
    Kilometers,
    /// Feet
    Feet,
    /// Miles
    Miles,
}

impl GeoUnit {
    fn as_str(&self) -> &str {
        match self {
            GeoUnit::Meters => "M",
            GeoUnit::Kilometers => "KM",
            GeoUnit::Feet => "FT",
            GeoUnit::Miles => "MI",
        }
    }
}

/// GEOADD key \[NX|XX\] \[CH\] longitude latitude member \[longitude latitude member ...\]
///
/// Adds the specified geospatial items (longitude, latitude, name) to the
/// specified key. Returns the number of elements added to the sorted set
/// (excluding score updates when `CH` is not set).
pub struct GeoAdd {
    key: String,
    members: Vec<(f64, f64, String)>,
    nx: bool,
    xx: bool,
    ch: bool,
}

impl GeoAdd {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: Vec::new(),
            nx: false,
            xx: false,
            ch: false,
        }
    }

    /// Adds a member with the given longitude and latitude.
    pub fn member(mut self, longitude: f64, latitude: f64, name: impl Into<String>) -> Self {
        self.members.push((longitude, latitude, name.into()));
        self
    }

    /// Only add new elements. Do not update already existing elements.
    pub fn nx(mut self) -> Self {
        self.nx = true;
        self.xx = false;
        self
    }

    /// Only update elements that already exist. Do not add new elements.
    pub fn xx(mut self) -> Self {
        self.xx = true;
        self.nx = false;
        self
    }

    /// Modify the return value from the number of new elements added to the
    /// total number of elements changed (including score updates).
    pub fn ch(mut self) -> Self {
        self.ch = true;
        self
    }
}

impl Command for GeoAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("GEOADD"), bulk(self.key.as_str())];
        if self.nx {
            args.push(bulk("NX"));
        } else if self.xx {
            args.push(bulk("XX"));
        }
        if self.ch {
            args.push(bulk("CH"));
        }
        for (longitude, latitude, name) in &self.members {
            args.push(bulk(longitude.to_string()));
            args.push(bulk(latitude.to_string()));
            args.push(bulk(name.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "GEOADD"
    }
}

/// GEODIST key member1 member2 \[M|KM|FT|MI\]
///
/// Returns the distance between two members of a geospatial index. The
/// distance is returned as a floating-point number in the specified unit,
/// or `None` if one or both members are missing.
pub struct GeoDist {
    key: String,
    member1: String,
    member2: String,
    unit: Option<GeoUnit>,
}

impl GeoDist {
    pub fn new(
        key: impl Into<String>,
        member1: impl Into<String>,
        member2: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            member1: member1.into(),
            member2: member2.into(),
            unit: None,
        }
    }

    /// Sets the unit of the returned distance.
    pub fn unit(mut self, unit: GeoUnit) -> Self {
        self.unit = Some(unit);
        self
    }
}

impl Command for GeoDist {
    type Response = Option<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("GEODIST"),
            bulk(self.key.as_str()),
            bulk(self.member1.as_str()),
            bulk(self.member2.as_str()),
        ];
        if let Some(unit) = &self.unit {
            args.push(bulk(unit.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                let dist = s
                    .parse::<f64>()
                    .map_err(|_| RedisError::UnexpectedResponse {
                        expected: "float string",
                        actual: format!("{s}"),
                    })?;
                Ok(Some(dist))
            }
            Frame::Double(d) => Ok(Some(d)),
            Frame::BulkString(None) | Frame::Null => Ok(None),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string, double, or null",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "GEODIST"
    }
}

/// GEOHASH key member \[member ...\]
///
/// Returns the Geohash strings representing the position of each member.
/// Each element is `Some(hash)` if the member exists, or `None` if it
/// does not.
pub struct GeoHash {
    key: String,
    members: Vec<String>,
}

impl GeoHash {
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: vec![member.into()],
        }
    }

    /// Creates a `GeoHash` command for multiple members.
    pub fn members(
        key: impl Into<String>,
        members: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(Into::into).collect(),
        }
    }
}

impl Command for GeoHash {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("GEOHASH"), bulk(self.key.as_str())];
        for member in &self.members {
            args.push(bulk(member.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::BulkString(Some(data)) => {
                        Ok(Some(String::from_utf8_lossy(&data).into_owned()))
                    }
                    Frame::BulkString(None) | Frame::Null => Ok(None),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "bulk string or null",
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
        "GEOHASH"
    }
}

/// GEOPOS key member \[member ...\]
///
/// Returns the longitude and latitude of each specified member. Each
/// element is `Some((longitude, latitude))` if the member exists, or
/// `None` if it does not.
pub struct GeoPos {
    key: String,
    members: Vec<String>,
}

impl GeoPos {
    pub fn new(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            members: vec![member.into()],
        }
    }

    /// Creates a `GeoPos` command for multiple members.
    pub fn members(
        key: impl Into<String>,
        members: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(Into::into).collect(),
        }
    }
}

impl GeoPos {
    /// Parses a single coordinate pair from a two-element array of bulk strings
    /// or doubles.
    fn parse_coordinate(frames: Vec<Frame>) -> Result<(f64, f64), RedisError> {
        if frames.len() != 2 {
            return Err(RedisError::UnexpectedResponse {
                expected: "two-element array",
                actual: format!("array of length {}", frames.len()),
            });
        }
        let lon = Self::parse_f64(&frames[0])?;
        let lat = Self::parse_f64(&frames[1])?;
        Ok((lon, lat))
    }

    fn parse_f64(frame: &Frame) -> Result<f64, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(data);
                s.parse::<f64>()
                    .map_err(|_| RedisError::UnexpectedResponse {
                        expected: "float string",
                        actual: format!("{s}"),
                    })
            }
            Frame::Double(d) => Ok(*d),
            other => Err(RedisError::UnexpectedResponse {
                expected: "bulk string or double",
                actual: format!("{other:?}"),
            }),
        }
    }
}

impl Command for GeoPos {
    type Response = Vec<Option<(f64, f64)>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("GEOPOS"), bulk(self.key.as_str())];
        for member in &self.members {
            args.push(bulk(member.as_str()));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(Some(frames)) => frames
                .into_iter()
                .map(|f| match f {
                    Frame::Array(Some(coords)) => Self::parse_coordinate(coords).map(Some),
                    Frame::Array(None) | Frame::Null | Frame::BulkString(None) => Ok(None),
                    other => Err(RedisError::UnexpectedResponse {
                        expected: "array or null",
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
        "GEOPOS"
    }
}

/// Origin for a GEOSEARCH or GEOSEARCHSTORE command.
enum GeoSearchOrigin {
    Member(String),
    LonLat(f64, f64),
}

/// Shape predicate for a GEOSEARCH or GEOSEARCHSTORE command.
enum GeoSearchShape {
    Radius(f64, GeoUnit),
    Box(f64, f64, GeoUnit),
}

/// Sort order for GEOSEARCH results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GeoSearchOrder {
    Asc,
    Desc,
}

/// GEOSEARCH key FROMMEMBER member|FROMLONLAT longitude latitude BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI \[ASC|DESC\] \[COUNT count \[ANY\]\]
///
/// Returns the members of a sorted set populated with geospatial data
/// that are within the borders of the area specified by a given shape.
pub struct GeoSearch {
    key: String,
    origin: GeoSearchOrigin,
    shape: GeoSearchShape,
    order: Option<GeoSearchOrder>,
    count: Option<i64>,
    any: bool,
}

impl GeoSearch {
    fn new_with_origin(key: impl Into<String>, origin: GeoSearchOrigin) -> Self {
        Self {
            key: key.into(),
            origin,
            shape: GeoSearchShape::Radius(0.0, GeoUnit::Meters),
            order: None,
            count: None,
            any: false,
        }
    }

    /// Searches from an existing member in the sorted set.
    pub fn from_member(key: impl Into<String>, member: impl Into<String>) -> Self {
        Self::new_with_origin(key, GeoSearchOrigin::Member(member.into()))
    }

    /// Searches from the given longitude and latitude.
    pub fn from_lonlat(key: impl Into<String>, longitude: f64, latitude: f64) -> Self {
        Self::new_with_origin(key, GeoSearchOrigin::LonLat(longitude, latitude))
    }

    /// Searches within a circular area of the given radius.
    pub fn by_radius(mut self, radius: f64, unit: GeoUnit) -> Self {
        self.shape = GeoSearchShape::Radius(radius, unit);
        self
    }

    /// Searches within a rectangular area of the given width and height.
    pub fn by_box(mut self, width: f64, height: f64, unit: GeoUnit) -> Self {
        self.shape = GeoSearchShape::Box(width, height, unit);
        self
    }

    /// Sorts results from nearest to farthest.
    pub fn asc(mut self) -> Self {
        self.order = Some(GeoSearchOrder::Asc);
        self
    }

    /// Sorts results from farthest to nearest.
    pub fn desc(mut self) -> Self {
        self.order = Some(GeoSearchOrder::Desc);
        self
    }

    /// Limits the number of results to `count`.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self.any = false;
        self
    }

    /// Limits the number of results to `count`, but allows returning
    /// results as soon as enough matches are found (not necessarily the
    /// closest ones).
    pub fn count_any(mut self, count: i64) -> Self {
        self.count = Some(count);
        self.any = true;
        self
    }

    /// Builds the common argument list for GEOSEARCH and GEOSEARCHSTORE.
    fn build_search_args(&self) -> Vec<Frame> {
        let mut args = Vec::new();
        match &self.origin {
            GeoSearchOrigin::Member(member) => {
                args.push(bulk("FROMMEMBER"));
                args.push(bulk(member.as_str()));
            }
            GeoSearchOrigin::LonLat(lon, lat) => {
                args.push(bulk("FROMLONLAT"));
                args.push(bulk(lon.to_string()));
                args.push(bulk(lat.to_string()));
            }
        }
        match &self.shape {
            GeoSearchShape::Radius(radius, unit) => {
                args.push(bulk("BYRADIUS"));
                args.push(bulk(radius.to_string()));
                args.push(bulk(unit.as_str()));
            }
            GeoSearchShape::Box(width, height, unit) => {
                args.push(bulk("BYBOX"));
                args.push(bulk(width.to_string()));
                args.push(bulk(height.to_string()));
                args.push(bulk(unit.as_str()));
            }
        }
        if let Some(order) = &self.order {
            args.push(bulk(match order {
                GeoSearchOrder::Asc => "ASC",
                GeoSearchOrder::Desc => "DESC",
            }));
        }
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
            if self.any {
                args.push(bulk("ANY"));
            }
        }
        args
    }
}

impl Command for GeoSearch {
    type Response = Vec<Bytes>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![bulk("GEOSEARCH"), bulk(self.key.as_str())];
        args.extend(self.build_search_args());
        array(args)
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
            Frame::Array(None) => Ok(Vec::new()),
            other => Err(RedisError::UnexpectedResponse {
                expected: "array",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "GEOSEARCH"
    }
}

/// GEOSEARCHSTORE destination source FROMMEMBER member|FROMLONLAT longitude latitude BYRADIUS radius M|KM|FT|MI|BYBOX width height M|KM|FT|MI \[ASC|DESC\] \[COUNT count \[ANY\]\] \[STOREDIST\]
///
/// Like \[`GeoSearch`\], but stores the result in `destination`. Returns the
/// number of elements stored. When `store_dist` is set, the sorted set
/// stores distances instead of geospatial data.
pub struct GeoSearchStore {
    destination: String,
    source: String,
    origin: GeoSearchOrigin,
    shape: GeoSearchShape,
    order: Option<GeoSearchOrder>,
    count: Option<i64>,
    any: bool,
    store_dist: bool,
}

impl GeoSearchStore {
    fn new_with_origin(
        destination: impl Into<String>,
        source: impl Into<String>,
        origin: GeoSearchOrigin,
    ) -> Self {
        Self {
            destination: destination.into(),
            source: source.into(),
            origin,
            shape: GeoSearchShape::Radius(0.0, GeoUnit::Meters),
            order: None,
            count: None,
            any: false,
            store_dist: false,
        }
    }

    /// Searches from an existing member in the source sorted set.
    pub fn from_member(
        destination: impl Into<String>,
        source: impl Into<String>,
        member: impl Into<String>,
    ) -> Self {
        Self::new_with_origin(destination, source, GeoSearchOrigin::Member(member.into()))
    }

    /// Searches from the given longitude and latitude.
    pub fn from_lonlat(
        destination: impl Into<String>,
        source: impl Into<String>,
        longitude: f64,
        latitude: f64,
    ) -> Self {
        Self::new_with_origin(
            destination,
            source,
            GeoSearchOrigin::LonLat(longitude, latitude),
        )
    }

    /// Searches within a circular area of the given radius.
    pub fn by_radius(mut self, radius: f64, unit: GeoUnit) -> Self {
        self.shape = GeoSearchShape::Radius(radius, unit);
        self
    }

    /// Searches within a rectangular area of the given width and height.
    pub fn by_box(mut self, width: f64, height: f64, unit: GeoUnit) -> Self {
        self.shape = GeoSearchShape::Box(width, height, unit);
        self
    }

    /// Sorts results from nearest to farthest.
    pub fn asc(mut self) -> Self {
        self.order = Some(GeoSearchOrder::Asc);
        self
    }

    /// Sorts results from farthest to nearest.
    pub fn desc(mut self) -> Self {
        self.order = Some(GeoSearchOrder::Desc);
        self
    }

    /// Limits the number of results to `count`.
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self.any = false;
        self
    }

    /// Limits the number of results to `count`, but allows returning
    /// results as soon as enough matches are found (not necessarily the
    /// closest ones).
    pub fn count_any(mut self, count: i64) -> Self {
        self.count = Some(count);
        self.any = true;
        self
    }

    /// Stores distances in the destination sorted set instead of
    /// geospatial data.
    pub fn store_dist(mut self) -> Self {
        self.store_dist = true;
        self
    }

    /// Builds the search-specific argument list (shared logic with
    /// \[`GeoSearch`\]).
    fn build_search_args(&self) -> Vec<Frame> {
        let mut args = Vec::new();
        match &self.origin {
            GeoSearchOrigin::Member(member) => {
                args.push(bulk("FROMMEMBER"));
                args.push(bulk(member.as_str()));
            }
            GeoSearchOrigin::LonLat(lon, lat) => {
                args.push(bulk("FROMLONLAT"));
                args.push(bulk(lon.to_string()));
                args.push(bulk(lat.to_string()));
            }
        }
        match &self.shape {
            GeoSearchShape::Radius(radius, unit) => {
                args.push(bulk("BYRADIUS"));
                args.push(bulk(radius.to_string()));
                args.push(bulk(unit.as_str()));
            }
            GeoSearchShape::Box(width, height, unit) => {
                args.push(bulk("BYBOX"));
                args.push(bulk(width.to_string()));
                args.push(bulk(height.to_string()));
                args.push(bulk(unit.as_str()));
            }
        }
        if let Some(order) = &self.order {
            args.push(bulk(match order {
                GeoSearchOrder::Asc => "ASC",
                GeoSearchOrder::Desc => "DESC",
            }));
        }
        if let Some(count) = self.count {
            args.push(bulk("COUNT"));
            args.push(bulk(count.to_string()));
            if self.any {
                args.push(bulk("ANY"));
            }
        }
        args
    }
}

impl Command for GeoSearchStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            bulk("GEOSEARCHSTORE"),
            bulk(self.destination.as_str()),
            bulk(self.source.as_str()),
        ];
        args.extend(self.build_search_args());
        if self.store_dist {
            args.push(bulk("STOREDIST"));
        }
        array(args)
    }

    fn parse_response(&self, frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            other => Err(RedisError::UnexpectedResponse {
                expected: "integer",
                actual: format!("{other:?}"),
            }),
        }
    }

    fn name(&self) -> &str {
        "GEOSEARCHSTORE"
    }
}
