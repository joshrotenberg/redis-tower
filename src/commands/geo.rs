//! Geospatial commands for Redis
//!
//! Redis geospatial indexes allow storing coordinates and performing
//! radius queries, distance calculations, and more.

use crate::codec::Frame;
use crate::commands::Command;
use crate::types::RedisError;
use bytes::Bytes;

/// Geographic coordinate (longitude, latitude)
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoCoordinate {
    /// Longitude (-180 to 180)
    pub longitude: f64,
    /// Latitude (-85.05112878 to 85.05112878)
    pub latitude: f64,
}

impl GeoCoordinate {
    /// Create a new geographic coordinate
    pub fn new(longitude: f64, latitude: f64) -> Self {
        Self {
            longitude,
            latitude,
        }
    }
}

/// Geographic item with member name and coordinates
#[derive(Debug, Clone)]
pub struct GeoItem {
    /// Longitude
    pub longitude: f64,
    /// Latitude
    pub latitude: f64,
    /// Member name
    pub member: String,
}

impl GeoItem {
    /// Create a new geographic item
    pub fn new(longitude: f64, latitude: f64, member: impl Into<String>) -> Self {
        Self {
            longitude,
            latitude,
            member: member.into(),
        }
    }
}

/// Unit of distance for geospatial commands
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoUnit {
    /// Meters
    Meters,
    /// Kilometers
    Kilometers,
    /// Miles
    Miles,
    /// Feet
    Feet,
}

impl GeoUnit {
    fn as_str(&self) -> &'static str {
        match self {
            GeoUnit::Meters => "m",
            GeoUnit::Kilometers => "km",
            GeoUnit::Miles => "mi",
            GeoUnit::Feet => "ft",
        }
    }
}

/// GEOADD command - Add geospatial items to a sorted set
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{GeoAdd, GeoItem};
///
/// // Add single location
/// let cmd = GeoAdd::new("locations", vec![
///     GeoItem::new(13.361389, 38.115556, "Palermo")
/// ]);
///
/// // Add multiple locations
/// let cmd = GeoAdd::new("locations", vec![
///     GeoItem::new(13.361389, 38.115556, "Palermo"),
///     GeoItem::new(15.087269, 37.502669, "Catania"),
/// ]);
/// ```
#[derive(Debug, Clone)]
pub struct GeoAdd {
    key: String,
    items: Vec<GeoItem>,
}

impl GeoAdd {
    /// Create a new GEOADD command
    pub fn new(key: impl Into<String>, items: Vec<GeoItem>) -> Self {
        Self {
            key: key.into(),
            items,
        }
    }
}

impl Command for GeoAdd {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEOADD"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for item in &self.items {
            args.push(Frame::BulkString(Some(Bytes::from(
                item.longitude.to_string(),
            ))));
            args.push(Frame::BulkString(Some(Bytes::from(
                item.latitude.to_string(),
            ))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                item.member.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(count) => Ok(count),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GEODIST command - Get distance between two members
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{GeoDist, GeoUnit};
///
/// // Get distance in meters (default)
/// let cmd = GeoDist::new("locations", "Palermo", "Catania");
///
/// // Get distance in kilometers
/// let cmd = GeoDist::new("locations", "Palermo", "Catania")
///     .unit(GeoUnit::Kilometers);
/// ```
#[derive(Debug, Clone)]
pub struct GeoDist {
    key: String,
    member1: String,
    member2: String,
    unit: GeoUnit,
}

impl GeoDist {
    /// Create a new GEODIST command (default: meters)
    pub fn new(
        key: impl Into<String>,
        member1: impl Into<String>,
        member2: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            member1: member1.into(),
            member2: member2.into(),
            unit: GeoUnit::Meters,
        }
    }

    /// Set the unit of distance
    pub fn unit(mut self, unit: GeoUnit) -> Self {
        self.unit = unit;
        self
    }
}

impl Command for GeoDist {
    type Response = Option<f64>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEODIST"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.member1.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.member2.as_bytes()))),
        ];

        args.push(Frame::BulkString(Some(Bytes::from(self.unit.as_str()))));

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::BulkString(Some(data)) => {
                let s = String::from_utf8_lossy(&data);
                Ok(Some(
                    s.parse::<f64>()
                        .map_err(|_| RedisError::UnexpectedResponse)?,
                ))
            }
            Frame::BulkString(None) | Frame::Null => Ok(None),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GEOHASH command - Get geohash strings for members
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::GeoHash;
///
/// let cmd = GeoHash::new("locations", vec!["Palermo", "Catania"]);
/// ```
#[derive(Debug, Clone)]
pub struct GeoHash {
    key: String,
    members: Vec<String>,
}

impl GeoHash {
    /// Create a new GEOHASH command
    pub fn new(key: impl Into<String>, members: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(|m| m.into()).collect(),
        }
    }
}

impl Command for GeoHash {
    type Response = Vec<Option<String>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEOHASH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for member in &self.members {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                member.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut hashes = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            hashes.push(Some(String::from_utf8_lossy(&data).into_owned()));
                        }
                        Frame::BulkString(None) | Frame::Null => {
                            hashes.push(None);
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(hashes)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GEOPOS command - Get coordinates for members
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::GeoPos;
///
/// let cmd = GeoPos::new("locations", vec!["Palermo", "Catania"]);
/// ```
#[derive(Debug, Clone)]
pub struct GeoPos {
    key: String,
    members: Vec<String>,
}

impl GeoPos {
    /// Create a new GEOPOS command
    pub fn new(key: impl Into<String>, members: Vec<impl Into<String>>) -> Self {
        Self {
            key: key.into(),
            members: members.into_iter().map(|m| m.into()).collect(),
        }
    }
}

impl Command for GeoPos {
    type Response = Vec<Option<GeoCoordinate>>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEOPOS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        for member in &self.members {
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                member.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut positions = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::Array(coords) if coords.len() == 2 => {
                            let lon = match &coords[0] {
                                Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                                    .parse::<f64>()
                                    .map_err(|_| RedisError::UnexpectedResponse)?,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            let lat = match &coords[1] {
                                Frame::BulkString(Some(data)) => String::from_utf8_lossy(data)
                                    .parse::<f64>()
                                    .map_err(|_| RedisError::UnexpectedResponse)?,
                                _ => return Err(RedisError::UnexpectedResponse),
                            };

                            positions.push(Some(GeoCoordinate::new(lon, lat)));
                        }
                        Frame::Null | Frame::Array(_) => {
                            positions.push(None);
                        }
                        _ => return Err(RedisError::UnexpectedResponse),
                    }
                }
                Ok(positions)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GEOSEARCH command - Search for members in a radius (Redis 6.2+)
///
/// Modern replacement for GEORADIUS/GEORADIUSBYMEMBER
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{GeoSearch, GeoUnit};
///
/// // Search by member with radius
/// let cmd = GeoSearch::new("locations")
///     .from_member("Palermo")
///     .by_radius(100.0, GeoUnit::Kilometers);
///
/// // Search by coordinates with box
/// let cmd = GeoSearch::new("locations")
///     .from_lonlat(15.0, 37.0)
///     .by_box(400.0, 400.0, GeoUnit::Kilometers);
/// ```
#[derive(Debug, Clone)]
pub struct GeoSearch {
    key: String,
    from_member: Option<String>,
    from_lonlat: Option<(f64, f64)>,
    by_radius: Option<(f64, GeoUnit)>,
    by_box: Option<(f64, f64, GeoUnit)>,
    count: Option<i64>,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
}

impl GeoSearch {
    /// Create a new GEOSEARCH command
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            from_member: None,
            from_lonlat: None,
            by_radius: None,
            by_box: None,
            count: None,
            with_coord: false,
            with_dist: false,
            with_hash: false,
        }
    }

    /// Search from a member
    pub fn from_member(mut self, member: impl Into<String>) -> Self {
        self.from_member = Some(member.into());
        self
    }

    /// Search from coordinates
    pub fn from_lonlat(mut self, longitude: f64, latitude: f64) -> Self {
        self.from_lonlat = Some((longitude, latitude));
        self
    }

    /// Search by radius
    pub fn by_radius(mut self, radius: f64, unit: GeoUnit) -> Self {
        self.by_radius = Some((radius, unit));
        self
    }

    /// Search by box dimensions
    pub fn by_box(mut self, width: f64, height: f64, unit: GeoUnit) -> Self {
        self.by_box = Some((width, height, unit));
        self
    }

    /// Limit result count
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Include coordinates in results
    pub fn with_coord(mut self) -> Self {
        self.with_coord = true;
        self
    }

    /// Include distance in results
    pub fn with_dist(mut self) -> Self {
        self.with_dist = true;
        self
    }

    /// Include geohash in results
    pub fn with_hash(mut self) -> Self {
        self.with_hash = true;
        self
    }
}

impl Command for GeoSearch {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEOSEARCH"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
        ];

        // FROM clause (required)
        if let Some(ref member) = self.from_member {
            args.push(Frame::BulkString(Some(Bytes::from("FROMMEMBER"))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                member.as_bytes(),
            ))));
        } else if let Some((lon, lat)) = self.from_lonlat {
            args.push(Frame::BulkString(Some(Bytes::from("FROMLONLAT"))));
            args.push(Frame::BulkString(Some(Bytes::from(lon.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(lat.to_string()))));
        }

        // BY clause (required)
        if let Some((radius, unit)) = self.by_radius {
            args.push(Frame::BulkString(Some(Bytes::from("BYRADIUS"))));
            args.push(Frame::BulkString(Some(Bytes::from(radius.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(unit.as_str()))));
        } else if let Some((width, height, unit)) = self.by_box {
            args.push(Frame::BulkString(Some(Bytes::from("BYBOX"))));
            args.push(Frame::BulkString(Some(Bytes::from(width.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(height.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(unit.as_str()))));
        }

        // Optional modifiers
        if self.with_coord {
            args.push(Frame::BulkString(Some(Bytes::from("WITHCOORD"))));
        }
        if self.with_dist {
            args.push(Frame::BulkString(Some(Bytes::from("WITHDIST"))));
        }
        if self.with_hash {
            args.push(Frame::BulkString(Some(Bytes::from("WITHHASH"))));
        }

        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut members = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            members.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        Frame::Array(nested) if !nested.is_empty() => {
                            // When WITH* options are used, first element is the member name
                            if let Frame::BulkString(Some(data)) = &nested[0] {
                                members.push(String::from_utf8_lossy(data).into_owned());
                            }
                        }
                        _ => {}
                    }
                }
                Ok(members)
            }
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GEOSEARCHSTORE command - Search and store results
///
/// Performs a GEOSEARCH and stores the results in a destination key.
/// Available since Redis 6.2.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{GeoSearchStore, GeoUnit};
///
/// // Search by radius from member and store
/// let cmd = GeoSearchStore::new("dest", "locations")
///     .from_member("Palermo")
///     .by_radius(100.0, GeoUnit::Kilometers);
///
/// // Search by box from coordinates and store with distances
/// let cmd = GeoSearchStore::new("dest", "locations")
///     .from_lonlat(15.0, 37.0)
///     .by_box(200.0, 200.0, GeoUnit::Kilometers)
///     .storedist();
/// ```
#[derive(Debug, Clone)]
pub struct GeoSearchStore {
    destination: String,
    source: String,
    from_member: Option<String>,
    from_lonlat: Option<(f64, f64)>,
    by_radius: Option<(f64, GeoUnit)>,
    by_box: Option<(f64, f64, GeoUnit)>,
    count: Option<i64>,
    storedist: bool,
}

impl GeoSearchStore {
    /// Create a new GEOSEARCHSTORE command
    pub fn new(destination: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            destination: destination.into(),
            source: source.into(),
            from_member: None,
            from_lonlat: None,
            by_radius: None,
            by_box: None,
            count: None,
            storedist: false,
        }
    }

    /// Search from a member
    pub fn from_member(mut self, member: impl Into<String>) -> Self {
        self.from_member = Some(member.into());
        self
    }

    /// Search from coordinates
    pub fn from_lonlat(mut self, longitude: f64, latitude: f64) -> Self {
        self.from_lonlat = Some((longitude, latitude));
        self
    }

    /// Search by radius
    pub fn by_radius(mut self, radius: f64, unit: GeoUnit) -> Self {
        self.by_radius = Some((radius, unit));
        self
    }

    /// Search by box dimensions
    pub fn by_box(mut self, width: f64, height: f64, unit: GeoUnit) -> Self {
        self.by_box = Some((width, height, unit));
        self
    }

    /// Limit result count
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Store distances instead of just members
    pub fn storedist(mut self) -> Self {
        self.storedist = true;
        self
    }
}

impl Command for GeoSearchStore {
    type Response = i64;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEOSEARCHSTORE"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.destination.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.source.as_bytes()))),
        ];

        // FROM clause (required)
        if let Some(ref member) = self.from_member {
            args.push(Frame::BulkString(Some(Bytes::from("FROMMEMBER"))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                member.as_bytes(),
            ))));
        } else if let Some((lon, lat)) = self.from_lonlat {
            args.push(Frame::BulkString(Some(Bytes::from("FROMLONLAT"))));
            args.push(Frame::BulkString(Some(Bytes::from(lon.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(lat.to_string()))));
        }

        // BY clause (required)
        if let Some((radius, unit)) = self.by_radius {
            args.push(Frame::BulkString(Some(Bytes::from("BYRADIUS"))));
            args.push(Frame::BulkString(Some(Bytes::from(radius.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(unit.as_str()))));
        } else if let Some((width, height, unit)) = self.by_box {
            args.push(Frame::BulkString(Some(Bytes::from("BYBOX"))));
            args.push(Frame::BulkString(Some(Bytes::from(width.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(height.to_string()))));
            args.push(Frame::BulkString(Some(Bytes::from(unit.as_str()))));
        }

        // Optional COUNT
        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }

        // STOREDIST option
        if self.storedist {
            args.push(Frame::BulkString(Some(Bytes::from("STOREDIST"))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Integer(n) => Ok(n),
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

// Read-only trait implementations
use crate::read_preference::ReadOnly;

impl ReadOnly for GeoSearch {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for GeoDist {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for GeoHash {
    fn is_read_only(&self) -> bool {
        true
    }
}

impl ReadOnly for GeoPos {
    fn is_read_only(&self) -> bool {
        true
    }
}

// Write commands
impl ReadOnly for GeoAdd {}
impl ReadOnly for GeoSearchStore {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_geoadd_frame() {
        let cmd = GeoAdd::new(
            "locations",
            vec![GeoItem::new(13.361389, 38.115556, "Palermo")],
        );
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5); // GEOADD + key + lon + lat + member
                assert!(matches!(
                    &args[0],
                    Frame::BulkString(Some(s)) if s == &Bytes::from("GEOADD")
                ));
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_geoadd_response() {
        let response = GeoAdd::parse_response(Frame::Integer(1)).unwrap();
        assert_eq!(response, 1);
    }

    #[test]
    fn test_geodist_frame() {
        let cmd = GeoDist::new("locations", "Palermo", "Catania").unit(GeoUnit::Kilometers);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 5); // GEODIST + key + member1 + member2 + unit
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_geodist_response() {
        let response =
            GeoDist::parse_response(Frame::BulkString(Some(Bytes::from("166.2742")))).unwrap();
        assert_eq!(response, Some(166.2742));

        let response = GeoDist::parse_response(Frame::Null).unwrap();
        assert_eq!(response, None);
    }

    #[test]
    fn test_geohash_frame() {
        let cmd = GeoHash::new("locations", vec!["Palermo", "Catania"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 4); // GEOHASH + key + 2 members
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_geopos_frame() {
        let cmd = GeoPos::new("locations", vec!["Palermo"]);
        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert_eq!(args.len(), 3); // GEOPOS + key + member
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_geosearch_from_member() {
        let cmd = GeoSearch::new("locations")
            .from_member("Palermo")
            .by_radius(100.0, GeoUnit::Kilometers);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert!(args.len() >= 6); // GEOSEARCH + key + FROMMEMBER + member + BYRADIUS + radius + unit
            }
            _ => panic!("Expected Array frame"),
        }
    }

    #[test]
    fn test_geosearch_with_options() {
        let cmd = GeoSearch::new("locations")
            .from_lonlat(15.0, 37.0)
            .by_radius(200.0, GeoUnit::Kilometers)
            .with_coord()
            .with_dist()
            .count(10);

        let frame = cmd.to_frame();

        match frame {
            Frame::Array(args) => {
                assert!(args.len() >= 11); // Many arguments with options
            }
            _ => panic!("Expected Array frame"),
        }
    }
}

/// GEORADIUS_RO command - Query by radius (read-only, Redis 6.2+)
///
/// Read-only variant of GEORADIUS for replica routing in cluster mode.
/// This is a deprecated command - use GEOSEARCH instead for new applications.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{GeoRadiusReadOnly, GeoUnit};
///
/// let cmd = GeoRadiusReadOnly::new("locations", 15.0, 37.0, 100.0, GeoUnit::Kilometers);
/// ```
#[derive(Debug, Clone)]
pub struct GeoRadiusReadOnly {
    key: String,
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: GeoUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<i64>,
}

impl GeoRadiusReadOnly {
    /// Create a new GEORADIUS_RO command
    pub fn new(
        key: impl Into<String>,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> Self {
        Self {
            key: key.into(),
            longitude,
            latitude,
            radius,
            unit,
            with_coord: false,
            with_dist: false,
            with_hash: false,
            count: None,
        }
    }

    /// Include coordinates in results
    pub fn with_coord(mut self) -> Self {
        self.with_coord = true;
        self
    }

    /// Include distance in results
    pub fn with_dist(mut self) -> Self {
        self.with_dist = true;
        self
    }

    /// Include geohash in results
    pub fn with_hash(mut self) -> Self {
        self.with_hash = true;
        self
    }

    /// Limit result count
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl crate::commands::Command for GeoRadiusReadOnly {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        let mut args = vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("GEORADIUS_RO"))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::copy_from_slice(
                self.key.as_bytes(),
            ))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.longitude.to_string()))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.latitude.to_string()))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.radius.to_string()))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.unit.as_str()))),
        ];

        if self.with_coord {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "WITHCOORD",
            ))));
        }
        if self.with_dist {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "WITHDIST",
            ))));
        }
        if self.with_hash {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "WITHHASH",
            ))));
        }
        if let Some(count) = self.count {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "COUNT",
            ))));
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                count.to_string(),
            ))));
        }

        crate::codec::Frame::Array(args)
    }

    fn parse_response(
        frame: crate::codec::Frame,
    ) -> Result<Self::Response, crate::types::RedisError> {
        match frame {
            crate::codec::Frame::Array(items) => {
                let mut members = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        crate::codec::Frame::BulkString(Some(data)) => {
                            members.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        crate::codec::Frame::Array(nested) if !nested.is_empty() => {
                            if let crate::codec::Frame::BulkString(Some(data)) = &nested[0] {
                                members.push(String::from_utf8_lossy(data).into_owned());
                            }
                        }
                        _ => {}
                    }
                }
                Ok(members)
            }
            crate::codec::Frame::Error(e) => Err(crate::types::RedisError::from_redis_error(
                &String::from_utf8_lossy(&e),
            )),
            _ => Err(crate::types::RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for GeoRadiusReadOnly {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// GEORADIUSBYMEMBER_RO command - Query by member radius (read-only, Redis 6.2+)
///
/// Read-only variant of GEORADIUSBYMEMBER for replica routing in cluster mode.
/// This is a deprecated command - use GEOSEARCH instead for new applications.
///
/// # Examples
///
/// ```no_run
/// use redis_tower::commands::{GeoRadiusByMemberReadOnly, GeoUnit};
///
/// let cmd = GeoRadiusByMemberReadOnly::new("locations", "Palermo", 100.0, GeoUnit::Kilometers);
/// ```
#[derive(Debug, Clone)]
pub struct GeoRadiusByMemberReadOnly {
    key: String,
    member: String,
    radius: f64,
    unit: GeoUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<i64>,
}

impl GeoRadiusByMemberReadOnly {
    /// Create a new GEORADIUSBYMEMBER_RO command
    pub fn new(
        key: impl Into<String>,
        member: impl Into<String>,
        radius: f64,
        unit: GeoUnit,
    ) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
            radius,
            unit,
            with_coord: false,
            with_dist: false,
            with_hash: false,
            count: None,
        }
    }

    /// Include coordinates in results
    pub fn with_coord(mut self) -> Self {
        self.with_coord = true;
        self
    }

    /// Include distance in results
    pub fn with_dist(mut self) -> Self {
        self.with_dist = true;
        self
    }

    /// Include geohash in results
    pub fn with_hash(mut self) -> Self {
        self.with_hash = true;
        self
    }

    /// Limit result count
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }
}

impl crate::commands::Command for GeoRadiusByMemberReadOnly {
    type Response = Vec<String>;

    fn to_frame(&self) -> crate::codec::Frame {
        let mut args = vec![
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from("GEORADIUSBYMEMBER_RO"))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::copy_from_slice(
                self.key.as_bytes(),
            ))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::copy_from_slice(
                self.member.as_bytes(),
            ))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.radius.to_string()))),
            crate::codec::Frame::BulkString(Some(bytes::Bytes::from(self.unit.as_str()))),
        ];

        if self.with_coord {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "WITHCOORD",
            ))));
        }
        if self.with_dist {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "WITHDIST",
            ))));
        }
        if self.with_hash {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "WITHHASH",
            ))));
        }
        if let Some(count) = self.count {
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                "COUNT",
            ))));
            args.push(crate::codec::Frame::BulkString(Some(bytes::Bytes::from(
                count.to_string(),
            ))));
        }

        crate::codec::Frame::Array(args)
    }

    fn parse_response(
        frame: crate::codec::Frame,
    ) -> Result<Self::Response, crate::types::RedisError> {
        match frame {
            crate::codec::Frame::Array(items) => {
                let mut members = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        crate::codec::Frame::BulkString(Some(data)) => {
                            members.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        crate::codec::Frame::Array(nested) if !nested.is_empty() => {
                            if let crate::codec::Frame::BulkString(Some(data)) = &nested[0] {
                                members.push(String::from_utf8_lossy(data).into_owned());
                            }
                        }
                        _ => {}
                    }
                }
                Ok(members)
            }
            crate::codec::Frame::Error(e) => Err(crate::types::RedisError::from_redis_error(
                &String::from_utf8_lossy(&e),
            )),
            _ => Err(crate::types::RedisError::UnexpectedResponse),
        }
    }
}

impl ReadOnly for GeoRadiusByMemberReadOnly {
    fn is_read_only(&self) -> bool {
        true
    }
}

/// GEORADIUS command - Query by radius (deprecated, use GEOSEARCH)
///
/// This command is deprecated. Use GEOSEARCH with BYRADIUS and FROMLONLAT instead.
///
/// Available since Redis 3.2.0. Deprecated in Redis 6.2.0.
#[derive(Debug, Clone)]
pub struct GeoRadius {
    key: String,
    longitude: f64,
    latitude: f64,
    radius: f64,
    unit: GeoUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<i64>,
    store: Option<String>,
    storedist: Option<String>,
}

impl GeoRadius {
    /// Create a new GEORADIUS command
    pub fn new(
        key: impl Into<String>,
        longitude: f64,
        latitude: f64,
        radius: f64,
        unit: GeoUnit,
    ) -> Self {
        Self {
            key: key.into(),
            longitude,
            latitude,
            radius,
            unit,
            with_coord: false,
            with_dist: false,
            with_hash: false,
            count: None,
            store: None,
            storedist: None,
        }
    }

    /// Include coordinates in results
    pub fn with_coord(mut self) -> Self {
        self.with_coord = true;
        self
    }

    /// Include distance in results
    pub fn with_dist(mut self) -> Self {
        self.with_dist = true;
        self
    }

    /// Include geohash in results
    pub fn with_hash(mut self) -> Self {
        self.with_hash = true;
        self
    }

    /// Limit result count
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Store results in destination key
    pub fn store(mut self, destination: impl Into<String>) -> Self {
        self.store = Some(destination.into());
        self
    }

    /// Store distances in destination key
    pub fn storedist(mut self, destination: impl Into<String>) -> Self {
        self.storedist = Some(destination.into());
        self
    }
}

impl Command for GeoRadius {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEORADIUS"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.longitude.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.latitude.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.radius.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.unit.as_str()))),
        ];

        if self.with_coord {
            args.push(Frame::BulkString(Some(Bytes::from("WITHCOORD"))));
        }
        if self.with_dist {
            args.push(Frame::BulkString(Some(Bytes::from("WITHDIST"))));
        }
        if self.with_hash {
            args.push(Frame::BulkString(Some(Bytes::from("WITHHASH"))));
        }
        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }
        if let Some(ref dest) = self.store {
            args.push(Frame::BulkString(Some(Bytes::from("STORE"))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                dest.as_bytes(),
            ))));
        }
        if let Some(ref dest) = self.storedist {
            args.push(Frame::BulkString(Some(Bytes::from("STOREDIST"))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                dest.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut members = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            members.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        Frame::Array(nested) if !nested.is_empty() => {
                            if let Frame::BulkString(Some(data)) = &nested[0] {
                                members.push(String::from_utf8_lossy(data).into_owned());
                            }
                        }
                        _ => {}
                    }
                }
                Ok(members)
            }
            Frame::Integer(n) => Ok(vec![n.to_string()]), // For STORE/STOREDIST
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}

/// GEORADIUSBYMEMBER command - Query by member radius (deprecated, use GEOSEARCH)
///
/// This command is deprecated. Use GEOSEARCH with BYRADIUS and FROMMEMBER instead.
///
/// Available since Redis 3.2.0. Deprecated in Redis 6.2.0.
#[derive(Debug, Clone)]
pub struct GeoRadiusByMember {
    key: String,
    member: String,
    radius: f64,
    unit: GeoUnit,
    with_coord: bool,
    with_dist: bool,
    with_hash: bool,
    count: Option<i64>,
    store: Option<String>,
    storedist: Option<String>,
}

impl GeoRadiusByMember {
    /// Create a new GEORADIUSBYMEMBER command
    pub fn new(
        key: impl Into<String>,
        member: impl Into<String>,
        radius: f64,
        unit: GeoUnit,
    ) -> Self {
        Self {
            key: key.into(),
            member: member.into(),
            radius,
            unit,
            with_coord: false,
            with_dist: false,
            with_hash: false,
            count: None,
            store: None,
            storedist: None,
        }
    }

    /// Include coordinates in results
    pub fn with_coord(mut self) -> Self {
        self.with_coord = true;
        self
    }

    /// Include distance in results
    pub fn with_dist(mut self) -> Self {
        self.with_dist = true;
        self
    }

    /// Include geohash in results
    pub fn with_hash(mut self) -> Self {
        self.with_hash = true;
        self
    }

    /// Limit result count
    pub fn count(mut self, count: i64) -> Self {
        self.count = Some(count);
        self
    }

    /// Store results in destination key
    pub fn store(mut self, destination: impl Into<String>) -> Self {
        self.store = Some(destination.into());
        self
    }

    /// Store distances in destination key
    pub fn storedist(mut self, destination: impl Into<String>) -> Self {
        self.storedist = Some(destination.into());
        self
    }
}

impl Command for GeoRadiusByMember {
    type Response = Vec<String>;

    fn to_frame(&self) -> Frame {
        let mut args = vec![
            Frame::BulkString(Some(Bytes::from("GEORADIUSBYMEMBER"))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.key.as_bytes()))),
            Frame::BulkString(Some(Bytes::copy_from_slice(self.member.as_bytes()))),
            Frame::BulkString(Some(Bytes::from(self.radius.to_string()))),
            Frame::BulkString(Some(Bytes::from(self.unit.as_str()))),
        ];

        if self.with_coord {
            args.push(Frame::BulkString(Some(Bytes::from("WITHCOORD"))));
        }
        if self.with_dist {
            args.push(Frame::BulkString(Some(Bytes::from("WITHDIST"))));
        }
        if self.with_hash {
            args.push(Frame::BulkString(Some(Bytes::from("WITHHASH"))));
        }
        if let Some(count) = self.count {
            args.push(Frame::BulkString(Some(Bytes::from("COUNT"))));
            args.push(Frame::BulkString(Some(Bytes::from(count.to_string()))));
        }
        if let Some(ref dest) = self.store {
            args.push(Frame::BulkString(Some(Bytes::from("STORE"))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                dest.as_bytes(),
            ))));
        }
        if let Some(ref dest) = self.storedist {
            args.push(Frame::BulkString(Some(Bytes::from("STOREDIST"))));
            args.push(Frame::BulkString(Some(Bytes::copy_from_slice(
                dest.as_bytes(),
            ))));
        }

        Frame::Array(args)
    }

    fn parse_response(frame: Frame) -> Result<Self::Response, RedisError> {
        match frame {
            Frame::Array(items) => {
                let mut members = Vec::new();
                for item in items {
                    match item {
                        Frame::BulkString(Some(data)) => {
                            members.push(String::from_utf8_lossy(&data).into_owned());
                        }
                        Frame::Array(nested) if !nested.is_empty() => {
                            if let Frame::BulkString(Some(data)) = &nested[0] {
                                members.push(String::from_utf8_lossy(data).into_owned());
                            }
                        }
                        _ => {}
                    }
                }
                Ok(members)
            }
            Frame::Integer(n) => Ok(vec![n.to_string()]), // For STORE/STOREDIST
            Frame::Error(e) => Err(RedisError::from_redis_error(&String::from_utf8_lossy(&e))),
            _ => Err(RedisError::UnexpectedResponse),
        }
    }
}
