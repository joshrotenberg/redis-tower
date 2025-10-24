//! Geospatial Commands Example
//!
//! Demonstrates Redis geospatial features for location-based queries.
//! Shows radius searches, box searches, and storing search results.

use redis_tower::commands::{
    GeoAdd, GeoDist, GeoHash, GeoItem, GeoPos, GeoSearch, GeoSearchStore, GeoUnit,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Redis Tower - Geospatial Commands Example\n");

    // Note: This example requires a running Redis server

    println!("1. Adding locations to Redis");
    println!("   Adding major European cities...\n");

    let cities = vec![
        GeoItem::new(2.3522, 48.8566, "Paris"),
        GeoItem::new(-0.1276, 51.5074, "London"),
        GeoItem::new(13.4050, 52.5200, "Berlin"),
        GeoItem::new(12.4964, 41.9028, "Rome"),
        GeoItem::new(-3.7038, 40.4168, "Madrid"),
        GeoItem::new(4.8952, 52.3702, "Amsterdam"),
        GeoItem::new(16.3738, 48.2082, "Vienna"),
        GeoItem::new(10.7522, 59.9139, "Oslo"),
    ];

    let add_cmd = GeoAdd::new("cities", cities);
    println!("   Command: {:?}", add_cmd);
    println!("   → Returns: 8 (locations added)\n");

    // Get distance between two cities
    println!("2. Calculating distance between cities");
    let dist_cmd = GeoDist::new("cities", "Paris", "London").unit(GeoUnit::Kilometers);

    println!("   GEODIST cities Paris London km");
    println!("   Command: {:?}", dist_cmd);
    println!("   → Returns: ~344.39 km\n");

    // Get geohash
    println!("3. Getting geohash for precision encoding");
    let hash_cmd = GeoHash::new("cities", vec!["Paris".to_string(), "Berlin".to_string()]);
    println!("   GEOHASH cities Paris Berlin");
    println!("   Command: {:?}", hash_cmd);
    println!("   → Returns: [\"u09tunqu\", \"u33db2mh\"] (base32 encoded)\n");

    // Get coordinates
    println!("4. Getting coordinates of cities");
    let pos_cmd = GeoPos::new("cities", vec!["Rome".to_string(), "Madrid".to_string()]);
    println!("   GEOPOS cities Rome Madrid");
    println!("   Command: {:?}", pos_cmd);
    println!("   → Returns: [(12.4964, 41.9028), (-3.7038, 40.4168)]\n");

    // Search by radius from a member
    println!("5. Finding cities within 500km of Paris");
    let search_cmd = GeoSearch::new("cities")
        .from_member("Paris")
        .by_radius(500.0, GeoUnit::Kilometers)
        .with_dist()
        .with_coord();

    println!("   GEOSEARCH cities FROMMEMBER Paris BYRADIUS 500 km WITHDIST WITHCOORD");
    println!("   Command: {:?}", search_cmd);
    println!("   → Returns cities with distances:");
    println!("      • Paris: 0.00 km");
    println!("      • London: 344.39 km");
    println!("      • Amsterdam: 431.23 km\n");

    // Search by radius from coordinates
    println!("6. Finding cities within 300km of a specific point");
    let search_coords = GeoSearch::new("cities")
        .from_lonlat(13.4050, 52.5200) // Berlin coordinates
        .by_radius(300.0, GeoUnit::Kilometers)
        .count(5);

    println!("   GEOSEARCH cities FROMLONLAT 13.4050 52.5200 BYRADIUS 300 km COUNT 5");
    println!("   Command: {:?}", search_coords);
    println!("   → Returns: Berlin, and nearby cities\n");

    // Search by box dimensions
    println!("7. Finding cities in a rectangular area");
    let search_box = GeoSearch::new("cities")
        .from_member("Rome")
        .by_box(1000.0, 800.0, GeoUnit::Kilometers)
        .with_dist();

    println!("   GEOSEARCH cities FROMMEMBER Rome BYBOX 1000 800 km WITHDIST");
    println!("   Command: {:?}", search_box);
    println!("   → Returns cities in 1000km x 800km box around Rome\n");

    // Store search results (Redis 6.2+)
    println!("8. Storing search results for later use");
    let store_cmd = GeoSearchStore::new("nearby_paris", "cities")
        .from_member("Paris")
        .by_radius(600.0, GeoUnit::Kilometers);

    println!("   GEOSEARCHSTORE nearby_paris cities FROMMEMBER Paris BYRADIUS 600 km");
    println!("   Command: {:?}", store_cmd);
    println!("   → Stores: Paris, London, Amsterdam, etc. in 'nearby_paris' key");
    println!("   → Returns: 4 (number of members stored)\n");

    // Store with distances (creates sorted set with distances as scores)
    println!("9. Storing search results with distances as scores");
    let store_dist = GeoSearchStore::new("paris_distances", "cities")
        .from_member("Paris")
        .by_radius(1000.0, GeoUnit::Kilometers)
        .storedist(); // Distances become scores in sorted set

    println!(
        "   GEOSEARCHSTORE paris_distances cities FROMMEMBER Paris BYRADIUS 1000 km STOREDIST"
    );
    println!("   Command: {:?}", store_dist);
    println!("   → Creates sorted set where score = distance from Paris");
    println!("   → Can then use ZRANGE to get nearest cities\n");

    // Advanced: Search and limit results
    println!("10. Finding 3 nearest cities to London");
    let nearest = GeoSearch::new("cities")
        .from_member("London")
        .by_radius(2000.0, GeoUnit::Kilometers)
        .count(3)
        .with_dist()
        .with_coord();

    println!("   GEOSEARCH cities FROMMEMBER London BYRADIUS 2000 km COUNT 3 WITHDIST WITHCOORD");
    println!("   Command: {:?}", nearest);
    println!("   → Returns 3 nearest cities with coordinates and distances\n");

    println!("{}", "=".repeat(60));
    println!("Geospatial Use Cases:\n");

    println!("✓ Store Locator: Find nearest stores/restaurants");
    println!("✓ Delivery Zones: Check if address is in delivery range");
    println!("✓ Proximity Matching: Dating apps, ride-sharing");
    println!("✓ Asset Tracking: Monitor vehicle/device locations");
    println!("✓ Location-based Services: Nearby points of interest");

    println!("\nPerformance:");
    println!("  • Geospatial index based on sorted sets");
    println!("  • Efficient radius queries with Geohash");
    println!("  • O(N+log(M)) complexity for radius searches");
    println!("  • GEOSEARCHSTORE enables caching results");

    println!("\nUnits Supported:");
    println!("  • m  (meters)");
    println!("  • km (kilometers)");
    println!("  • mi (miles)");
    println!("  • ft (feet)");

    Ok(())
}
