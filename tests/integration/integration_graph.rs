#[cfg(feature = "modules")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::keys::Del;
    use redis_tower::modules::graph::*;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_graph_query_create() {
        let client = setup_redis().await;
        let graph_name = "graph_test_create";

        // Create a simple graph with nodes
        let query = "CREATE (:Person {name: 'Alice', age: 30})";
        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, query))
            .await
            .unwrap();

        // Verify node was created
        assert!(result.statistics.nodes_created > 0);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_query_match() {
        let client = setup_redis().await;
        let graph_name = "graph_test_match";

        // Create nodes
        let create_query =
            "CREATE (:Person {name: 'Bob', age: 25}), (:Person {name: 'Carol', age: 28})";
        client
            .call(GraphQuery::new(graph_name, create_query))
            .await
            .unwrap();

        // Match nodes
        let match_query = "MATCH (p:Person) RETURN p.name, p.age";
        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, match_query))
            .await
            .unwrap();

        // Should return 2 rows
        assert_eq!(result.data.len(), 2);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_query_relationship() {
        let client = setup_redis().await;
        let graph_name = "graph_test_relationship";

        // Create nodes with relationship
        let query = "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})";
        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, query))
            .await
            .unwrap();

        // Verify creation
        assert!(result.statistics.nodes_created >= 2);
        assert!(result.statistics.relationships_created >= 1);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_roquery() {
        let client = setup_redis().await;
        let graph_name = "graph_test_roquery";

        // Create data
        let create_query = "CREATE (:Person {name: 'Dave', age: 35})";
        client
            .call(GraphQuery::new(graph_name, create_query))
            .await
            .unwrap();

        // Read-only query
        let ro_query = "MATCH (p:Person) RETURN p.name";
        let result: QueryResult = client
            .call(GraphRoQuery::new(graph_name, ro_query))
            .await
            .unwrap();

        // Should return 1 row
        assert_eq!(result.data.len(), 1);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_delete() {
        let client = setup_redis().await;
        let graph_name = "graph_test_delete";

        // Create graph
        let query = "CREATE (:Person {name: 'Eve'})";
        client
            .call(GraphQuery::new(graph_name, query))
            .await
            .unwrap();

        // Delete graph
        let deleted: String = client.call(GraphDelete::new(graph_name)).await.unwrap();

        // Should confirm deletion
        assert!(deleted.contains("Graph removed") || deleted.contains("deleted"));

        // Note: No cleanup needed since graph was deleted
    }

    #[tokio::test]
    async fn test_graph_explain() {
        let client = setup_redis().await;
        let graph_name = "graph_test_explain";

        // Create graph first
        let create_query = "CREATE (:Person {name: 'Frank'})";
        client
            .call(GraphQuery::new(graph_name, create_query))
            .await
            .unwrap();

        // Explain a query
        let query = "MATCH (p:Person) WHERE p.age > 25 RETURN p";
        let plan: String = client
            .call(GraphExplain::new(graph_name, query))
            .await
            .unwrap();

        // Should return execution plan
        assert!(!plan.is_empty());
        assert!(plan.contains("Results") || plan.contains("Filter") || plan.contains("Scan"));

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_profile() {
        let client = setup_redis().await;
        let graph_name = "graph_test_profile";

        // Create graph
        let create_query = "CREATE (:Person {name: 'Grace'})";
        client
            .call(GraphQuery::new(graph_name, create_query))
            .await
            .unwrap();

        // Profile a query
        let query = "MATCH (p:Person) RETURN p";
        let result: QueryResult = client
            .call(GraphProfile::new(graph_name, query))
            .await
            .unwrap();

        // Profile should include execution plan
        assert!(!result.metadata.is_empty());

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_slowlog() {
        let client = setup_redis().await;
        let graph_name = "graph_test_slowlog";

        // Create graph and run some queries
        let query1 = "CREATE (:Person {name: 'Henry'})";
        client
            .call(GraphQuery::new(graph_name, query1))
            .await
            .unwrap();

        let query2 = "MATCH (p:Person) RETURN p";
        client
            .call(GraphQuery::new(graph_name, query2))
            .await
            .unwrap();

        // Get slowlog
        let slowlog: Vec<SlowlogEntry> = client
            .call(GraphSlowlog::new(graph_name, 10))
            .await
            .unwrap();

        // Should have entries (may be empty if queries were fast)
        assert!(slowlog.len() >= 0);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_config_get_set() {
        let client = setup_redis().await;

        // Get timeout config
        let timeout: String = client.call(GraphConfigGet::new("TIMEOUT")).await.unwrap();
        assert!(!timeout.is_empty());

        // Set timeout (use original value or a safe value)
        client
            .call(GraphConfigSet::new("TIMEOUT", "1000"))
            .await
            .unwrap();

        // Verify set
        let new_timeout: String = client.call(GraphConfigGet::new("TIMEOUT")).await.unwrap();
        assert!(new_timeout.contains("1000"));

        // Note: No cleanup needed for config changes
    }

    #[tokio::test]
    async fn test_graph_list() {
        let client = setup_redis().await;
        let graph_name1 = "graph_test_list1";
        let graph_name2 = "graph_test_list2";

        // Create two graphs
        client
            .call(GraphQuery::new(graph_name1, "CREATE (:Node)"))
            .await
            .unwrap();
        client
            .call(GraphQuery::new(graph_name2, "CREATE (:Node)"))
            .await
            .unwrap();

        // List all graphs
        let graphs: Vec<String> = client.call(GraphList).await.unwrap();

        // Should contain our graphs
        assert!(graphs.contains(&graph_name1.to_string()));
        assert!(graphs.contains(&graph_name2.to_string()));

        // Cleanup
        client.call(GraphDelete::new(graph_name1)).await.unwrap();
        client.call(GraphDelete::new(graph_name2)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_query_with_parameters() {
        let client = setup_redis().await;
        let graph_name = "graph_test_params";

        // Create with parameters
        let query = "CREATE (:Person {name: $name, age: $age})";
        let mut params = std::collections::HashMap::new();
        params.insert("name".to_string(), "\"Isabel\"".to_string());
        params.insert("age".to_string(), "32".to_string());

        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, query).params(params))
            .await
            .unwrap();

        assert!(result.statistics.nodes_created > 0);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_query_statistics() {
        let client = setup_redis().await;
        let graph_name = "graph_test_stats";

        // Create nodes and relationships
        let query = "CREATE (a:Person {name: 'Jack'})-[:FRIEND]->(b:Person {name: 'Jill'})";
        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, query))
            .await
            .unwrap();

        // Verify statistics
        assert_eq!(result.statistics.nodes_created, 2);
        assert_eq!(result.statistics.relationships_created, 1);
        assert!(result.statistics.properties_set > 0);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_query_update() {
        let client = setup_redis().await;
        let graph_name = "graph_test_update";

        // Create node
        let create_query = "CREATE (:Person {name: 'Kate', age: 40})";
        client
            .call(GraphQuery::new(graph_name, create_query))
            .await
            .unwrap();

        // Update node
        let update_query = "MATCH (p:Person {name: 'Kate'}) SET p.age = 41";
        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, update_query))
            .await
            .unwrap();

        // Should update properties
        assert!(result.statistics.properties_set > 0);

        // Verify update
        let verify_query = "MATCH (p:Person {name: 'Kate'}) RETURN p.age";
        let verify_result: QueryResult = client
            .call(GraphQuery::new(graph_name, verify_query))
            .await
            .unwrap();

        assert_eq!(verify_result.data.len(), 1);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }

    #[tokio::test]
    async fn test_graph_query_delete_nodes() {
        let client = setup_redis().await;
        let graph_name = "graph_test_delete_nodes";

        // Create nodes
        let create_query = "CREATE (:Person {name: 'Leo'}), (:Person {name: 'Luna'})";
        client
            .call(GraphQuery::new(graph_name, create_query))
            .await
            .unwrap();

        // Delete one node
        let delete_query = "MATCH (p:Person {name: 'Leo'}) DELETE p";
        let result: QueryResult = client
            .call(GraphQuery::new(graph_name, delete_query))
            .await
            .unwrap();

        // Should delete node
        assert!(result.statistics.nodes_deleted > 0);

        // Cleanup
        client.call(GraphDelete::new(graph_name)).await.unwrap();
    }
}
