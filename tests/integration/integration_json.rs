#[cfg(feature = "modules")]
mod tests {
    use redis_tower::RedisClient;
    use redis_tower::commands::strings::Del;
    use redis_tower::modules::json::*;
    use serde_json::json;

    async fn setup_redis() -> RedisClient {
        RedisClient::connect("localhost:6379")
            .await
            .expect("Failed to connect to Redis")
    }

    #[tokio::test]
    async fn test_json_set_get() {
        let client = setup_redis().await;
        let key = "json_test_set_get";

        // Set JSON document
        let doc = json!({"name": "Alice", "age": 30});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Get JSON document
        let result: String = client.call(JsonGet::new(key, "$")).await.unwrap();
        assert!(result.contains("Alice"));
        assert!(result.contains("30"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_set_path() {
        let client = setup_redis().await;
        let key = "json_test_set_path";

        // Set root document
        let doc = json!({"user": {"name": "Bob", "age": 25}});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Update nested field
        client
            .call(JsonSet::new(key, "$.user.age", "26".to_string()))
            .await
            .unwrap();

        // Get updated field
        let result: String = client.call(JsonGet::new(key, "$.user.age")).await.unwrap();
        assert!(result.contains("26"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_del() {
        let client = setup_redis().await;
        let key = "json_test_del";

        // Set document
        let doc = json!({"field1": "value1", "field2": "value2"});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Delete a field
        let deleted: i64 = client.call(JsonDel::new(key, "$.field1")).await.unwrap();
        assert_eq!(deleted, 1);

        // Verify deletion
        let result: String = client.call(JsonGet::new(key, "$")).await.unwrap();
        assert!(!result.contains("field1"));
        assert!(result.contains("field2"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_mget() {
        let client = setup_redis().await;
        let key1 = "json_test_mget1";
        let key2 = "json_test_mget2";

        // Set multiple documents
        client
            .call(JsonSet::new(key1, "$", json!({"id": 1}).to_string()))
            .await
            .unwrap();
        client
            .call(JsonSet::new(key2, "$", json!({"id": 2}).to_string()))
            .await
            .unwrap();

        // Get multiple documents
        let results: Vec<String> = client
            .call(JsonMGet::new(vec![key1, key2], "$"))
            .await
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].contains("\"id\":1") || results[0].contains("\"id\": 1"));
        assert!(results[1].contains("\"id\":2") || results[1].contains("\"id\": 2"));

        // Cleanup
        client
            .call(Del::new(vec![key1.to_string(), key2.to_string()]))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_json_arr_append() {
        let client = setup_redis().await;
        let key = "json_test_arr_append";

        // Set document with array
        let doc = json!({"items": [1, 2, 3]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Append to array
        let new_len: i64 = client
            .call(JsonArrAppend::new(key, "$.items", vec!["4".to_string()]))
            .await
            .unwrap();
        assert_eq!(new_len, 4);

        // Verify array
        let result: String = client.call(JsonGet::new(key, "$.items")).await.unwrap();
        assert!(result.contains("4"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_arr_index() {
        let client = setup_redis().await;
        let key = "json_test_arr_index";

        // Set document with array
        let doc = json!({"items": ["a", "b", "c", "d"]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Find index of element
        let index: i64 = client
            .call(JsonArrIndex::new(key, "$.items", "\"c\""))
            .await
            .unwrap();
        assert_eq!(index, 2);

        // Search for non-existent element
        let not_found: i64 = client
            .call(JsonArrIndex::new(key, "$.items", "\"z\""))
            .await
            .unwrap();
        assert_eq!(not_found, -1);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_arr_insert() {
        let client = setup_redis().await;
        let key = "json_test_arr_insert";

        // Set document with array
        let doc = json!({"items": [1, 3, 4]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Insert at index 1
        let new_len: i64 = client
            .call(JsonArrInsert::new(key, "$.items", 1, vec!["2".to_string()]))
            .await
            .unwrap();
        assert_eq!(new_len, 4);

        // Verify insertion
        let result: String = client.call(JsonGet::new(key, "$.items")).await.unwrap();
        assert!(result.contains("1") && result.contains("2") && result.contains("3"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_arr_len() {
        let client = setup_redis().await;
        let key = "json_test_arr_len";

        // Set document with array
        let doc = json!({"items": [1, 2, 3, 4, 5]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Get array length
        let len: i64 = client.call(JsonArrLen::new(key, "$.items")).await.unwrap();
        assert_eq!(len, 5);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_arr_pop() {
        let client = setup_redis().await;
        let key = "json_test_arr_pop";

        // Set document with array
        let doc = json!({"items": [1, 2, 3, 4, 5]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Pop last element (default)
        let popped: String = client.call(JsonArrPop::new(key, "$.items")).await.unwrap();
        assert!(popped.contains("5"));

        // Verify length
        let len: i64 = client.call(JsonArrLen::new(key, "$.items")).await.unwrap();
        assert_eq!(len, 4);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_arr_trim() {
        let client = setup_redis().await;
        let key = "json_test_arr_trim";

        // Set document with array
        let doc = json!({"items": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Trim to keep only indices 2-5
        let new_len: i64 = client
            .call(JsonArrTrim::new(key, "$.items", 2, 5))
            .await
            .unwrap();
        assert_eq!(new_len, 4); // Indices 2,3,4,5 = 4 elements

        // Verify trim
        let result: String = client.call(JsonGet::new(key, "$.items")).await.unwrap();
        assert!(!result.contains("1"));
        assert!(result.contains("3"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_obj_keys() {
        let client = setup_redis().await;
        let key = "json_test_obj_keys";

        // Set document with object
        let doc = json!({"user": {"name": "Alice", "age": 30, "city": "NYC"}});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Get object keys
        let keys: Vec<String> = client.call(JsonObjKeys::new(key, "$.user")).await.unwrap();
        assert!(keys.contains(&"name".to_string()));
        assert!(keys.contains(&"age".to_string()));
        assert!(keys.contains(&"city".to_string()));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_obj_len() {
        let client = setup_redis().await;
        let key = "json_test_obj_len";

        // Set document with object
        let doc = json!({"user": {"name": "Bob", "age": 25}});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Get object length (number of keys)
        let len: i64 = client.call(JsonObjLen::new(key, "$.user")).await.unwrap();
        assert_eq!(len, 2);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_num_incrby() {
        let client = setup_redis().await;
        let key = "json_test_num_incrby";

        // Set document with number
        let doc = json!({"counter": 10});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Increment by 5
        let new_val: String = client
            .call(JsonNumIncrBy::new(key, "$.counter", 5.0))
            .await
            .unwrap();
        assert!(new_val.contains("15"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_num_multby() {
        let client = setup_redis().await;
        let key = "json_test_num_multby";

        // Set document with number
        let doc = json!({"value": 10});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Multiply by 3
        let new_val: String = client
            .call(JsonNumMultBy::new(key, "$.value", 3.0))
            .await
            .unwrap();
        assert!(new_val.contains("30"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_str_append() {
        let client = setup_redis().await;
        let key = "json_test_str_append";

        // Set document with string
        let doc = json!({"message": "Hello"});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Append to string
        let new_len: i64 = client
            .call(JsonStrAppend::new(key, "$.message", "\" World\""))
            .await
            .unwrap();
        assert!(new_len > 5); // "Hello" was 5 chars

        // Verify append
        let result: String = client.call(JsonGet::new(key, "$.message")).await.unwrap();
        assert!(result.contains("Hello World"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_str_len() {
        let client = setup_redis().await;
        let key = "json_test_str_len";

        // Set document with string
        let doc = json!({"text": "Hello"});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Get string length
        let len: i64 = client.call(JsonStrLen::new(key, "$.text")).await.unwrap();
        assert_eq!(len, 5);

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_type() {
        let client = setup_redis().await;
        let key = "json_test_type";

        // Set document with various types
        let doc = json!({
            "string": "text",
            "number": 42,
            "bool": true,
            "null": null,
            "array": [1, 2, 3],
            "object": {"nested": true}
        });
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Get type of string field
        let type_str: String = client.call(JsonType::new(key, "$.string")).await.unwrap();
        assert!(type_str.contains("string"));

        // Get type of number field
        let type_num: String = client.call(JsonType::new(key, "$.number")).await.unwrap();
        assert!(type_num.contains("number") || type_num.contains("integer"));

        // Get type of array field
        let type_arr: String = client.call(JsonType::new(key, "$.array")).await.unwrap();
        assert!(type_arr.contains("array"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_clear() {
        let client = setup_redis().await;
        let key = "json_test_clear";

        // Set document with array
        let doc = json!({"items": [1, 2, 3, 4, 5]});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Clear the array
        let cleared: i64 = client.call(JsonClear::new(key, "$.items")).await.unwrap();
        assert_eq!(cleared, 1);

        // Verify cleared (array should be empty)
        let result: String = client.call(JsonGet::new(key, "$.items")).await.unwrap();
        assert!(result.contains("[]"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_toggle() {
        let client = setup_redis().await;
        let key = "json_test_toggle";

        // Set document with boolean
        let doc = json!({"flag": true});
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Toggle boolean
        let new_val: String = client.call(JsonToggle::new(key, "$.flag")).await.unwrap();
        assert!(new_val.contains("false"));

        // Toggle again
        let toggled_again: String = client.call(JsonToggle::new(key, "$.flag")).await.unwrap();
        assert!(toggled_again.contains("true"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }

    #[tokio::test]
    async fn test_json_nested_operations() {
        let client = setup_redis().await;
        let key = "json_test_nested";

        // Set complex nested document
        let doc = json!({
            "user": {
                "profile": {
                    "name": "Alice",
                    "scores": [10, 20, 30]
                }
            }
        });
        client
            .call(JsonSet::new(key, "$", doc.to_string()))
            .await
            .unwrap();

        // Update nested field
        client
            .call(JsonSet::new(
                key,
                "$.user.profile.name",
                "\"Bob\"".to_string(),
            ))
            .await
            .unwrap();

        // Append to nested array
        client
            .call(JsonArrAppend::new(
                key,
                "$.user.profile.scores",
                vec!["40".to_string()],
            ))
            .await
            .unwrap();

        // Verify changes
        let result: String = client
            .call(JsonGet::new(key, "$.user.profile"))
            .await
            .unwrap();
        assert!(result.contains("Bob"));
        assert!(result.contains("40"));

        // Cleanup
        client.call(Del::new(vec![key.to_string()])).await.unwrap();
    }
}
