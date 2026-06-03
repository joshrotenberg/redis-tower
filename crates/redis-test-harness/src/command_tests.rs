/// Generate command tests for any connection type that has an `execute` method.
///
/// Usage:
/// ```ignore
/// // Standalone (tests run normally):
/// command_tests!(my_conn_fn, "prefix");
///
/// // Cluster (tests are #[ignore] -- need a running cluster):
/// command_tests!(my_conn_fn, "prefix", ignored);
/// ```
///
/// The caller must have `bytes::Bytes` and all command types in scope.
#[macro_export]
macro_rules! command_tests {
    ($conn_fn:ident, $prefix:expr) => {
        $crate::__command_tests_inner!($conn_fn, $prefix,);
    };
    ($conn_fn:ident, $prefix:expr, ignored) => {
        $crate::__command_tests_inner!($conn_fn, $prefix, #[ignore]);
    };
}

#[macro_export]
#[doc(hidden)]
macro_rules! __command_tests_inner {
    ($conn_fn:ident, $prefix:expr, $(#[$attr:meta])*) => {
        fn _cmd_key(test: &str, name: &str) -> String {
            format!("{}:{test}:{name}", $prefix)
        }

        // -- Strings --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_set_and_get() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("set_get", "k");
            c.execute(Set::new(&k, "bar")).await.unwrap();
            let val = c.execute(Get::new(&k)).await.unwrap();
            assert_eq!(val, Some(Bytes::from("bar")));
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_get_nonexistent() {
            let mut c = $conn_fn().await;
            let val = c.execute(Get::new(_cmd_key("get_none", "k"))).await.unwrap();
            assert_eq!(val, None);
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_set_with_ex() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("set_ex", "k");
            c.execute(Set::new(&k, "v").ex(10)).await.unwrap();
            let ttl = c.execute(Ttl::new(&k)).await.unwrap();
            assert!(ttl > 0 && ttl <= 10);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_set_nx() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("set_nx", "k");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(Set::new(&k, "first").nx()).await.unwrap();
            c.execute(Set::new(&k, "second").nx()).await.unwrap();
            let val = c.execute(Get::new(&k)).await.unwrap();
            assert_eq!(val, Some(Bytes::from("first")));
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_set_xx() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("set_xx", "k");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(Set::new(&k, "v").xx()).await.unwrap();
            let val = c.execute(Get::new(&k)).await.unwrap();
            assert_eq!(val, None);
            c.execute(Set::new(&k, "v")).await.unwrap();
            c.execute(Set::new(&k, "new").xx()).await.unwrap();
            let val = c.execute(Get::new(&k)).await.unwrap();
            assert_eq!(val, Some(Bytes::from("new")));
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_incr() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("incr", "k");
            c.execute(Set::new(&k, "10")).await.unwrap();
            let val = c.execute(Incr::new(&k)).await.unwrap();
            assert_eq!(val, 11);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_append() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("append", "k");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(Set::new(&k, "hello")).await.unwrap();
            let len = c.execute(Append::new(&k, " world")).await.unwrap();
            assert_eq!(len, 11);
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Keys --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_del() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("del", "k");
            c.execute(Set::new(&k, "x")).await.unwrap();
            let removed = c.execute(Del::new(&k)).await.unwrap();
            assert_eq!(removed, 1);
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_exists() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("exists", "k");
            c.execute(Set::new(&k, "x")).await.unwrap();
            assert_eq!(c.execute(Exists::new(&k)).await.unwrap(), 1);
            c.execute(Del::new(&k)).await.unwrap();
            assert_eq!(c.execute(Exists::new(&k)).await.unwrap(), 0);
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_expire_and_ttl() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("expire_ttl", "k");
            c.execute(Set::new(&k, "x")).await.unwrap();
            assert_eq!(c.execute(Ttl::new(&k)).await.unwrap(), -1);
            c.execute(Expire::new(&k, 60)).await.unwrap();
            let ttl = c.execute(Ttl::new(&k)).await.unwrap();
            assert!(ttl > 0 && ttl <= 60);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_type() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("type", "k");
            c.execute(Set::new(&k, "v")).await.unwrap();
            assert_eq!(c.execute(Type::new(&k)).await.unwrap(), "string");
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_ping() {
            let mut c = $conn_fn().await;
            let pong = c.execute(Ping::new()).await.unwrap();
            assert_eq!(pong, "PONG");
        }

        // -- Hashes --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_hset_hget() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("hset_hget", "h");
            c.execute(HSet::new(&k, "f1", "v1").field("f2", "v2")).await.unwrap();
            let val = c.execute(HGet::new(&k, "f1")).await.unwrap();
            assert_eq!(val, Some(Bytes::from("v1")));
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_hdel() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("hdel", "h");
            c.execute(HSet::new(&k, "a", "1").field("b", "2")).await.unwrap();
            let removed = c.execute(HDel::new(&k, "a")).await.unwrap();
            assert_eq!(removed, 1);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_hgetall() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("hgetall", "h");
            c.execute(HSet::new(&k, "a", "1").field("b", "2")).await.unwrap();
            let pairs = c.execute(HGetAll::new(&k)).await.unwrap();
            assert_eq!(pairs.len(), 2);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_hincrby() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("hincrby", "h");
            c.execute(HSet::new(&k, "n", "10")).await.unwrap();
            let val = c.execute(HIncrBy::new(&k, "n", 5)).await.unwrap();
            assert_eq!(val, 15);
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Lists --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_lpush_rpush_lrange() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("list_ops", "l");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(RPush::new(&k, "a")).await.unwrap();
            c.execute(RPush::new(&k, "b")).await.unwrap();
            c.execute(LPush::new(&k, "z")).await.unwrap();
            let items = c.execute(LRange::new(&k, 0, -1)).await.unwrap();
            assert_eq!(items, vec![Bytes::from("z"), Bytes::from("a"), Bytes::from("b")]);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_lpop_rpop() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("pop", "l");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(RPush::elements(&k, ["1", "2", "3"])).await.unwrap();
            assert_eq!(c.execute(LPop::new(&k)).await.unwrap(), Some(Bytes::from("1")));
            assert_eq!(c.execute(RPop::new(&k)).await.unwrap(), Some(Bytes::from("3")));
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_llen() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("llen", "l");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(RPush::elements(&k, ["a", "b", "c"])).await.unwrap();
            assert_eq!(c.execute(LLen::new(&k)).await.unwrap(), 3);
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Sets --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_sadd_smembers_scard() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("set_ops", "s");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(SAdd::members(&k, ["a", "b", "c"])).await.unwrap();
            assert_eq!(c.execute(SCard::new(&k)).await.unwrap(), 3);
            let members = c.execute(SMembers::new(&k)).await.unwrap();
            assert_eq!(members.len(), 3);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_sismember() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("sismember", "s");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(SAdd::new(&k, "x")).await.unwrap();
            assert!(c.execute(SIsMember::new(&k, "x")).await.unwrap());
            assert!(!c.execute(SIsMember::new(&k, "nope")).await.unwrap());
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Sorted Sets --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_zadd_zscore_zcard() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("zset_ops", "z");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(ZAdd::new(&k).member(1.0, "a").member(2.0, "b")).await.unwrap();
            assert_eq!(c.execute(ZCard::new(&k)).await.unwrap(), 2);
            assert_eq!(c.execute(ZScore::new(&k, "b")).await.unwrap(), Some(2.0));
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_zrange() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("zrange", "z");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(ZAdd::new(&k).member(1.0, "a").member(2.0, "b").member(3.0, "c")).await.unwrap();
            let range = c.execute(ZRange::new(&k, 0, -1)).await.unwrap();
            assert_eq!(range, vec![Bytes::from("a"), Bytes::from("b"), Bytes::from("c")]);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_zincrby() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("zincrby", "z");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(ZAdd::new(&k).member(10.0, "player")).await.unwrap();
            let score = c.execute(ZIncrBy::new(&k, 5.5, "player")).await.unwrap();
            assert!((score - 15.5).abs() < f64::EPSILON);
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Vector Sets (Redis 8.0+) --
        //
        // These tests skip gracefully if the server doesn't support
        // vector set commands (Redis < 8.0).

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_vadd_vcard_vdim() {
            let mut c = $conn_fn().await;
            // Skip if vector sets not supported (Redis < 8.0).
            let probe_k = _cmd_key("_vset_probe", "v");
            if c.execute(VAdd::new(&probe_k, vec![1.0_f32, 0.0, 0.0], "_p")).await.is_err() { return; }
            let _ = c.execute(Del::new(&probe_k)).await;
            let k = _cmd_key("vset_ops", "v");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(VAdd::new(&k, vec![1.0_f32, 2.0, 3.0], "a")).await.unwrap();
            c.execute(VAdd::new(&k, vec![4.0_f32, 5.0, 6.0], "b")).await.unwrap();
            assert_eq!(c.execute(VCard::new(&k)).await.unwrap(), 2);
            assert_eq!(c.execute(VDim::new(&k)).await.unwrap(), 3);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_vadd_vrem() {
            let mut c = $conn_fn().await;
            if c.execute(VAdd::new(_cmd_key("_p","v"), vec![1.0_f32,0.0,0.0], "_p")).await.is_err() { return; }
            let k = _cmd_key("vrem", "v");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(VAdd::new(&k, vec![1.0_f32, 2.0, 3.0], "a")).await.unwrap();
            assert!(c.execute(VRem::new(&k, "a")).await.unwrap());
            assert!(!c.execute(VRem::new(&k, "a")).await.unwrap());
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_vemb() {
            let mut c = $conn_fn().await;
            if c.execute(VAdd::new(_cmd_key("_p","v"), vec![1.0_f32,0.0,0.0], "_p")).await.is_err() { return; }
            let k = _cmd_key("vemb", "v");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(VAdd::new(&k, vec![1.0_f32, 2.0, 3.0], "a")).await.unwrap();
            let emb = c.execute(VEmb::new(&k, "a")).await.unwrap();
            assert_eq!(emb.len(), 3);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_vsim() {
            let mut c = $conn_fn().await;
            if c.execute(VAdd::new(_cmd_key("_p","v"), vec![1.0_f32,0.0,0.0], "_p")).await.is_err() { return; }
            let k = _cmd_key("vsim", "v");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(VAdd::new(&k, vec![1.0_f32, 0.0, 0.0], "x")).await.unwrap();
            c.execute(VAdd::new(&k, vec![0.0_f32, 1.0, 0.0], "y")).await.unwrap();
            c.execute(VAdd::new(&k, vec![0.9_f32, 0.1, 0.0], "close_to_x")).await.unwrap();
            let results = c.execute(VSim::by_element(&k, "x").count(2)).await.unwrap();
            assert!(!results.is_empty());
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_vsetattr_vgetattr() {
            let mut c = $conn_fn().await;
            if c.execute(VAdd::new(_cmd_key("_p","v"), vec![1.0_f32,0.0,0.0], "_p")).await.is_err() { return; }
            let k = _cmd_key("vattr", "v");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(VAdd::new(&k, vec![1.0_f32, 2.0, 3.0], "a")).await.unwrap();
            c.execute(VSetAttr::new(&k, "a", r#"{"color":"red"}"#)).await.unwrap();
            let attr = c.execute(VGetAttr::new(&k, "a")).await.unwrap();
            assert_eq!(attr, Some(r#"{"color":"red"}"#.to_string()));
            // Clear attribute by setting to empty string.
            c.execute(VSetAttr::new(&k, "a", "")).await.unwrap();
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_vinfo() {
            let mut c = $conn_fn().await;
            if c.execute(VAdd::new(_cmd_key("_p","v"), vec![1.0_f32,0.0,0.0], "_p")).await.is_err() { return; }
            let k = _cmd_key("vinfo", "v");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(VAdd::new(&k, vec![1.0_f32, 2.0, 3.0], "a")).await.unwrap();
            let info = c.execute(VInfo::new(&k)).await.unwrap();
            assert!(!info.is_empty());
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Bitmap --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_setbit_getbit() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("bitmap", "setbit");
            c.execute(Del::new(&k)).await.unwrap();
            let old = c.execute(SetBit::new(&k, 7, 1)).await.unwrap();
            assert_eq!(old, 0);
            let val = c.execute(GetBit::new(&k, 7)).await.unwrap();
            assert_eq!(val, 1);
            c.execute(Del::new(&k)).await.unwrap();
        }

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_bitcount() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("bitmap", "bitcount");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(SetBit::new(&k, 0, 1)).await.unwrap();
            c.execute(SetBit::new(&k, 7, 1)).await.unwrap();
            let count = c.execute(BitCount::new(&k)).await.unwrap();
            assert_eq!(count, 2);
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Geo --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_geoadd_geopos_geodist() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("geo", "sf");
            c.execute(Del::new(&k)).await.unwrap();
            let added = c.execute(
                GeoAdd::new(&k).member(-122.4194, 37.7749, "sf")
            ).await.unwrap();
            assert_eq!(added, 1);
            let pos = c.execute(GeoPos::new(&k, "sf")).await.unwrap();
            assert_eq!(pos.len(), 1);
            assert!(pos[0].is_some());
            let (lon, lat) = pos[0].unwrap();
            assert!((lon - (-122.4194)).abs() < 0.001);
            assert!((lat - 37.7749).abs() < 0.001);
            c.execute(GeoAdd::new(&k).member(-118.2437, 34.0522, "la")).await.unwrap();
            let dist = c.execute(
                GeoDist::new(&k, "sf", "la").unit(GeoUnit::Kilometers)
            ).await.unwrap();
            assert!(dist.is_some());
            let d = dist.unwrap();
            assert!((500.0..700.0).contains(&d), "SF-LA distance should be ~560km, got {d}");
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- HyperLogLog --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_pfadd_pfcount() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("hll", "pfadd");
            c.execute(Del::new(&k)).await.unwrap();
            c.execute(PfAdd::elements(&k, ["a", "b", "c", "d", "e"])).await.unwrap();
            let count = c.execute(PfCount::new(&k)).await.unwrap();
            assert!((4..=6).contains(&count), "HLL cardinality should be ~5, got {count}");
            c.execute(Del::new(&k)).await.unwrap();
        }

        // -- Streams --

        #[tokio::test]
        $(#[$attr])*
        async fn cmd_xadd_xlen_xrange() {
            let mut c = $conn_fn().await;
            let k = _cmd_key("stream", "basic");
            c.execute(Del::new(&k)).await.unwrap();
            let id = c.execute(XAdd::new(&k).field("sensor", "temp").field("value", "22"))
                .await.unwrap();
            assert!(!id.is_empty());
            let len = c.execute(XLen::new(&k)).await.unwrap();
            assert_eq!(len, 1);
            let entries = c.execute(XRange::all(&k)).await.unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].id, id);
            assert_eq!(entries[0].fields[0].0, "sensor");
            c.execute(Del::new(&k)).await.unwrap();
        }
    };
}
