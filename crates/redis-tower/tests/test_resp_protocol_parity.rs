//! Live RESP2/RESP3 parity for typed responses.
//!
//! The same commands run over a RESP2 and a RESP3 connection, and the typed
//! results must match. These cover replies whose wire types differ by
//! protocol: special doubles (`inf`, `-inf`), nested score pairs, RESP3 nulls
//! and sets, LPOS with COUNT, and pending stream entries deleted after
//! delivery. The frame-level counterparts with captured wire bytes live in
//! `redis-tower-commands/tests/resp_protocol_parity.rs`.

mod common;

use bytes::Bytes;
use common::redis_addr;
use redis_tower::commands::*;
use redis_tower::{ProtocolVersion, RedisConnection};

/// Typed results that must not depend on the negotiated protocol.
#[derive(Debug, PartialEq)]
struct Observed {
    zscore_top: Option<f64>,
    zscore_missing: Option<f64>,
    zmscore: Vec<Option<f64>>,
    zincrby_top: f64,
    zadd_incr_bottom: Option<f64>,
    zrevrange: Vec<(Bytes, f64)>,
    zunion: Vec<(Bytes, f64)>,
    zpopmin_no_count: Vec<(Bytes, f64)>,
    zpopmax_count: Vec<(Bytes, f64)>,
    zmpop: Option<(Bytes, Vec<(Bytes, f64)>)>,
    bzpopmax: Option<(Bytes, Bytes, f64)>,
    geopos: Vec<Option<(f64, f64)>>,
    spop_missing: Vec<Bytes>,
    srandmember_missing: Vec<Bytes>,
    spop_count_len: usize,
    lpos_missing: Option<i64>,
    lpos_all: Vec<i64>,
    xreadgroup_pending: Vec<(String, bool)>,
    raw_zrange_pairs: Vec<(String, f64)>,
    raw_hgetall_flat: Vec<String>,
    raw_zscore_string: String,
}

async fn observe(protocol: ProtocolVersion, prefix: &str) -> Observed {
    let mut c = RedisConnection::connect_with_protocol(redis_addr().await, protocol)
        .await
        .expect("connect");
    let k = |name: &str| format!("{prefix}:{name}");
    let (zi, z, g, s, l, st, h) = (k("zi"), k("z"), k("g"), k("s"), k("l"), k("st"), k("h"));
    c.execute(Del::keys([&zi, &z, &g, &s, &l, &st, &h]))
        .await
        .unwrap();

    let reset_zi = || {
        RawCommand::new("ZADD")
            .arg(&zi)
            .arg("+inf")
            .arg("top")
            .arg("-inf")
            .arg("bottom")
            .arg("0")
            .arg("mid")
    };
    c.execute(reset_zi()).await.unwrap();
    let zscore_top = c.execute(ZScore::new(&zi, "top")).await.unwrap();
    let zscore_missing = c.execute(ZScore::new(&zi, "nope")).await.unwrap();
    let zmscore = c
        .execute(ZMScore::members(&zi, ["top", "nope", "bottom"]))
        .await
        .unwrap();
    let zincrby_top = c.execute(ZIncrBy::new(&zi, 1.0, "top")).await.unwrap();
    let zadd_incr_bottom = c.execute(ZAddIncr::new(&zi, 1.0, "bottom")).await.unwrap();
    let zrevrange = c
        .execute(ZRevRangeWithScores::new(&zi, 0, -1))
        .await
        .unwrap();
    let zunion = c.execute(ZUnionWithScores::new([&zi])).await.unwrap();
    let zpopmin_no_count = c.execute(ZPopMin::new(&zi)).await.unwrap();
    c.execute(reset_zi()).await.unwrap();
    let zpopmax_count = c.execute(ZPopMax::new(&zi).count(2)).await.unwrap();
    c.execute(reset_zi()).await.unwrap();
    let zmpop = c
        .execute(ZMPop::new([&zi], ZMPopDirection::Min).count(2))
        .await
        .unwrap();
    let bzpopmax = c.execute(BZPopMax::new(&zi, 1.0)).await.unwrap();

    c.execute(GeoAdd::new(&g).member(13.361389, 38.115556, "Palermo"))
        .await
        .unwrap();
    let geopos = c
        .execute(GeoPos::members(&g, ["Palermo", "nope"]))
        .await
        .unwrap();

    let spop_missing = c.execute(SPop::new(k("nokey"))).await.unwrap();
    let srandmember_missing = c.execute(SRandMember::new(k("nokey"))).await.unwrap();
    c.execute(SAdd::members(&s, ["a", "b", "c", "d"]))
        .await
        .unwrap();
    let spop_count_len = c.execute(SPop::new(&s).count(2)).await.unwrap().len();

    c.execute(RPush::elements(&l, ["a", "x", "b", "x"]))
        .await
        .unwrap();
    let lpos_missing = c.execute(LPos::new(&l, "nope")).await.unwrap();
    let lpos_all = c.execute(LPos::new(&l, "x").with_count(0)).await.unwrap();

    // A consumer reads two entries, then the first is deleted while pending.
    c.execute(XAdd::new(&st).id("1000-0").field("hello", "world"))
        .await
        .unwrap();
    c.execute(XAdd::new(&st).id("1000-1").field("hello", "world2"))
        .await
        .unwrap();
    c.execute(XGroupCreate::new(&st, "grp", "0")).await.unwrap();
    c.execute(XReadGroup::new("grp", "alice", &st))
        .await
        .unwrap();
    c.execute(XDel::new(&st, "1000-0")).await.unwrap();
    let pending = c
        .execute(XReadGroup::new("grp", "alice", &st).with_id("0"))
        .await
        .expect("a deleted pending entry must not fail the read");
    let xreadgroup_pending = pending
        .into_iter()
        .flat_map(|(_, entries)| entries)
        .map(|entry| (entry.id.clone(), entry.is_deleted()))
        .collect();

    // Dynamic decoding through FromFrame.
    c.execute(reset_zi()).await.unwrap();
    let raw_zrange_pairs = c
        .execute(
            RawCommand::new("ZRANGE")
                .arg(&zi)
                .arg("0")
                .arg("-1")
                .arg("WITHSCORES")
                .query::<Vec<(String, f64)>>(),
        )
        .await
        .unwrap();
    c.execute(HSet::new(&h, "f", "v")).await.unwrap();
    let raw_hgetall_flat = c
        .execute(RawCommand::new("HGETALL").arg(&h).query::<Vec<String>>())
        .await
        .unwrap();
    c.execute(ZAdd::new(&z).member(2.5, "m")).await.unwrap();
    let raw_zscore_string = c
        .execute(RawCommand::new("ZSCORE").arg(&z).arg("m").query::<String>())
        .await
        .unwrap();

    c.execute(Del::keys([&zi, &z, &g, &s, &l, &st, &h]))
        .await
        .unwrap();

    Observed {
        zscore_top,
        zscore_missing,
        zmscore,
        zincrby_top,
        zadd_incr_bottom,
        zrevrange,
        zunion,
        zpopmin_no_count,
        zpopmax_count,
        zmpop,
        bzpopmax,
        geopos,
        spop_missing,
        srandmember_missing,
        spop_count_len,
        lpos_missing,
        lpos_all,
        xreadgroup_pending,
        raw_zrange_pairs,
        raw_hgetall_flat,
        raw_zscore_string,
    }
}

#[tokio::test]
async fn typed_responses_match_across_protocols() {
    // Same keys for both runs; each run deletes them before and after.
    let resp2 = observe(ProtocolVersion::Resp2, "parity").await;
    let resp3 = observe(ProtocolVersion::Resp3, "parity").await;
    assert_eq!(resp2, resp3);

    let inf = f64::INFINITY;
    let neg = f64::NEG_INFINITY;
    let m = |s: &str| Bytes::from(s.to_string());
    assert_eq!(resp3.zscore_top, Some(inf));
    assert_eq!(resp3.zscore_missing, None);
    assert_eq!(resp3.zmscore, vec![Some(inf), None, Some(neg)]);
    assert_eq!(resp3.zincrby_top, inf);
    assert_eq!(resp3.zadd_incr_bottom, Some(neg));
    assert_eq!(
        resp3.zrevrange,
        vec![(m("top"), inf), (m("mid"), 0.0), (m("bottom"), neg)]
    );
    assert_eq!(resp3.zpopmin_no_count, vec![(m("bottom"), neg)]);
    assert_eq!(resp3.zpopmax_count, vec![(m("top"), inf), (m("mid"), 0.0)]);
    assert_eq!(
        resp3.zmpop.as_ref().map(|(_, members)| members.clone()),
        Some(vec![(m("bottom"), neg), (m("mid"), 0.0)])
    );
    assert_eq!(
        resp3.bzpopmax.as_ref().map(|(_, m, s)| (m.clone(), *s)),
        Some((m("top"), inf))
    );
    assert!(resp3.geopos[0].is_some() && resp3.geopos[1].is_none());
    assert!(resp3.spop_missing.is_empty() && resp3.srandmember_missing.is_empty());
    assert_eq!(resp3.spop_count_len, 2);
    assert_eq!(resp3.lpos_missing, None);
    assert_eq!(resp3.lpos_all, vec![1, 3]);
    assert_eq!(
        resp3.xreadgroup_pending,
        vec![("1000-0".to_string(), true), ("1000-1".to_string(), false)]
    );
    assert_eq!(
        resp3.raw_zrange_pairs,
        vec![
            ("bottom".to_string(), neg),
            ("mid".to_string(), 0.0),
            ("top".to_string(), inf)
        ]
    );
    assert_eq!(resp3.raw_hgetall_flat, vec!["f", "v"]);
    assert_eq!(resp3.raw_zscore_string, "2.5");
}
