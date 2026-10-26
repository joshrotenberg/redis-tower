//! RESP2/RESP3 parity for typed `parse_response`.
//!
//! `connect()` negotiates RESP3 by default, and several commands reply with
//! different frame types per protocol. Each test feeds the exact RESP2 and
//! RESP3 wire bytes captured from redis-server 8.10.2 (with RedisJSON,
//! RedisBloom, and RedisTimeSeries loaded) through the real `RespCodec`, and
//! expects both to decode to the same typed value.

use bytes::{Bytes, BytesMut};
use redis_tower_commands::*;
use redis_tower_core::{Command, Frame};
use redis_tower_protocol::RespCodec;
use tokio_util::codec::Decoder;

/// Decode one complete reply from wire bytes.
fn wire(bytes: &[u8]) -> Frame {
    let mut buf = BytesMut::from(bytes);
    let frame = RespCodec::new()
        .decode(&mut buf)
        .expect("decode")
        .expect("complete frame");
    assert!(buf.is_empty(), "trailing bytes after one reply");
    frame
}

/// Parse the RESP2 and RESP3 forms of one reply and assert they agree.
fn parity<C>(cmd: &C, resp2: &[u8], resp3: &[u8]) -> C::Response
where
    C: Command,
    C::Response: PartialEq + std::fmt::Debug,
{
    let from_resp2 = cmd
        .parse_response(wire(resp2))
        .unwrap_or_else(|e| panic!("{} RESP2 reply failed to parse: {e}", cmd.name()));
    let from_resp3 = cmd
        .parse_response(wire(resp3))
        .unwrap_or_else(|e| panic!("{} RESP3 reply failed to parse: {e}", cmd.name()));
    assert_eq!(from_resp2, from_resp3, "{} differs by protocol", cmd.name());
    from_resp2
}

fn b(s: &str) -> Bytes {
    Bytes::from(s.to_string())
}

// -- Special doubles: RESP3 `,inf` / `,-inf` / `,nan` decode to SpecialFloat --

#[test]
fn resp3_special_doubles_decode_to_special_float() {
    for bytes in [&b",inf\r\n"[..], b",-inf\r\n", b",nan\r\n"] {
        assert!(matches!(wire(bytes), Frame::SpecialFloat(_)));
    }
}

#[test]
fn zscore_infinite_and_missing() {
    let cmd = ZScore::new("zi", "top");
    assert_eq!(
        parity(&cmd, b"$3\r\ninf\r\n", b",inf\r\n"),
        Some(f64::INFINITY)
    );
    assert_eq!(
        parity(&cmd, b"$4\r\n-inf\r\n", b",-inf\r\n"),
        Some(f64::NEG_INFINITY)
    );
    assert_eq!(parity(&cmd, b"$-1\r\n", b"_\r\n"), None);
}

#[test]
fn zmscore_infinite_and_missing() {
    let cmd = ZMScore::members("zi", ["top", "nope", "bottom"]);
    assert_eq!(
        parity(
            &cmd,
            b"*3\r\n$3\r\ninf\r\n$-1\r\n$4\r\n-inf\r\n",
            b"*3\r\n,inf\r\n_\r\n,-inf\r\n"
        ),
        vec![Some(f64::INFINITY), None, Some(f64::NEG_INFINITY)]
    );
}

#[test]
fn zincrby_and_zadd_incr_infinite() {
    assert_eq!(
        parity(
            &ZIncrBy::new("zi", 1.0, "top"),
            b"$3\r\ninf\r\n",
            b",inf\r\n"
        ),
        f64::INFINITY
    );
    assert_eq!(
        parity(
            &ZAddIncr::new("zi", 1.0, "bottom"),
            b"$4\r\n-inf\r\n",
            b",-inf\r\n"
        ),
        Some(f64::NEG_INFINITY)
    );
}

#[test]
fn withscores_reads_infinite_scores() {
    let resp2 =
        b"*6\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n$3\r\nmid\r\n$1\r\n0\r\n$3\r\ntop\r\n$3\r\ninf\r\n";
    let resp3 = b"*3\r\n*2\r\n$6\r\nbottom\r\n,-inf\r\n*2\r\n$3\r\nmid\r\n,0\r\n*2\r\n$3\r\ntop\r\n,inf\r\n";
    let expected = vec![
        (b("bottom"), f64::NEG_INFINITY),
        (b("mid"), 0.0),
        (b("top"), f64::INFINITY),
    ];
    assert_eq!(
        parity(&ZUnionWithScores::new(["zi"]), resp2, resp3),
        expected
    );
    assert_eq!(
        parity(&ZInterWithScores::new(["zi"]), resp2, resp3),
        expected
    );
    assert_eq!(
        parity(&ZDiffWithScores::new(["zi"]), resp2, resp3),
        expected
    );
    assert_eq!(
        parity(&ZRevRangeWithScores::new("zi", 0, -1), resp2, resp3),
        expected
    );
    assert_eq!(
        parity(
            &ZRevRangeByScoreWithScores::new("zi", "+inf", "-inf"),
            resp2,
            resp3
        ),
        expected
    );
}

// -- ZPOPMIN/ZPOPMAX: flat without COUNT, nested pairs with COUNT in RESP3 --

#[test]
fn zpopmin_without_count_is_flat_in_both_protocols() {
    assert_eq!(
        parity(
            &ZPopMin::new("zi"),
            b"*2\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n",
            b"*2\r\n$6\r\nbottom\r\n,-inf\r\n"
        ),
        vec![(b("bottom"), f64::NEG_INFINITY)]
    );
}

#[test]
fn zpop_with_count_reads_resp3_nested_pairs() {
    assert_eq!(
        parity(
            &ZPopMax::new("zi").count(1),
            b"*2\r\n$3\r\ntop\r\n$3\r\ninf\r\n",
            b"*1\r\n*2\r\n$3\r\ntop\r\n,inf\r\n"
        ),
        vec![(b("top"), f64::INFINITY)]
    );
    assert_eq!(
        parity(
            &ZPopMax::new("z").count(2),
            b"*4\r\n$1\r\nb\r\n$3\r\n2.5\r\n$1\r\na\r\n$1\r\n1\r\n",
            b"*2\r\n*2\r\n$1\r\nb\r\n,2.5\r\n*2\r\n$1\r\na\r\n,1\r\n"
        ),
        vec![(b("b"), 2.5), (b("a"), 1.0)]
    );
    assert_eq!(
        parity(
            &ZPopMin::new("z").count(1),
            b"*2\r\n$1\r\nc\r\n$2\r\n-3\r\n",
            b"*1\r\n*2\r\n$1\r\nc\r\n,-3\r\n"
        ),
        vec![(b("c"), -3.0)]
    );
    assert!(parity(&ZPopMin::new("nokey").count(2), b"*0\r\n", b"*0\r\n").is_empty());
}

#[test]
fn zmpop_and_bzmpop_infinite_scores() {
    assert_eq!(
        parity(
            &ZMPop::new(["zi"], ZMPopDirection::Min).count(2),
            b"*2\r\n$2\r\nzi\r\n*2\r\n*2\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n*2\r\n$3\r\nmid\r\n$1\r\n0\r\n",
            b"*2\r\n$2\r\nzi\r\n*2\r\n*2\r\n$6\r\nbottom\r\n,-inf\r\n*2\r\n$3\r\nmid\r\n,0\r\n"
        ),
        Some((
            b("zi"),
            vec![(b("bottom"), f64::NEG_INFINITY), (b("mid"), 0.0)]
        ))
    );
    assert_eq!(
        parity(
            &BZMPop::new(1.0, ["zi"], ZMPopDirection::Max),
            b"*2\r\n$2\r\nzi\r\n*1\r\n*2\r\n$3\r\ntop\r\n$3\r\ninf\r\n",
            b"*2\r\n$2\r\nzi\r\n*1\r\n*2\r\n$3\r\ntop\r\n,inf\r\n"
        ),
        Some((b("zi"), vec![(b("top"), f64::INFINITY)]))
    );
    assert_eq!(
        parity(
            &ZMPop::new(["nokey"], ZMPopDirection::Min),
            b"*-1\r\n",
            b"_\r\n"
        ),
        None
    );
}

#[test]
fn bzpop_infinite_scores() {
    assert_eq!(
        parity(
            &BZPopMax::new("zi", 1.0),
            b"*3\r\n$2\r\nzi\r\n$3\r\ntop\r\n$3\r\ninf\r\n",
            b"*3\r\n$2\r\nzi\r\n$3\r\ntop\r\n,inf\r\n"
        ),
        Some((b("zi"), b("top"), f64::INFINITY))
    );
    assert_eq!(
        parity(
            &BZPopMin::new("zi", 1.0),
            b"*3\r\n$2\r\nzi\r\n$6\r\nbottom\r\n$4\r\n-inf\r\n",
            b"*3\r\n$2\r\nzi\r\n$6\r\nbottom\r\n,-inf\r\n"
        ),
        Some((b("zi"), b("bottom"), f64::NEG_INFINITY))
    );
}

// -- Geo --

#[test]
fn geopos_reads_resp3_doubles() {
    let pos = parity(
        &GeoPos::members("g", ["Palermo", "nope"]),
        b"*2\r\n*2\r\n$18\r\n13.361389338970184\r\n$16\r\n38.1155563954963\r\n*-1\r\n",
        b"*2\r\n*2\r\n,13.361389338970184\r\n,38.1155563954963\r\n_\r\n",
    );
    assert_eq!(
        pos,
        vec![Some((13.361389338970184, 38.1155563954963)), None]
    );
}

#[test]
fn geodist_missing_member() {
    assert_eq!(
        parity(&GeoDist::new("g", "a", "b"), b"$-1\r\n", b"_\r\n"),
        None
    );
}

// -- Sets: RESP3 null and set replies --

#[test]
fn spop_and_srandmember_missing_key() {
    assert!(parity(&SPop::new("nokey"), b"$-1\r\n", b"_\r\n").is_empty());
    assert!(parity(&SRandMember::new("nokey"), b"$-1\r\n", b"_\r\n").is_empty());
    assert!(parity(&SPop::new("nokey").count(2), b"*0\r\n", b"~0\r\n").is_empty());
}

#[test]
fn spop_with_count_reads_resp3_set() {
    assert_eq!(
        parity(
            &SPop::new("s").count(2),
            b"*2\r\n$1\r\na\r\n$1\r\nd\r\n",
            b"~2\r\n$1\r\na\r\n$1\r\nd\r\n"
        ),
        vec![b("a"), b("d")]
    );
}

// -- LPOS --

#[test]
fn lpos_miss_reads_resp2_nil_bulk() {
    assert_eq!(parity(&LPos::new("l", "nope"), b"$-1\r\n", b"_\r\n"), None);
    assert_eq!(parity(&LPos::new("l", "x"), b":1\r\n", b":1\r\n"), Some(1));
}

#[test]
fn lpos_with_count_returns_positions() {
    assert_eq!(
        parity(
            &LPos::new("l", "x").with_count(0),
            b"*2\r\n:1\r\n:3\r\n",
            b"*2\r\n:1\r\n:3\r\n"
        ),
        vec![1, 3]
    );
    assert!(parity(&LPos::new("l", "nope").with_count(0), b"*0\r\n", b"*0\r\n").is_empty());
}

// -- Streams: a pending entry deleted after delivery --

#[test]
fn xreadgroup_pending_entry_with_deleted_payload() {
    let cmd = XReadGroup::new("grp", "alice", "st").with_id("0");
    let streams = parity(
        &cmd,
        b"*1\r\n*2\r\n$2\r\nst\r\n*2\r\n*2\r\n$6\r\n1000-0\r\n*-1\r\n*2\r\n$6\r\n1000-1\r\n*2\r\n$5\r\nhello\r\n$6\r\nworld2\r\n",
        b"%1\r\n$2\r\nst\r\n*2\r\n*2\r\n$6\r\n1000-0\r\n_\r\n*2\r\n$6\r\n1000-1\r\n*2\r\n$5\r\nhello\r\n$6\r\nworld2\r\n",
    );
    let (_, entries) = &streams[0];
    assert_eq!(entries[0].id, "1000-0");
    assert!(entries[0].is_deleted());
    assert!(!entries[1].is_deleted());
    assert_eq!(entries[1].fields, vec![(b("hello"), b("world2"))]);
}

// -- T-Digest: empty digests reply nan --

#[test]
fn tdigest_nan_and_finite_values() {
    let min = TdigestMin::new("td");
    assert!(parity_nan(&min, b"$3\r\nnan\r\n", b",nan\r\n"));
    assert_eq!(parity(&min, b"$1\r\n1\r\n", b",1\r\n"), 1.0);

    let quantile = TdigestQuantile::new("td", [0.5]);
    let resp2 = quantile
        .parse_response(wire(b"*1\r\n$3\r\nnan\r\n"))
        .unwrap();
    let resp3 = quantile.parse_response(wire(b"*1\r\n,nan\r\n")).unwrap();
    assert!(resp2[0].is_nan() && resp3[0].is_nan());
    assert_eq!(
        parity(
            &TdigestQuantile::new("td", [0.5, 1.0]),
            b"*2\r\n$1\r\n2\r\n$1\r\n3\r\n",
            b"*2\r\n,2\r\n,3\r\n"
        ),
        vec![2.0, 3.0]
    );
}

/// `NaN != NaN`, so parity for a NaN reply checks both sides are NaN.
fn parity_nan<C: Command<Response = f64>>(cmd: &C, resp2: &[u8], resp3: &[u8]) -> bool {
    let from_resp2 = cmd.parse_response(wire(resp2)).expect("RESP2 parses");
    let from_resp3 = cmd.parse_response(wire(resp3)).expect("RESP3 parses");
    from_resp2.is_nan() && from_resp3.is_nan()
}

// -- Vector sets --

#[test]
fn vemb_reads_resp3_doubles() {
    assert_eq!(
        parity(
            &VEmb::new("vs", "e1"),
            b"*2\r\n$1\r\n1\r\n$1\r\n0\r\n",
            b"*2\r\n,1\r\n,0\r\n"
        ),
        vec![1.0, 0.0]
    );
}

#[test]
fn vlinks_returns_one_list_per_layer() {
    assert_eq!(
        parity(
            &VLinks::new("vs", "e1"),
            b"*1\r\n*2\r\n$2\r\ne2\r\n$2\r\ne3\r\n",
            b"*1\r\n*2\r\n$2\r\ne2\r\n$2\r\ne3\r\n"
        ),
        Some(vec![vec![(b("e2"), None), (b("e3"), None)]])
    );
    assert_eq!(
        parity(
            &VLinks::new("vs", "e1").withscores(),
            b"*1\r\n*4\r\n$2\r\ne2\r\n$3\r\n0.5\r\n$2\r\ne3\r\n$18\r\n0.8535534143447876\r\n",
            b"*1\r\n%2\r\n$2\r\ne2\r\n,0.5\r\n$2\r\ne3\r\n,0.8535534143447876\r\n"
        ),
        Some(vec![vec![
            (b("e2"), Some(0.5)),
            (b("e3"), Some(0.8535534143447876))
        ]])
    );
    assert_eq!(
        parity(&VLinks::new("vs", "nope"), b"$-1\r\n", b"_\r\n"),
        None
    );
}

#[test]
fn vlinks_keeps_layers_separate() {
    let layers = parity(
        &VLinks::new("vs", "e1"),
        b"*2\r\n*1\r\n$2\r\ne2\r\n*2\r\n$2\r\ne2\r\n$2\r\ne3\r\n",
        b"*2\r\n*1\r\n$2\r\ne2\r\n*2\r\n$2\r\ne2\r\n$2\r\ne3\r\n",
    )
    .expect("element exists");
    assert_eq!(layers.len(), 2);
    assert_eq!(layers[1].len(), 2, "layer 0 is last");
}

#[test]
fn vsim_withscores_flat_and_map() {
    assert_eq!(
        parity(
            &VSim::by_element("vs", "e1").withscores(),
            b"*4\r\n$2\r\ne1\r\n$1\r\n1\r\n$2\r\ne2\r\n$3\r\n0.5\r\n",
            b"%2\r\n$2\r\ne1\r\n,1\r\n$2\r\ne2\r\n,0.5\r\n"
        ),
        vec![(b("e1"), Some(1.0)), (b("e2"), Some(0.5))]
    );
}

// -- RedisJSON --

#[test]
fn json_set_condition_not_met_is_not_an_error() {
    let cmd = JsonSet::new("j", "$", r#"{"a":2}"#).nx();
    parity(&cmd, b"$-1\r\n", b"_\r\n");
    parity(&cmd, b"+OK\r\n", b"+OK\r\n");
}

#[test]
fn json_set_with_outcome_reports_unmet_condition() {
    let cmd = JsonSet::new("j", "$", r#"{"a":2}"#).nx().with_outcome();
    assert_eq!(parity(&cmd, b"$-1\r\n", b"_\r\n"), SetStatus::NotApplied);
    assert_eq!(parity(&cmd, b"+OK\r\n", b"+OK\r\n"), SetStatus::Applied);

    let xx = JsonSet::new("j", "$.zz", "1").xx().with_outcome();
    assert_eq!(parity(&xx, b"$-1\r\n", b"_\r\n"), SetStatus::NotApplied);

    // Without a condition, Redis never replies null.
    let plain = JsonSet::new("j", "$", "1").with_outcome();
    assert!(plain.parse_response(wire(b"_\r\n")).is_err());
}

#[test]
fn json_numincrby_jsonpath_renders_the_resp2_text() {
    let cmd = JsonNumIncrBy::new("j", "$.a", 1.5);
    assert_eq!(
        parity(&cmd, b"$5\r\n[6.5]\r\n", b"*1\r\n,6.5\r\n"),
        b("[6.5]")
    );
    // Whole floats keep their fractional part, integers do not.
    assert_eq!(
        parity(&cmd, b"$5\r\n[2.0]\r\n", b"*1\r\n,2\r\n"),
        b("[2.0]")
    );
    assert_eq!(parity(&cmd, b"$4\r\n[11]\r\n", b"*1\r\n:11\r\n"), b("[11]"));
    // A match that is not a number reports null.
    let multi = JsonNumIncrBy::new("j", "$..s", 1.0);
    assert_eq!(
        parity(&multi, b"$8\r\n[null,6]\r\n", b"*2\r\n_\r\n:6\r\n"),
        b("[null,6]")
    );
    assert_eq!(parity(&multi, b"$2\r\n[]\r\n", b"*0\r\n"), b("[]"));
}

#[test]
fn json_numincrby_legacy_path_renders_a_bare_number() {
    // RESP3 wraps even a legacy-path result in a one-element array.
    let cmd = JsonNumIncrBy::new("j", ".a", 0.25);
    assert_eq!(
        parity(&cmd, b"$4\r\n2.25\r\n", b"*1\r\n,2.25\r\n"),
        b("2.25")
    );
}

#[test]
fn json_arrpop_jsonpath_single_match() {
    let cmd = JsonArrPop::new("j").path("$.b");
    assert_eq!(
        parity(&cmd, b"*1\r\n$1\r\n4\r\n", b"*1\r\n$1\r\n4\r\n"),
        Some(b("4"))
    );
    assert_eq!(parity(&cmd, b"*0\r\n", b"*0\r\n"), None);
    assert_eq!(parity(&cmd, b"*1\r\n$-1\r\n", b"*1\r\n_\r\n"), None);
    // Legacy path: the value itself.
    let legacy = JsonArrPop::new("j").path(".b");
    assert_eq!(
        parity(&legacy, b"$1\r\n3\r\n", b"$1\r\n3\r\n"),
        Some(b("3"))
    );
}

#[test]
fn json_arrpop_several_matches_needs_all_matches() {
    let resp2 = b"*2\r\n$1\r\n1\r\n$-1\r\n";
    let resp3 = b"*2\r\n$1\r\n1\r\n_\r\n";
    let single = JsonArrPop::new("j").path("$..s");
    let err = single.parse_response(wire(resp2)).unwrap_err();
    assert!(err.to_string().contains("all_matches"), "{err}");

    let all = JsonArrPop::new("j").path("$..s").all_matches();
    assert_eq!(parity(&all, resp2, resp3), vec![Some(b("1")), None]);
    // A legacy path's value becomes a one-element list.
    let legacy = JsonArrPop::new("j").path(".b").all_matches();
    assert_eq!(
        parity(&legacy, b"$1\r\n3\r\n", b"$1\r\n3\r\n"),
        vec![Some(b("3"))]
    );
}
