//! Node-scoped commands: keyless commands whose effect or answer covers only
//! the node that runs them.
//!
//! Sent to the keyless default node, such a command returns a reply that
//! looks cluster-wide but is not: `DBSIZE` counts one master's keys,
//! `FLUSHALL` empties one master, and `SCRIPT LOAD` caches a script where an
//! `EVALSHA` for most slots cannot find it (redis-rs #2474). Both cluster
//! clients send the commands classified here to every node of the given
//! scope and combine the replies with [`aggregate`]:
//!
//! | Command | Nodes | Reply |
//! |---------|-------|-------|
//! | `DBSIZE` | masters | sum |
//! | `FLUSHALL`, `FLUSHDB`, `SAVE`, `BGSAVE` | masters | one reply once all succeed |
//! | `FUNCTION LOAD` / `FLUSH` / `DELETE` / `RESTORE` | masters | one reply once all succeed |
//! | `SCRIPT LOAD` / `FLUSH` | masters and connected replicas | one reply once all succeed |
//! | `SCRIPT EXISTS` | masters and connected replicas | element-wise AND |
//! | `CONFIG SET` / `RESETSTAT` / `REWRITE` | masters and connected replicas | one reply once all succeed |
//!
//! Replicas are included only for commands they accept, and only those the
//! client holds a connection to (`ReadPreference` other than `Master`), so
//! scripts loaded for `EVALSHA_RO` and settings changed by `CONFIG SET` also
//! reach the nodes replica reads use. Every other keyless command, including
//! node-local diagnostics such as `INFO`, `CONFIG GET`, and `CLIENT LIST`,
//! still runs on the default node; use
//! [`MultiplexedClusterClient::connect_to_node`](crate::MultiplexedClusterClient::connect_to_node)
//! to address one node explicitly. Explicit pipelines do not fan out.
//!
//! A fan-out is not atomic: when one node fails, the others may already have
//! applied the command, and the first error (in node-address order) is
//! returned.

use redis_tower_core::{Frame, RedisError};

/// Which nodes a node-scoped command runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeTargets {
    Masters,
    /// Masters plus every replica the client holds a connection to.
    MastersAndReplicas,
}

/// How per-node replies combine into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Aggregate {
    /// Integer replies are added.
    Sum,
    /// Every node must succeed; the first node's reply is returned.
    AllSucceeded,
    /// Arrays of 0/1 (or booleans) are combined element-wise with AND.
    LogicalAnd,
}

/// A node-scoped command's fan-out plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FanOut {
    pub(crate) targets: NodeTargets,
    pub(crate) aggregate: Aggregate,
}

/// The fan-out plan for `frame`, or `None` for an ordinary command.
pub(crate) fn fan_out(frame: &Frame) -> Option<FanOut> {
    let Frame::Array(Some(items)) = frame else {
        return None;
    };
    let name = upper(items.first()?)?;
    let subcommand = items.get(1).and_then(upper);
    let (targets, aggregate) = match (name.as_slice(), subcommand.as_deref()) {
        (b"DBSIZE", _) => (NodeTargets::Masters, Aggregate::Sum),
        (b"FLUSHALL" | b"FLUSHDB" | b"SAVE" | b"BGSAVE", _) => {
            (NodeTargets::Masters, Aggregate::AllSucceeded)
        }
        (b"FUNCTION", Some(b"LOAD" | b"FLUSH" | b"DELETE" | b"RESTORE")) => {
            (NodeTargets::Masters, Aggregate::AllSucceeded)
        }
        (b"SCRIPT", Some(b"LOAD" | b"FLUSH")) => {
            (NodeTargets::MastersAndReplicas, Aggregate::AllSucceeded)
        }
        (b"SCRIPT", Some(b"EXISTS")) => (NodeTargets::MastersAndReplicas, Aggregate::LogicalAnd),
        (b"CONFIG", Some(b"SET" | b"RESETSTAT" | b"REWRITE")) => {
            (NodeTargets::MastersAndReplicas, Aggregate::AllSucceeded)
        }
        _ => return None,
    };
    Some(FanOut { targets, aggregate })
}

fn upper(frame: &Frame) -> Option<Vec<u8>> {
    match frame {
        Frame::BulkString(Some(bytes)) | Frame::SimpleString(bytes) => {
            Some(bytes.to_ascii_uppercase())
        }
        _ => None,
    }
}

/// Combine per-node replies, given in a stable node order.
///
/// A transport error or error reply from any node fails the command with the
/// first such error. Otherwise the replies are combined per `aggregate`.
pub(crate) fn aggregate(
    aggregate: Aggregate,
    replies: impl IntoIterator<Item = (String, Result<Frame, RedisError>)>,
) -> Result<Frame, RedisError> {
    let mut frames = Vec::new();
    let mut first_error = None;
    for (node, reply) in replies {
        match reply {
            Ok(Frame::Error(error)) => {
                tracing::warn!(
                    node,
                    error = %String::from_utf8_lossy(&error),
                    "cluster: node-scoped command failed on one node"
                );
                first_error.get_or_insert(RedisError::Redis(
                    String::from_utf8_lossy(&error).into_owned(),
                ));
            }
            Ok(frame) => frames.push(frame),
            Err(error) => {
                tracing::warn!(node, %error, "cluster: node-scoped command failed on one node");
                first_error.get_or_insert(error);
            }
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    let mut frames = frames.into_iter();
    let Some(first) = frames.next() else {
        return Err(RedisError::Redis(
            "cluster: no node to run a node-scoped command on".to_string(),
        ));
    };
    match aggregate {
        Aggregate::AllSucceeded => Ok(first),
        Aggregate::Sum => {
            let mut total = integer(&first)?;
            for frame in frames {
                total = total.saturating_add(integer(&frame)?);
            }
            Ok(Frame::Integer(total))
        }
        Aggregate::LogicalAnd => {
            let (mut combined, boolean) = flags(&first)?;
            for frame in frames {
                let (next, _) = flags(&frame)?;
                if next.len() != combined.len() {
                    return Err(RedisError::UnexpectedResponse {
                        expected: "one flag per argument from every node",
                        actual: format!("{} and {} flags", combined.len(), next.len()),
                    });
                }
                for (flag, other) in combined.iter_mut().zip(next) {
                    *flag &= other;
                }
            }
            Ok(Frame::Array(Some(
                combined
                    .into_iter()
                    .map(|flag| {
                        if boolean {
                            Frame::Boolean(flag)
                        } else {
                            Frame::Integer(i64::from(flag))
                        }
                    })
                    .collect(),
            )))
        }
    }
}

fn integer(frame: &Frame) -> Result<i64, RedisError> {
    match frame {
        Frame::Integer(value) => Ok(*value),
        other => Err(RedisError::UnexpectedResponse {
            expected: "integer reply from every node",
            actual: format!("{other:?}"),
        }),
    }
}

/// The flags of an array reply, and whether they were RESP3 booleans.
fn flags(frame: &Frame) -> Result<(Vec<bool>, bool), RedisError> {
    let unexpected = || RedisError::UnexpectedResponse {
        expected: "array of 0/1 flags from every node",
        actual: format!("{frame:?}"),
    };
    let Frame::Array(Some(items)) = frame else {
        return Err(unexpected());
    };
    let mut boolean = false;
    let flags = items
        .iter()
        .map(|item| match item {
            Frame::Integer(value) => Ok(*value != 0),
            Frame::Boolean(value) => {
                boolean = true;
                Ok(*value)
            }
            _ => Err(unexpected()),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((flags, boolean))
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis_tower_protocol::helpers::{array, bulk};

    fn command(parts: &[&str]) -> Frame {
        array(parts.iter().map(|part| bulk(*part)).collect())
    }

    #[test]
    fn classifies_node_scoped_commands_case_insensitively() {
        assert_eq!(
            fan_out(&command(&["dbsize"])),
            Some(FanOut {
                targets: NodeTargets::Masters,
                aggregate: Aggregate::Sum
            })
        );
        for parts in [
            &["FLUSHALL", "ASYNC"][..],
            &["flushdb"],
            &["SAVE"],
            &["function", "load", "#!lua name=lib\n"],
        ] {
            assert_eq!(
                fan_out(&command(parts)).map(|plan| plan.targets),
                Some(NodeTargets::Masters),
                "{parts:?}"
            );
        }
        assert_eq!(
            fan_out(&command(&["SCRIPT", "EXISTS", "abc"])).map(|plan| plan.aggregate),
            Some(Aggregate::LogicalAnd)
        );
        assert_eq!(
            fan_out(&command(&["config", "set", "timeout", "0"])).map(|plan| plan.targets),
            Some(NodeTargets::MastersAndReplicas)
        );
        for parts in [
            &["INFO"][..],
            &["CONFIG", "GET", "timeout"],
            &["FUNCTION", "LIST"],
            &["SCRIPT", "KILL"],
            &["GET", "dbsize"],
            &["PING"],
        ] {
            assert_eq!(fan_out(&command(parts)), None, "{parts:?}");
        }
    }

    fn ok(node: &str, frame: Frame) -> (String, Result<Frame, RedisError>) {
        (node.to_string(), Ok(frame))
    }

    #[test]
    fn sums_integers_and_requires_every_node_to_succeed() {
        assert_eq!(
            aggregate(
                Aggregate::Sum,
                [
                    ok("a", Frame::Integer(2)),
                    ok("b", Frame::Integer(3)),
                    ok("c", Frame::Integer(0)),
                ]
            )
            .unwrap(),
            Frame::Integer(5)
        );
        let error = aggregate(
            Aggregate::AllSucceeded,
            [
                ok("a", Frame::SimpleString("OK".into())),
                ok("b", Frame::Error("ERR first".into())),
                ("c".to_string(), Err(RedisError::ConnectionClosed)),
            ],
        )
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            RedisError::Redis("ERR first".into()).to_string()
        );
        assert!(aggregate(Aggregate::AllSucceeded, []).is_err());
    }

    #[test]
    fn combines_script_exists_flags_element_wise() {
        let flags = |values: &[i64]| {
            Frame::Array(Some(values.iter().map(|v| Frame::Integer(*v)).collect()))
        };
        assert_eq!(
            aggregate(
                Aggregate::LogicalAnd,
                [ok("a", flags(&[1, 1, 0])), ok("b", flags(&[1, 0, 0]))]
            )
            .unwrap(),
            flags(&[1, 0, 0])
        );
        let booleans = Frame::Array(Some(vec![Frame::Boolean(true), Frame::Boolean(true)]));
        assert_eq!(
            aggregate(
                Aggregate::LogicalAnd,
                [
                    ok("a", booleans.clone()),
                    ok(
                        "b",
                        Frame::Array(Some(vec![Frame::Boolean(true), Frame::Boolean(false)]))
                    )
                ]
            )
            .unwrap(),
            Frame::Array(Some(vec![Frame::Boolean(true), Frame::Boolean(false)]))
        );
        assert!(
            aggregate(
                Aggregate::LogicalAnd,
                [ok("a", flags(&[1])), ok("b", flags(&[1, 1]))]
            )
            .is_err()
        );
    }
}
