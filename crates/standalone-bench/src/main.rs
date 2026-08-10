//! Standalone Redis comparison across redis-tower, redis-rs, and fred.
//!
//! The default matrix sweeps 64 B, 1 KiB, and 16 KiB values across GET, SET,
//! and explicit pipeline workloads. Use the environment variables documented
//! in `crates/standalone-bench/README.md`, or their CLI equivalents, to select
//! smaller smoke matrices. Publication tooling can pass `--include-samples` to
//! retain the bounded per-run inputs behind each aggregate JSON cell.

mod clients;
mod runner;

use std::time::Duration;

use redis_server_wrapper::RedisServer;

use crate::clients::{Client, ClientKind};
use crate::runner::{AggregatedReport, BenchConfig, BenchReport, Workload, aggregate};

const THROUGHPUT_SCHEMA_VERSION: u64 = 2;

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let json = std::env::args().any(|argument| argument == "--json");
    if let Err(error) = run(json).await {
        eprintln!("standalone benchmark failed: {error}");
        std::process::exit(1);
    }
    // Blocking comparison clients can retain runtime resources after output.
    std::process::exit(0);
}

struct MatrixConfig {
    duration: Duration,
    warmup: Duration,
    runs: usize,
    concurrencies: Vec<usize>,
    pipeline_concurrency: Vec<usize>,
    payload_sizes: Vec<usize>,
    pipeline_commands: usize,
    clients: Vec<ClientKind>,
    workloads: Vec<Workload>,
    include_samples: bool,
}

impl MatrixConfig {
    fn from_env() -> Result<Self, String> {
        Ok(Self {
            duration: Duration::from_secs(env_or_arg_parse("BENCH_SECS", "--secs", 10_u64)?),
            warmup: Duration::from_secs(env_or_arg_parse("BENCH_WARMUP", "--warmup", 2_u64)?),
            runs: env_or_arg_parse("BENCH_RUNS", "--runs", 3_usize)?.max(1),
            concurrencies: parse_positive_list(
                &env_or_arg("BENCH_CONCURRENCY", "--concurrency", "1,8,32,128"),
                "concurrency",
            )?,
            pipeline_concurrency: parse_positive_list(
                &env_or_arg("BENCH_PIPELINE_CONCURRENCY", "--pipeline-concurrency", "1"),
                "pipeline concurrency",
            )?,
            payload_sizes: parse_size_list(&env_or_arg(
                "BENCH_PAYLOAD_SIZES",
                "--payload-sizes",
                "64,1024,16384",
            ))?,
            pipeline_commands: env_or_arg_parse(
                "BENCH_PIPELINE_COMMANDS",
                "--pipeline-commands",
                100_usize,
            )?
            .max(1),
            clients: parse_clients(env_or_arg_optional("BENCH_CLIENTS", "--clients").as_deref())?,
            workloads: parse_workloads(&env_or_arg(
                "BENCH_WORKLOADS",
                "--workloads",
                "set,get,pipeline",
            ))?,
            include_samples: flag_or_env("BENCH_INCLUDE_SAMPLES", "--include-samples")?,
        })
    }
}

async fn run(json: bool) -> Result<(), String> {
    let config = MatrixConfig::from_env()?;
    let port = env_or_arg_parse("BENCH_PORT", "--port", 6480_u16)?;
    eprintln!("starting redis server on port {port}");
    let server = RedisServer::new()
        .port(port)
        .start()
        .await
        .map_err(|error| format!("failed to start redis server: {error}"))?;
    let addr = server.addr();
    eprintln!("server ready at {addr}");

    let mut reports = Vec::<AggregatedReport>::new();
    for &payload_bytes in &config.payload_sizes {
        let payload = "x".repeat(payload_bytes);
        eprintln!("pre-populating 1024 keys with {payload_bytes}-byte values...");
        clients::prepopulate(&addr, &payload).await?;

        for &workload in &config.workloads {
            let concurrencies = if matches!(workload, Workload::Pipeline) {
                &config.pipeline_concurrency
            } else {
                &config.concurrencies
            };
            for &concurrency in concurrencies {
                for &kind in &config.clients {
                    let bench = BenchConfig {
                        duration: config.duration,
                        warmup: config.warmup,
                        concurrency,
                        workload,
                        payload_bytes,
                        pipeline_commands: config.pipeline_commands,
                    };
                    let mut cell = Vec::<BenchReport>::with_capacity(config.runs);
                    for run_index in 0..config.runs {
                        eprintln!(
                            "running {} workload={workload:?} payload={payload_bytes} concurrency={concurrency} run={}/{}",
                            kind.as_str(),
                            run_index + 1,
                            config.runs
                        );
                        let client = Client::connect(kind, &addr).await.map_err(|error| {
                            format!("{} connect failed: {error}", kind.as_str())
                        })?;
                        cell.push(runner::run(client, bench).await.map_err(|error| {
                            format!("{} worker failed: {error}", kind.as_str())
                        })?);
                    }
                    reports.push(aggregate(&cell));
                }
            }
        }
    }

    drop(server);
    if json {
        println!("{}", to_json(&reports, config.include_samples));
    } else {
        println!();
        print_table(&reports);
    }
    Ok(())
}

fn print_table(reports: &[AggregatedReport]) {
    println!(
        "{:<20} {:<8} {:>7} {:>5} {:>5} {:>6} {:>11} {:>12} {:>12} {:>9} {:>9} {:>9} {:>7}",
        "client",
        "work",
        "bytes",
        "conc",
        "runs",
        "cmd/b",
        "batches/s",
        "commands/s",
        "cmd/s sd",
        "p50 (us)",
        "p99 (us)",
        "p999 (us)",
        "errors",
    );
    println!("{}", "-".repeat(143));
    for report in reports {
        println!(
            "{:<20} {:<8} {:>7} {:>5} {:>5} {:>6} {:>11} {:>12} {:>12} {:>9} {:>9} {:>9} {:>7}",
            report.client.as_str(),
            format!("{:?}", report.workload),
            report.payload_bytes,
            report.concurrency,
            report.runs,
            report.commands_per_batch,
            format!("{:.0}", report.batches_per_sec_mean),
            format!("{:.0}", report.commands_per_sec_mean),
            format!("{:.0}", report.commands_per_sec_stddev),
            format!("{:.0}", report.p50_us),
            format!("{:.0}", report.p99_us),
            format!("{:.0}", report.p999_us),
            report.errors,
        );
    }
    println!("pipeline latency is measured per batch; GET/SET latency is per command");
}

fn to_json(reports: &[AggregatedReport], include_samples: bool) -> String {
    let reports = reports
        .iter()
        .map(|report| {
            let mut value = serde_json::json!({
                "schema_version": THROUGHPUT_SCHEMA_VERSION,
                "client": format!("{:?}", report.client),
                "client_id": report.client.as_str(),
                "workload": format!("{:?}", report.workload),
                "payload_bytes": report.payload_bytes,
                "concurrency": report.concurrency,
                "runs": report.runs,
                "commands_per_batch": report.commands_per_batch,
                // Compatibility aliases retain v1's per-iteration semantics.
                "total_ops": report.total_batches,
                "ops_per_sec_mean": report.batches_per_sec_mean,
                "ops_per_sec_stddev": report.batches_per_sec_stddev,
                "total_batches": report.total_batches,
                "total_commands": report.total_commands,
                "errors": report.errors,
                "batches_per_sec_mean": report.batches_per_sec_mean,
                "batches_per_sec_stddev": report.batches_per_sec_stddev,
                "commands_per_sec_mean": report.commands_per_sec_mean,
                "commands_per_sec_stddev": report.commands_per_sec_stddev,
                "latency_unit": if matches!(report.workload, Workload::Pipeline) { "batch" } else { "command" },
                "p50_us": report.p50_us,
                "p90_us": report.p90_us,
                "p99_us": report.p99_us,
                "p999_us": report.p999_us,
                "max_us": report.max_us,
            });
            if include_samples {
                value["samples"] = serde_json::Value::Array(
                    report
                        .samples
                        .iter()
                        .enumerate()
                        .map(|(index, sample)| {
                            serde_json::json!({
                                "run": index + 1,
                                "total_batches": sample.total_batches,
                                "total_commands": sample.total_commands,
                                "errors": sample.errors,
                                "batches_per_sec": sample.batches_per_sec,
                                "commands_per_sec": sample.commands_per_sec,
                                "p50_us": sample.p50_us,
                                "p90_us": sample.p90_us,
                                "p99_us": sample.p99_us,
                                "p999_us": sample.p999_us,
                                "max_us": sample.max_us,
                            })
                        })
                        .collect(),
                );
            }
            value
        })
        .collect::<Vec<_>>();
    serde_json::to_string_pretty(&serde_json::Value::Array(reports))
        .unwrap_or_else(|_| "[]".to_owned())
}

fn arg_value(name: &str) -> Option<String> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let prefix = format!("{name}=");
    for (index, value) in args.iter().enumerate() {
        if let Some(value) = value.strip_prefix(&prefix) {
            return Some(value.to_owned());
        }
        if value == name {
            return args.get(index + 1).cloned();
        }
    }
    None
}

fn env_or_arg_optional(env: &str, arg: &str) -> Option<String> {
    arg_value(arg).or_else(|| std::env::var(env).ok())
}

fn env_or_arg(env: &str, arg: &str, default: &str) -> String {
    env_or_arg_optional(env, arg).unwrap_or_else(|| default.to_owned())
}

fn env_or_arg_parse<T>(env: &str, arg: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr + Copy,
{
    let Some(value) = env_or_arg_optional(env, arg) else {
        return Ok(default);
    };
    value
        .parse()
        .map_err(|_| format!("invalid value {value:?} for {arg}/{env}"))
}

fn flag_or_env(env: &str, arg: &str) -> Result<bool, String> {
    if std::env::args().any(|value| value == arg) {
        return Ok(true);
    }
    match std::env::var(env) {
        Ok(value) => match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" => Ok(true),
            "0" | "false" | "no" => Ok(false),
            _ => Err(format!(
                "invalid value {value:?} for {env}; expected true or false"
            )),
        },
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(format!("could not read {env}: {error}")),
    }
}

fn parse_positive_list(value: &str, name: &str) -> Result<Vec<usize>, String> {
    let values = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<usize>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or_else(|| format!("invalid {name} value {value:?}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err(format!("{name} list must not be empty"));
    }
    Ok(values)
}

fn parse_size_list(value: &str) -> Result<Vec<usize>, String> {
    let values = value
        .split(',')
        .map(parse_size)
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Err("payload size list must not be empty".into());
    }
    Ok(values)
}

fn parse_size(value: &str) -> Result<usize, String> {
    let normalized = value.trim().to_ascii_lowercase();
    let (number, multiplier) = if let Some(number) = normalized.strip_suffix("kib") {
        (number, 1024usize)
    } else if let Some(number) = normalized.strip_suffix("kb") {
        (number, 1024)
    } else if let Some(number) = normalized.strip_suffix('k') {
        (number, 1024)
    } else if let Some(number) = normalized.strip_suffix("mib") {
        (number, 1024 * 1024)
    } else if let Some(number) = normalized.strip_suffix("mb") {
        (number, 1024 * 1024)
    } else if let Some(number) = normalized.strip_suffix('m') {
        (number, 1024 * 1024)
    } else if let Some(number) = normalized.strip_suffix('b') {
        (number, 1)
    } else {
        (normalized.as_str(), 1)
    };
    let number = number
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("invalid payload size {value:?}"))?;
    number
        .checked_mul(multiplier)
        .filter(|size| *size > 0)
        .ok_or_else(|| format!("invalid payload size {value:?}"))
}

fn parse_clients(value: Option<&str>) -> Result<Vec<ClientKind>, String> {
    let Some(value) = value else {
        return Ok(ClientKind::DEFAULTS.to_vec());
    };
    let clients = value
        .split(',')
        .map(|value| {
            ClientKind::parse(value).ok_or_else(|| format!("unknown benchmark client {value:?}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if clients.is_empty() {
        return Err("client list must not be empty".into());
    }
    Ok(clients)
}

fn parse_workloads(value: &str) -> Result<Vec<Workload>, String> {
    let workloads = value
        .split(',')
        .map(|value| match value.trim().to_ascii_lowercase().as_str() {
            "set" => Ok(Workload::Set),
            "get" => Ok(Workload::Get),
            "pipeline" | "pipe" => Ok(Workload::Pipeline),
            _ => Err(format!("unknown workload {value:?}")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if workloads.is_empty() {
        return Err("workload list must not be empty".into());
    }
    Ok(workloads)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_binary_size_suffixes() {
        assert_eq!(parse_size("64B").unwrap(), 64);
        assert_eq!(parse_size("1K").unwrap(), 1024);
        assert_eq!(parse_size("16KiB").unwrap(), 16 * 1024);
        assert!(parse_size("0").is_err());
    }

    #[test]
    fn rejects_unknown_workloads() {
        assert!(parse_workloads("get,wat").is_err());
    }

    #[test]
    fn throughput_json_retains_v1_batch_aliases_and_adds_stable_ids() {
        let report = AggregatedReport {
            client: ClientKind::RedisTowerMux,
            workload: Workload::Pipeline,
            concurrency: 1,
            payload_bytes: 64,
            commands_per_batch: 100,
            runs: 3,
            total_batches: 7,
            total_commands: 700,
            errors: 0,
            batches_per_sec_mean: 10.0,
            batches_per_sec_stddev: 1.0,
            commands_per_sec_mean: 1000.0,
            commands_per_sec_stddev: 100.0,
            p50_us: 10.0,
            p90_us: 20.0,
            p99_us: 30.0,
            p999_us: 40.0,
            max_us: 50.0,
            samples: vec![BenchReport {
                client: ClientKind::RedisTowerMux,
                workload: Workload::Pipeline,
                concurrency: 1,
                payload_bytes: 64,
                commands_per_batch: 100,
                total_batches: 7,
                total_commands: 700,
                errors: 0,
                batches_per_sec: 10.0,
                commands_per_sec: 1000.0,
                p50_us: 10.0,
                p90_us: 20.0,
                p99_us: 30.0,
                p999_us: 40.0,
                max_us: 50.0,
            }],
        };
        let value: serde_json::Value = serde_json::from_str(&to_json(&[report], false)).unwrap();
        let record = &value[0];
        assert_eq!(record["schema_version"], 2);
        assert_eq!(record["client"], "RedisTowerMux");
        assert_eq!(record["client_id"], "redis-tower-mux");
        assert_eq!(record["total_ops"], record["total_batches"]);
        assert_eq!(record["ops_per_sec_mean"], record["batches_per_sec_mean"]);
        assert_ne!(record["ops_per_sec_mean"], record["commands_per_sec_mean"]);
        assert!(record.get("samples").is_none());
    }

    #[test]
    fn publication_json_can_retain_raw_run_samples() {
        let report = aggregate(&[
            BenchReport {
                client: ClientKind::RedisTowerMux,
                workload: Workload::Get,
                concurrency: 1,
                payload_bytes: 16,
                commands_per_batch: 1,
                total_batches: 10,
                total_commands: 10,
                errors: 0,
                batches_per_sec: 100.0,
                commands_per_sec: 100.0,
                p50_us: 10.0,
                p90_us: 20.0,
                p99_us: 30.0,
                p999_us: 40.0,
                max_us: 50.0,
            },
            BenchReport {
                client: ClientKind::RedisTowerMux,
                workload: Workload::Get,
                concurrency: 1,
                payload_bytes: 16,
                commands_per_batch: 1,
                total_batches: 20,
                total_commands: 20,
                errors: 0,
                batches_per_sec: 200.0,
                commands_per_sec: 200.0,
                p50_us: 11.0,
                p90_us: 21.0,
                p99_us: 31.0,
                p999_us: 41.0,
                max_us: 51.0,
            },
        ]);
        let value: serde_json::Value = serde_json::from_str(&to_json(&[report], true)).unwrap();
        assert_eq!(value[0]["samples"].as_array().unwrap().len(), 2);
        assert_eq!(value[0]["samples"][1]["commands_per_sec"], 200.0);
    }
}
