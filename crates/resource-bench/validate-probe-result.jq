def positive_integer: type == "number" and . > 0 and floor == .;
def close_to($expected): type == "number" and (. - $expected | fabs) < 0.000001;
def expected_policy:
  . as $r
  | ($r.config.profile == "matched-mux-resp2-v1") as $matched
  | {client_path: ({"redis-tower":"redis-tower-direct", "redis-tower-mux":"redis-tower-multiplexed", "redis-rs":"redis-rs-multiplexed", "fred":"fred"}[$r.client]),
     transport: $r.policy.transport,
     protocol_selection: (if $matched then "resp2" else "client-default-or-url" end),
     socket_policy: (if $r.policy.transport == "unix" then "not-applicable-unix" elif $matched then "matched-v1" elif ($r.client | startswith("redis-tower")) then "tower-default" elif $r.client == "redis-rs" then "redis-rs-default" else "fred-default-uninspected" end),
     tcp_nodelay: (if $r.policy.transport == "unix" then null elif $matched or ($r.client | startswith("redis-tower")) then true elif $r.client == "redis-rs" then false else null end),
     keepalive: (if $r.policy.transport != "unix" and ($matched or ($r.client | startswith("redis-tower"))) then {idle_secs:60, interval_secs:10, probes:(if $r.os == "windows" then null else 3 end)} else null end),
     physical_connections:$r.config.connections, inflight_per_socket:1,
     runtime_workers:$r.config.runtime_workers,
     measurement_model:"completion-gated-staggered-get"};

(.schema_version == 4)
and (.client as $client | ["redis-tower", "redis-tower-mux", "redis-rs", "fred"] | index($client) != null)
and (.config.profile == "baseline" or (.config.profile == "matched-mux-resp2-v1" and (.client == "redis-tower-mux" or .client == "redis-rs")))
and (.config.runtime_workers | positive_integer)
and (.config.runtime_workers <= 256)
and (.policy.transport as $transport | ["tcp", "tls", "unix"] | index($transport) != null)
and (.config.profile != "matched-mux-resp2-v1" or .policy.transport == "tcp")
and (.policy == expected_policy)
and (.client_features == {
  harness_feature: (if .client == "redis-rs" then "client-redis-rs" elif .client == "fred" then "client-fred" else "client-redis-tower" end),
  dependency_default_features:false,
  dependency_features: (if .client == "redis-rs" then ["tokio-comp"] elif .client == "fred" then ["i-keys"] else [] end)})
and ((.client | type) == "string")
and ((.client | length) > 0)
and ((.client_features.harness_feature | type) == "string")
and (.client_features.dependency_default_features == false)
and ((.client_features.dependency_features | type) == "array")
and ((.config | has("redis_url")) | not)
and (.config.redis_endpoint == "<redacted>")
and (.config.connections > 0)
and (.config.target_ops_per_sec > 0)
and (.config.duration_secs > 0)
and (.config.drain_timeout_ms > 0)
and (.config.payload_bytes > 0)
and (.cpu.errors == 0)
and (.cpu.cutoff_ops == 0)
and (.cpu.successful_ops > 0)
and (.cpu.attempted_ops == (.cpu.successful_ops + .cpu.errors + .cpu.cutoff_ops))
and (.cpu.launch_window_seconds > 0)
and (.cpu.drain_seconds >= 0)
and (.cpu.wall_seconds > 0)
and (.cpu.process_cpu_seconds >= 0)
and (.cpu.process_cpu_percent >= 0)
and (.cpu.wall_seconds >= .cpu.launch_window_seconds)
and (. as $r | $r.cpu.drain_seconds | close_to($r.cpu.wall_seconds - $r.cpu.launch_window_seconds))
and (.rss.baseline_peak_bytes >= 0)
and (.rss.connected_peak_bytes >= 0)
and (.rss.post_workload_peak_bytes >= 0)
and (.config.connections | positive_integer)
and (.config.target_ops_per_sec | positive_integer)
and (.config.duration_secs | positive_integer)
and (.config.payload_bytes | positive_integer)
and (.config.drain_timeout_ms | positive_integer)
and (.config.warmup_secs | type == "number" and . >= 0 and floor == .)
and (.config.drain_timeout_ms <= 60000)
and (.config.target_ops_per_sec <= 1000000000)
and (.cpu.attempted_ops | positive_integer)
and (.cpu.successful_ops | positive_integer)
and (.cpu.attempted_ops <= .config.target_ops_per_sec * .config.duration_secs)
and (.cpu.launch_window_seconds == .config.duration_secs)
and (.cpu.target_ops_per_sec == .config.target_ops_per_sec)
and (. as $r | $r.cpu.attempted_ops_per_sec | close_to($r.cpu.attempted_ops / $r.cpu.launch_window_seconds))
and (. as $r | $r.cpu.achieved_ops_per_sec | close_to($r.cpu.successful_ops / $r.cpu.wall_seconds))
and (. as $r | $r.cpu.process_cpu_percent | close_to(100 * $r.cpu.process_cpu_seconds / $r.cpu.wall_seconds))
and (. as $r | $r.rss.connection_delta_bytes == ([$r.rss.connected_peak_bytes - $r.rss.baseline_peak_bytes, 0] | max))
and (. as $r | $r.rss.bytes_per_connection | close_to($r.rss.connection_delta_bytes / $r.config.connections))
