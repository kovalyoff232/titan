use chrono::Utc;
use postgres::{Client, NoTls, SimpleQueryMessage};
use serde::Serialize;
use std::env;
use std::error::Error;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Target {
    Titan,
    Postgres,
    Both,
}

impl Target {
    fn from_str(raw: &str) -> Result<Self, String> {
        match raw.to_ascii_lowercase().as_str() {
            "titan" => Ok(Self::Titan),
            "postgres" => Ok(Self::Postgres),
            "both" => Ok(Self::Both),
            _ => Err(format!(
                "invalid target `{raw}`; expected one of: titan, postgres, both"
            )),
        }
    }

    fn includes_titan(self) -> bool {
        matches!(self, Self::Titan | Self::Both)
    }

    fn includes_postgres(self) -> bool {
        matches!(self, Self::Postgres | Self::Both)
    }
}

#[derive(Debug)]
struct Config {
    rows: u32,
    iterations: u32,
    warmup: u32,
    target: Target,
    postgres_dsn: Option<String>,
    out_json: Option<PathBuf>,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut rows: u32 = 1_000;
        let mut iterations: u32 = 40;
        let mut warmup: u32 = 5;
        let mut target = Target::Both;
        let mut postgres_dsn = env::var("PG_BENCH_DSN").ok();
        let mut out_json: Option<PathBuf> = None;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--rows" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "--rows requires a value".to_string())?;
                    rows = val
                        .parse::<u32>()
                        .map_err(|_| format!("invalid --rows value: {val}"))?;
                }
                "--iterations" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "--iterations requires a value".to_string())?;
                    iterations = val
                        .parse::<u32>()
                        .map_err(|_| format!("invalid --iterations value: {val}"))?;
                }
                "--warmup" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "--warmup requires a value".to_string())?;
                    warmup = val
                        .parse::<u32>()
                        .map_err(|_| format!("invalid --warmup value: {val}"))?;
                }
                "--target" => {
                    let val = args
                        .next()
                        .ok_or_else(|| "--target requires a value".to_string())?;
                    target = Target::from_str(&val)?;
                }
                "--postgres-dsn" => {
                    postgres_dsn = Some(
                        args.next()
                            .ok_or_else(|| "--postgres-dsn requires a value".to_string())?,
                    );
                }
                "--out-json" => {
                    let raw = args
                        .next()
                        .ok_or_else(|| "--out-json requires a value".to_string())?;
                    out_json = Some(PathBuf::from(raw));
                }
                "--help" | "-h" => {
                    return Err(Self::usage());
                }
                other => {
                    return Err(format!("unknown argument `{other}`\n\n{}", Self::usage()));
                }
            }
        }

        if rows == 0 {
            return Err("--rows must be > 0".to_string());
        }
        if iterations == 0 {
            return Err("--iterations must be > 0".to_string());
        }

        Ok(Self {
            rows,
            iterations,
            warmup,
            target,
            postgres_dsn,
            out_json,
        })
    }

    fn usage() -> String {
        [
            "Usage:",
            "  cargo run -p titan_bin --bin baseline_runner -- [options]",
            "",
            "Options:",
            "  --rows <n>            Number of rows to seed (default: 1000)",
            "  --iterations <n>      Timed iterations per query (default: 40)",
            "  --warmup <n>          Warmup iterations per query (default: 5)",
            "  --target <engine>     titan | postgres | both (default: both)",
            "  --postgres-dsn <dsn>  DSN for PostgreSQL (or use PG_BENCH_DSN)",
            "  --out-json <path>     Output JSON path (default: target/benchmarks/baseline-<ts>.json)",
        ]
        .join("\n")
    }
}

#[derive(Debug, Serialize)]
struct BaselineReport {
    generated_at_utc: String,
    rows: u32,
    iterations: u32,
    warmup: u32,
    results: Vec<EngineResult>,
}

#[derive(Debug, Serialize)]
struct EngineResult {
    engine: String,
    status: String,
    notes: Vec<String>,
    metrics: Vec<QueryMetric>,
}

#[derive(Debug, Serialize)]
struct QueryMetric {
    name: String,
    sql: String,
    iterations: u32,
    min_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    max_ms: f64,
    mean_ms: f64,
}

#[derive(Debug)]
struct TitanServerHandle {
    child: Child,
    _temp_dir: TempDir,
    dsn: String,
}

impl Drop for TitanServerHandle {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[derive(Debug)]
struct RunnerError(String);

impl fmt::Display for RunnerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for RunnerError {}

fn query_count(client: &mut Client) -> Result<i64, Box<dyn Error>> {
    let messages = client.simple_query("SELECT id FROM bench_t;")?;
    let rows = messages
        .into_iter()
        .filter(|msg| matches!(msg, SimpleQueryMessage::Row(_)))
        .count() as i64;
    Ok(rows)
}

fn setup_dataset(client: &mut Client, rows: u32) -> Result<(), Box<dyn Error>> {
    let _ = client.simple_query("CREATE TABLE bench_t (id INT, name TEXT, v INT);");
    client.simple_query("DELETE FROM bench_t;")?;
    client.simple_query("BEGIN;")?;
    for i in 0..rows {
        let sql = format!("INSERT INTO bench_t VALUES ({i}, 'name{i}', {});", i * 3);
        client.simple_query(&sql)?;
    }
    client.simple_query("COMMIT;")?;
    Ok(())
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let max_index = sorted.len() - 1;
    let idx = ((max_index as f64) * p).round() as usize;
    sorted[idx.min(max_index)]
}

fn summarize_query_metric(
    name: &str,
    sql: &str,
    samples_ms: &[f64],
    iterations: u32,
) -> QueryMetric {
    let mut sorted = samples_ms.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let sum: f64 = sorted.iter().sum();
    let mean = if sorted.is_empty() {
        0.0
    } else {
        sum / sorted.len() as f64
    };
    QueryMetric {
        name: name.to_string(),
        sql: sql.to_string(),
        iterations,
        min_ms: *sorted.first().unwrap_or(&0.0),
        p50_ms: percentile(&sorted, 0.50),
        p95_ms: percentile(&sorted, 0.95),
        max_ms: *sorted.last().unwrap_or(&0.0),
        mean_ms: mean,
    }
}

fn run_workload(
    engine: &str,
    client: &mut Client,
    rows: u32,
    iterations: u32,
    warmup: u32,
) -> Result<EngineResult, Box<dyn Error>> {
    setup_dataset(client, rows)?;
    let row_count = query_count(client)?;
    let mut notes = vec![format!("seeded_rows={row_count}")];
    let mut metrics = Vec::new();

    let point_lookup_id = rows / 2;
    let range_cutoff = (rows * 3) / 4;

    let queries: Vec<(&str, String)> = vec![
        (
            "point_lookup",
            format!("SELECT id, name, v FROM bench_t WHERE id = {point_lookup_id};"),
        ),
        (
            "range_scan",
            format!("SELECT id, v FROM bench_t WHERE id < {range_cutoff};"),
        ),
        (
            "ordered_scan",
            "SELECT id, v FROM bench_t ORDER BY v;".to_string(),
        ),
        ("full_scan", "SELECT id FROM bench_t;".to_string()),
    ];

    for (name, sql) in queries {
        for _ in 0..warmup {
            client.simple_query(&sql)?;
        }

        let mut samples = Vec::with_capacity(iterations as usize);
        for _ in 0..iterations {
            let start = Instant::now();
            client.simple_query(&sql)?;
            let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;
            samples.push(elapsed_ms);
        }
        metrics.push(summarize_query_metric(name, &sql, &samples, iterations));
    }

    notes.push("workload=read_focused_smoke".to_string());
    Ok(EngineResult {
        engine: engine.to_string(),
        status: "ok".to_string(),
        notes,
        metrics,
    })
}

fn pick_free_port() -> Result<u16, Box<dyn Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

fn find_titan_server_binary() -> Result<PathBuf, Box<dyn Error>> {
    let exe = env::current_exe()?;
    let dir = exe.parent().ok_or_else(|| {
        Box::new(RunnerError(
            "failed to resolve current executable parent".to_string(),
        )) as Box<dyn Error>
    })?;
    let name = if cfg!(windows) {
        "titan_bin.exe"
    } else {
        "titan_bin"
    };
    let candidate = dir.join(name);
    if candidate.exists() {
        return Ok(candidate);
    }
    Err(Box::new(RunnerError(format!(
        "titan server binary not found at `{}`; build it first with `cargo build --bin titan_bin`",
        candidate.display()
    ))))
}

fn connect_with_retry(dsn: &str, timeout: Duration) -> Result<Client, Box<dyn Error>> {
    let deadline = Instant::now() + timeout;
    loop {
        match Client::connect(dsn, NoTls) {
            Ok(client) => return Ok(client),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(Box::new(RunnerError(format!(
                        "failed to connect to `{dsn}` within {:?}: {err}",
                        timeout
                    ))));
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

fn start_titan_server() -> Result<TitanServerHandle, Box<dyn Error>> {
    let titan_bin = find_titan_server_binary()?;
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("baseline.db");
    let wal_path = temp_dir.path().join("baseline.wal");
    let port = pick_free_port()?;
    let addr = format!("127.0.0.1:{port}");
    let dsn = format!("host=localhost port={port} user=postgres");

    let child = Command::new(titan_bin)
        .env("TITAN_DB_PATH", db_path)
        .env("TITAN_WAL_PATH", wal_path)
        .env("TITAN_ADDR", &addr)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()?;

    Ok(TitanServerHandle {
        child,
        _temp_dir: temp_dir,
        dsn,
    })
}

fn default_out_json_path() -> PathBuf {
    let timestamp = Utc::now().format("%Y%m%d-%H%M%S");
    PathBuf::from("target")
        .join("benchmarks")
        .join(format!("baseline-{timestamp}.json"))
}

fn write_report_files(report: &BaselineReport, out_json: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if let Some(parent) = out_json.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(report)?;
    fs::write(out_json, json)?;

    let out_csv = out_json.with_extension("csv");
    let mut csv = String::from("engine,query,iterations,min_ms,p50_ms,p95_ms,max_ms,mean_ms\n");
    for engine in &report.results {
        for metric in &engine.metrics {
            csv.push_str(&format!(
                "{},{},{},{:.6},{:.6},{:.6},{:.6},{:.6}\n",
                engine.engine,
                metric.name,
                metric.iterations,
                metric.min_ms,
                metric.p50_ms,
                metric.p95_ms,
                metric.max_ms,
                metric.mean_ms
            ));
        }
    }
    fs::write(&out_csv, csv)?;
    Ok(out_csv)
}

fn run() -> Result<(), Box<dyn Error>> {
    let cfg = Config::parse().map_err(|msg| Box::new(RunnerError(msg)) as Box<dyn Error>)?;
    let mut results = Vec::new();

    if cfg.target.includes_titan() {
        println!("[baseline] running Titan benchmark...");
        match start_titan_server().and_then(|server| {
            let mut client = connect_with_retry(&server.dsn, Duration::from_secs(8))?;
            run_workload("titan", &mut client, cfg.rows, cfg.iterations, cfg.warmup)
        }) {
            Ok(result) => results.push(result),
            Err(err) => results.push(EngineResult {
                engine: "titan".to_string(),
                status: "error".to_string(),
                notes: vec![err.to_string()],
                metrics: vec![],
            }),
        }
    }

    if cfg.target.includes_postgres() {
        println!("[baseline] running PostgreSQL benchmark...");
        match cfg.postgres_dsn.as_deref() {
            Some(dsn) => {
                match connect_with_retry(dsn, Duration::from_secs(8)).and_then(|mut client| {
                    run_workload(
                        "postgres",
                        &mut client,
                        cfg.rows,
                        cfg.iterations,
                        cfg.warmup,
                    )
                }) {
                    Ok(result) => results.push(result),
                    Err(err) => results.push(EngineResult {
                        engine: "postgres".to_string(),
                        status: "error".to_string(),
                        notes: vec![err.to_string()],
                        metrics: vec![],
                    }),
                }
            }
            None => results.push(EngineResult {
                engine: "postgres".to_string(),
                status: "skipped".to_string(),
                notes: vec!["PG_BENCH_DSN not provided".to_string()],
                metrics: vec![],
            }),
        }
    }

    let report = BaselineReport {
        generated_at_utc: Utc::now().to_rfc3339(),
        rows: cfg.rows,
        iterations: cfg.iterations,
        warmup: cfg.warmup,
        results,
    };

    let out_json = cfg.out_json.unwrap_or_else(default_out_json_path);
    let out_csv = write_report_files(&report, &out_json)?;
    println!("[baseline] JSON report: {}", out_json.display());
    println!("[baseline] CSV report:  {}", out_csv.display());

    let failed_engines: Vec<&str> = report
        .results
        .iter()
        .filter(|r| r.status == "error")
        .map(|r| r.engine.as_str())
        .collect();
    if !failed_engines.is_empty() {
        return Err(Box::new(RunnerError(format!(
            "benchmark failed for engine(s): {}",
            failed_engines.join(", ")
        ))));
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("[baseline] error: {err}");
        std::process::exit(1);
    }
}
