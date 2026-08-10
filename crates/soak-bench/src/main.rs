use soak_bench::{Config, run};

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if std::env::args().any(|argument| matches!(argument.as_str(), "-h" | "--help")) {
        print!("{}", Config::help());
        return;
    }

    let result = match Config::from_env_and_args() {
        Ok(config) => run(config).await,
        Err(error) => Err(error),
    };
    if let Err(error) = result {
        eprintln!("soak-bench failed: {error}");
        std::process::exit(1);
    }
}
