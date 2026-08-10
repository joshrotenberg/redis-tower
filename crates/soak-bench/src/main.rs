use soak_bench::{Config, run};
use std::process::ExitCode;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    if std::env::args().any(|argument| matches!(argument.as_str(), "-h" | "--help")) {
        print!("{}", Config::help());
        return ExitCode::SUCCESS;
    }

    enum Outcome {
        Finished(soak_bench::RunResult<()>),
        Signaled(std::io::Result<()>),
    }

    let outcome = match Config::from_env_and_args() {
        Ok(config) => tokio::select! {
            result = run(config) => Outcome::Finished(result),
            signal = shutdown_signal() => Outcome::Signaled(signal),
        },
        Err(error) => Outcome::Finished(Err(error)),
    };
    let result = match outcome {
        Outcome::Finished(result) => result,
        Outcome::Signaled(Ok(())) => {
            // Returning lets Tokio finish dropping every abort-owned task and
            // process guard before the operating system observes our exit.
            eprintln!("soak-bench: received SIGINT/SIGTERM; cleaning managed processes");
            return ExitCode::from(130);
        }
        Outcome::Signaled(Err(error)) => Err(error.into()),
    };
    if let Err(error) = result {
        eprintln!("soak-bench failed: {error}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

#[cfg(unix)]
async fn shutdown_signal() -> std::io::Result<()> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = interrupt.recv() => Ok(()),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() -> std::io::Result<()> {
    tokio::signal::ctrl_c().await
}
