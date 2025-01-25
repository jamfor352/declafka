use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};

/// Wait for any shutdown event (SIGINT, SIGTERM, etc.)
pub async fn wait_for_shutdown() {
    #[cfg(unix)]
    {
        let mut sigint = signal(SignalKind::interrupt()).unwrap();   // Ctrl+C
        let mut sigterm = signal(SignalKind::terminate()).unwrap();  // `kill` or system shutdown
        let mut sigquit = signal(SignalKind::quit()).unwrap();

        tokio::select! {
            _ = sigint.recv() => {
                info!("Received SIGINT (Ctrl+C), shutting down...");
            },
            _ = sigterm.recv() => {
                info!("Received SIGTERM, shutting down...");
            },
            _ = sigquit.recv() => {
                info!("Received SIGQUIT (Ctrl+\\), shutting down...");
            },
        }
    }

    #[cfg(windows)]
    {
        let _ = signal::ctrl_c().await;
        info!("Received Ctrl+C or termination signal, shutting down...");
    }
}
