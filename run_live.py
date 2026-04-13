"""Launch the full marathon analytics pipeline as a single command.

Starts the consumer first, then the dashboard, then the producer, with brief
delays between launches so each process is ready before the next one depends
on it. Press Ctrl+C to stop all three at once.

This script does not change how the pipeline works. It is a convenience
wrapper around the three commands you would otherwise run in three separate
terminals:

    python consumer.py
    streamlit run dashboard.py
    python producer.py

Usage:
    python run_live.py
"""
import os
import signal
import subprocess
import sys
import time

# Use the interpreter running this script so child processes inherit the venv
PYTHON = sys.executable

# Delay between launching each child process, in seconds
CONSUMER_STARTUP_DELAY = 3
DASHBOARD_STARTUP_DELAY = 2

# Subprocess handles, populated by launch()
processes: list[tuple[str, subprocess.Popen]] = []


def launch(name: str, command: list[str]) -> subprocess.Popen:
    """Start a child process and register it for shutdown."""
    print(f"starting {name}: {' '.join(command)}")
    process = subprocess.Popen(
        command,
        preexec_fn=os.setsid if os.name != "nt" else None,
    )
    processes.append((name, process))
    return process


def shutdown(*_):
    """Terminate every registered child process and exit."""
    print("\nstopping all processes")
    for name, process in processes:
        if process.poll() is None:
            try:
                if os.name == "nt":
                    process.terminate()
                else:
                    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass

    for name, process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()

    print("done")
    sys.exit(0)


def main():
    """Launch the pipeline and block until interrupted or a child exits."""
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    launch("consumer", [PYTHON, "consumer.py"])
    time.sleep(CONSUMER_STARTUP_DELAY)

    launch(
        "dashboard",
        [
            PYTHON, "-m", "streamlit", "run", "dashboard.py",
            "--server.headless", "true",
        ],
    )
    time.sleep(DASHBOARD_STARTUP_DELAY)

    launch("producer", [PYTHON, "producer.py"])

    print("\npipeline running. dashboard: http://localhost:8501")
    print("press Ctrl+C to stop.\n")

    # Keep the consumer and dashboard alive after the producer finishes so the
    # final state can still be inspected. The producer exiting on its own is
    # expected; any other child exiting is treated as a failure.
    while True:
        time.sleep(1)
        for name, process in processes:
            if process.poll() is not None and name != "producer":
                print(f"{name} exited unexpectedly with code {process.returncode}")
                shutdown()


if __name__ == "__main__":
    main()