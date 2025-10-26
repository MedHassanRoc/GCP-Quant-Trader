import os, subprocess, sys

# Configure once
PROJECT_ID = os.environ.get("PROJECT_ID", "leadmyroad")
BUCKET     = os.environ.get("BUCKET", f"{PROJECT_ID}-quantedge-raw")
INTERVAL   = os.environ.get("INTERVAL", "1h")
DAYS       = os.environ.get("DAYS", "30")

def run(cmd: list[str]):
    print("$", " ".join(cmd), flush=True)
    p = subprocess.run(cmd, stdout=sys.stdout, stderr=sys.stderr)
    if p.returncode != 0:
        sys.exit(p.returncode)

def main():
    # 1) Ingestion: BTCUSDT → GCS
    run([
        sys.executable, "ingest.py",
        "--bucket", BUCKET,
        "--interval", INTERVAL,
        "--days", DAYS
    ])

if __name__ == "__main__":
    main()
