#!/usr/bin/env python3
"""Exercise the example in a disposable copy and retain commands and responses."""

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from urllib.error import HTTPError
from urllib.request import urlopen

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--fed", default=os.environ.get("FED", "fed"), help="fed binary to test")
args = parser.parse_args()
source = Path(__file__).resolve().parent
executable = shutil.which(args.fed)
if not executable:
    parser.error(f"Cannot find fed: {args.fed}. Build with cargo build and pass --fed ../../target/debug/fed.")
root = Path(tempfile.mkdtemp(prefix="fed-variant-stack-"))
work = root / "project"
shutil.copytree(source, work, ignore=shutil.ignore_patterns(".fed", "__pycache__"))
# Freeze the executable: rebuilding the repo during a run must not replace it.
binary = root / "fed"
shutil.copy2(executable, binary)
evidence = []
print(f"Evidence and scratch project: {root}", flush=True)


def record(entry):
    evidence.append(entry)
    (root / "evidence.json").write_text(json.dumps(evidence, indent=2))


def run(*arguments, success=True):
    result = subprocess.run([str(binary), *arguments], cwd=work, text=True,
                            capture_output=True, timeout=45)
    record({"command": list(arguments), "code": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr})
    if success:
        assert result.returncode == 0, (arguments, result.stdout, result.stderr)
    else:
        assert result.returncode != 0, f"Unexpected success: {arguments}"
    return result.stdout


def status():
    return json.loads(run("status", "--json"))


def get(port, path="/catalog", expected_status=200):
    url = f"http://127.0.0.1:{port}{path}"
    try:
        response = urlopen(url, timeout=3)
    except HTTPError as error:
        response = error
    with response:
        body = response.read().decode()
        record({"url": url, "status": response.status, "body": body})
        assert response.status == expected_status, body
    return body


def wait_for(check, label):
    deadline = time.monotonic() + 12
    while True:
        if check():
            print(f"PASS {label}", flush=True)
            return
        assert time.monotonic() < deadline, label
        time.sleep(0.1)


try:
    summary = run("validate")
    assert "stack (oneshot)" in summary, summary
    run("start")  # wildcard entrypoint
    initial = status()
    assert "console" not in initial or initial["console"]["pid"] is None
    assert initial["stack"]["service_type"] == "oneshot"
    assert initial["catalog"]["variant"] == "live"
    assert all(initial[name]["status"] == "healthy" for name in ("storage", "catalog", "frontend"))
    ports = json.loads(run("ports", "list", "--json"))
    frontend_port = ports["FRONTEND_PORT"]
    catalog_port = ports["CATALOG_PORT"]
    data = json.loads(get(frontend_port))
    assert data["implementation"] == "live" and data["items"][0]["name"] == "Notebook"
    assert "Active implementation: <strong>live</strong>" in get(frontend_port, "/")
    print("PASS wildcard start, defaults, template, grouping, profile exclusion, real HTTP chain", flush=True)

    # An invalid persisted choice must fail without changing the saved file.
    run("variant", "set", "fixture")
    saved = (work / ".fed/variants.yaml").read_text()
    run("variant", "set", "catalog:missing", success=False)
    assert (work / ".fed/variants.yaml").read_text() == saved
    assert status()["catalog"]["variant"] == "live"  # running state, not future choice
    assert json.loads(run("variant", "list", "--json"))["catalog"]["variant"] == "fixture"
    run("restart", "--all")
    switched = status()
    assert switched["catalog"]["variant"] == "fixture"
    assert json.loads(run("ports", "list", "--json"))["FRONTEND_PORT"] == frontend_port
    data = json.loads(get(frontend_port))
    assert data["implementation"] == "fixture" and data["items"][1]["stock"] == 0
    assert "Active implementation: <strong>fixture</strong>" in get(frontend_port, "/")
    print("PASS persisted preference, rejected pin, truthful running status, restart --all, same URL", flush=True)

    # The error path must restore monitoring of the already-running stack.
    run("start", "missing-service", success=False)

    # Failed readiness must not mean process death or trigger a restart.
    catalog_pid = switched["catalog"]["pid"]
    marker = work / ".fed/catalog-unhealthy"
    marker.touch()
    get(catalog_port, "/health", expected_status=503)
    wait_for(lambda: status()["catalog"]["status"] == "failing", "health turns failing")
    assert status()["catalog"]["pid"] == catalog_pid
    marker.unlink()
    wait_for(lambda: status()["catalog"]["status"] == "healthy", "health recovers without restart")
    assert status()["catalog"]["pid"] == catalog_pid

    # A per-command pin beats the saved preference. The next status has no flag.
    run("--variant", "catalog:live", "restart", "catalog")
    assert status()["catalog"]["variant"] == "live"
    assert json.loads(get(frontend_port))["implementation"] == "live"
    run("start", "storage")  # unrelated start, without the original CLI pin
    assert status()["catalog"]["variant"] == "live"
    marker.touch()
    wait_for(lambda: status()["catalog"]["status"] == "failing", "unrelated start preserves supervision")
    marker.unlink()
    wait_for(lambda: status()["catalog"]["status"] == "healthy", "running CLI-pinned variant recovers")
    run("stop")

    # An explicit empty dependency list really permits standalone operation.
    run("start", "catalog")
    standalone = status()
    assert standalone["catalog"]["variant"] == "fixture"
    assert not standalone.get("storage", {}).get("pid")
    assert json.loads(get(json.loads(run("ports", "list", "--json"))["CATALOG_PORT"]))["implementation"] == "fixture"
    run("stop")
    print("PASS CLI pin precedence and fixture starts without storage", flush=True)

    run("variant", "clear")
    run("start", "--all")
    assert status()["catalog"]["variant"] == "live"
    if os.name == "posix":
        run("--profile", "console", "start", "--all")
        assert json.loads(run("--profile", "console", "status", "--json"))["console"]["attachable"]
        print("PASS optional console is hosted and attachable", flush=True)
finally:
    # Preserve the original failure, while always attempting the real stop path.
    result = subprocess.run([str(binary), "stop"], cwd=work, text=True, capture_output=True, timeout=45)
    record({"command": ["stop (cleanup)"], "code": result.returncode,
            "stdout": result.stdout, "stderr": result.stderr})
    assert result.returncode == 0, result.stderr

assert all(service["pid"] is None for service in status().values())
assert not any((work / ".fed").rglob("*.sock")), "Attach socket survived stop"
print("PASS cleanup; all scenarios passed", flush=True)
