"""Run the variant-stack walkthrough in a disposable copy."""

import argparse
import json
import os
import shutil
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen


class Project:
    """A scratch project with a stable binary and a transcript of each request."""

    def __init__(self, executable):
        self.root = Path(tempfile.mkdtemp(prefix="fed-variant-stack-"))
        self.work_dir = self.root / "project"
        self.binary = self.root / "fed"
        self.evidence = []

        shutil.copytree(
            Path(__file__).resolve().parent,
            self.work_dir,
            ignore=shutil.ignore_patterns(".fed", "__pycache__"),
        )
        # A concurrent repo rebuild must not replace a running daemon's binary.
        shutil.copy2(executable, self.binary)
        print(f"Scratch project and transcript: {self.root}", flush=True)

    def record(self, entry):
        self.evidence.append(entry)
        transcript = json.dumps(self.evidence, indent=2)
        (self.root / "evidence.json").write_text(transcript, encoding="utf-8")

    def run(self, *arguments, success=True):
        result = subprocess.run(
            [str(self.binary), *arguments],
            cwd=self.work_dir,
            text=True,
            capture_output=True,
            check=False,
            timeout=45,
        )
        self.record(
            {
                "command": list(arguments),
                "code": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }
        )
        if success:
            assert result.returncode == 0, (arguments, result.stdout, result.stderr)
        else:
            assert result.returncode != 0, f"Unexpected success: {arguments}"
        return result.stdout

    def status(self):
        return json.loads(self.run("status", "--json"))

    def ports(self):
        return json.loads(self.run("ports", "list", "--json"))

    def get(self, port, path="/catalog", expected_status=200):
        url = f"http://127.0.0.1:{port}{path}"
        try:
            response = urlopen(url, timeout=3)
        except HTTPError as error:
            response = error

        with response:
            body = response.read().decode("utf-8")
            self.record({"url": url, "status": response.status, "body": body})
            assert response.status == expected_status, body
        return body

    def wait_for_status(self, service, expected):
        deadline = time.monotonic() + 12
        while True:
            observed = self.status()[service]["status"]
            if observed == expected:
                return
            assert time.monotonic() < deadline, (service, expected, observed)
            time.sleep(0.1)


def passed(scenario):
    print(f"PASS {scenario}", flush=True)


def verify_startup(project):
    summary = project.run("validate")
    assert "stack (oneshot)" in summary, summary
    project.run("start")  # Uses the wildcard entrypoint.

    services = project.status()
    assert services["console"]["pid"] is None
    assert services["stack"]["service_type"] == "oneshot"
    assert services["stack"]["status"] == "completed"
    assert services["catalog"]["variant"] == "live"
    for name in ("storage", "catalog", "frontend"):
        assert services[name]["status"] == "healthy", services[name]

    frontend_port = project.ports()["FRONTEND_PORT"]
    catalog = json.loads(project.get(frontend_port))
    assert catalog["implementation"] == "live"
    assert catalog["items"][0] == {"name": "Notebook", "stock": 12}

    page = project.get(frontend_port, "/")
    assert "<strong>live</strong>" in page
    assert 'href="/styles.css"' in page
    assert "font-family:" in project.get(frontend_port, "/styles.css")
    project.get(frontend_port, "/missing", expected_status=404)
    passed("wildcard startup, shared defaults, template, grouping, and HTTP chain")


def verify_saved_selection(project):
    frontend_port = project.ports()["FRONTEND_PORT"]
    project.run("variant", "set", "fixture")

    # Rejected pins must leave the existing selection file untouched.
    selections = project.work_dir / ".fed/variants.yaml"
    saved = selections.read_text()
    project.run("variant", "set", "catalog:missing", success=False)
    assert selections.read_text() == saved

    # The saved selection describes the next start, not the running process.
    assert project.status()["catalog"]["variant"] == "live"
    selection = json.loads(project.run("variant", "list", "--json"))
    assert selection["catalog"]["variant"] == "fixture"

    project.run("restart", "--all")
    assert project.status()["catalog"]["variant"] == "fixture"
    assert project.ports()["FRONTEND_PORT"] == frontend_port
    catalog = json.loads(project.get(frontend_port))
    assert catalog["implementation"] == "fixture"
    assert catalog["items"][1] == {"name": "Sample pencil", "stock": 0}
    assert "<strong>fixture</strong>" in project.get(frontend_port, "/")
    passed("saved preference, rejected pin, running status, and whole-stack restart")


def verify_health_recovery(project):
    original_pid = project.status()["catalog"]["pid"]
    catalog_port = project.ports()["CATALOG_PORT"]
    marker = project.work_dir / ".fed/catalog-unhealthy"

    marker.touch()
    try:
        project.get(catalog_port, "/health", expected_status=503)
        project.wait_for_status("catalog", "failing")
        assert project.status()["catalog"]["pid"] == original_pid
    finally:
        marker.unlink(missing_ok=True)

    project.wait_for_status("catalog", "healthy")
    assert project.status()["catalog"]["pid"] == original_pid


def verify_supervisor_handoff(project):
    # Even a failed start must restore monitoring of the existing stack.
    project.run("start", "missing-service", success=False)
    verify_health_recovery(project)
    passed("health monitoring resumes after a failed start")

    # A CLI pin wins over the saved preference and survives an unrelated start.
    project.run("--variant", "catalog:live", "restart", "catalog")
    project.run("start", "storage")
    assert project.status()["catalog"]["variant"] == "live"
    catalog = json.loads(project.get(project.ports()["FRONTEND_PORT"]))
    assert catalog["implementation"] == "live"
    verify_health_recovery(project)
    passed("CLI pin and health monitoring survive an unrelated start")


def verify_standalone_fixture(project):
    project.run("stop")
    project.run("start", "catalog")

    services = project.status()
    assert services["catalog"]["variant"] == "fixture"
    assert services["storage"]["pid"] is None
    catalog = json.loads(project.get(project.ports()["CATALOG_PORT"]))
    assert catalog["implementation"] == "fixture"

    project.run("stop")
    passed("fixture starts without storage")


def verify_optional_console(project):
    project.run("variant", "clear")
    project.run("start", "--all")
    assert project.status()["catalog"]["variant"] == "live"

    if os.name == "posix":
        project.run("--profile", "console", "start", "--all")
        services = json.loads(project.run("--profile", "console", "status", "--json"))
        assert services["console"]["attachable"]
        passed("optional console is hosted and attachable")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fed",
        default=os.environ.get("FED", "fed"),
        help="path to the fed binary (defaults to $FED or fed on PATH)",
    )
    args = parser.parse_args()
    executable = shutil.which(args.fed)
    if not executable:
        parser.error(
            f"Cannot find fed: {args.fed}. From the repository root, run "
            "cargo build and pass --fed target/debug/fed."
        )

    project = Project(executable)
    try:
        project.run("isolate", "enable")
        verify_startup(project)
        verify_saved_selection(project)
        verify_supervisor_handoff(project)
        verify_standalone_fixture(project)
        verify_optional_console(project)
    finally:
        project.run("stop")

    assert all(service["pid"] is None for service in project.status().values())
    assert not any((project.work_dir / ".fed").rglob("*.sock"))
    passed("cleanup; all scenarios passed")


if __name__ == "__main__":
    main()
