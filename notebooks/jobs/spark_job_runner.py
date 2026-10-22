
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

JOBS_DIR = Path("/workspace/notebooks/jobs")

JOBS = {
    "bronze_incremental": JOBS_DIR / "bronze_incremental.py",
    "silver_refresh": JOBS_DIR / "silver_refresh.py",
    "gold_refresh": JOBS_DIR / "gold_refresh.py",
}

PORT = int(os.getenv("SPARK_JOB_RUNNER_PORT", "8090"))
RUN_LOCK = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    server_version = "MedicalSparkJobRunner/1.0"

    def _respond(self, status, payload):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._respond(200, {"status": "ok", "jobs": sorted(JOBS)})
        else:
            self._respond(404, {"error": "not_found"})

    def do_POST(self):
        if self.path != "/run":
            self._respond(404, {"error": "not_found"})
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))

            if length <= 0 or length > 100_000:
                self._respond(400, {"error": "invalid_request_size"})
                return

            request = json.loads(
                self.rfile.read(length).decode("utf-8")
            )
            job_name = request.get("job")

            if job_name not in JOBS:
                self._respond(
                    400,
                    {
                        "error": "unknown_job",
                        "allowed_jobs": sorted(JOBS),
                    },
                )
                return

            script = JOBS[job_name]

            if not script.is_file():
                self._respond(
                    500,
                    {
                        "success": False,
                        "error": f"script_missing: {script}",
                    },
                )
                return

            # Un seul job Spark à la fois dans ce service.
            if not RUN_LOCK.acquire(blocking=False):
                self._respond(
                    409,
                    {
                        "success": False,
                        "error": "another_job_is_running",
                    },
                )
                return

            try:
                env = os.environ.copy()
                env.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

                result = subprocess.run(
                    ["/opt/spark/bin/spark-submit", str(script)],
                    cwd=str(JOBS_DIR),
                    env=env,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=7200,
                    check=False,
                )

                self._respond(
                    200,
                    {
                        "success": result.returncode == 0,
                        "job": job_name,
                        "returncode": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    },
                )

            except subprocess.TimeoutExpired as exc:
                self._respond(
                    200,
                    {
                        "success": False,
                        "job": job_name,
                        "error": "spark_submit_timeout_after_7200_seconds",
                        "stdout": exc.stdout or "",
                        "stderr": exc.stderr or "",
                    },
                )

            finally:
                RUN_LOCK.release()

        except (ValueError, json.JSONDecodeError) as exc:
            self._respond(
                400,
                {"error": "invalid_json", "details": str(exc)},
            )

        except Exception as exc:
            self._respond(
                500,
                {
                    "success": False,
                    "error": type(exc).__name__,
                    "details": str(exc),
                },
            )

    def log_message(self, fmt, *args):
        print("[spark-job-runner]", fmt % args, flush=True)


if __name__ == "__main__":
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Spark job runner prêt sur le port interne {PORT}", flush=True)
    server.serve_forever()

