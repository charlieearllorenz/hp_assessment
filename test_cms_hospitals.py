"""Tests: python -m unittest discover -s tests -v

The integration test spins up a local HTTP server that imitates the CMS
metastore and its CSV downloads, so no network access is needed.
"""
import json
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import cms_hospitals as job  # noqa: E402


class SnakeCaseTests(unittest.TestCase):
    def test_example_from_spec(self):
        self.assertEqual(
            job.to_snake_case("Patients’ rating of the facility linear mean score"),
            "patients_rating_of_the_facility_linear_mean_score",
        )

    def test_special_characters(self):
        self.assertEqual(job.to_snake_case("Facility ID"), "facility_id")
        self.assertEqual(job.to_snake_case("CMS Certification Number (CCN)"),
                         "cms_certification_number_ccn")
        self.assertEqual(job.to_snake_case("Rate / 1,000 -- Patients"),
                         "rate_1_000_patients")
        self.assertEqual(job.to_snake_case("Hospital's Name & Address"),
                         "hospitals_name_and_address")
        self.assertEqual(job.to_snake_case("Café score"), "cafe_score")

    def test_unique_and_non_empty(self):
        self.assertEqual(job.snake_case_headers(["Rate %", "Rate #", "***"]),
                         ["rate", "rate_2", "column_3"])


class HeaderRewriteTests(unittest.TestCase):
    def test_body_untouched_bom_removed(self):
        with tempfile.TemporaryDirectory() as d:
            src, dst = Path(d, "a.csv"), Path(d, "b.csv")
            body = b'"1","Smith, J","caf\xc3\xa9"\r\n"2","O\'Neil","x"\r\n'
            src.write_bytes(b'\xef\xbb\xbf"Facility ID","Patients\xe2\x80\x99 Name","Note"\r\n' + body)
            header = job.rewrite_csv_header(src, dst)
            self.assertEqual(header, ["facility_id", "patients_name", "note"])
            out = dst.read_bytes()
            self.assertTrue(out.startswith(b"facility_id,patients_name,note\r\n"))
            self.assertTrue(out.endswith(body))


# ---- integration ---------------------------------------------------------- #
CSV_A = '"Facility ID","Facility Name","Patients’ rating of the facility linear mean score"\n"1","A","90"\n'
CSV_B = "Measure ID,Score (%)\nM1,5\nM2,6\n"


class MockCMS:
    def __init__(self):
        self.hits = {}
        self.modified = {"aaaa-0001": "2026-01-01", "bbbb-0002": "2026-01-01"}
        mock = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                mock.hits[self.path] = mock.hits.get(self.path, 0) + 1
                base = f"http://127.0.0.1:{mock.server.server_port}"
                if self.path.startswith("/items"):
                    def ds(i, title, theme, f):
                        return {"identifier": i, "title": title, "theme": theme,
                                "modified": mock.modified.get(i, "2026-01-01"),
                                "released": "2026-02-01",
                                "distribution": [
                                    {"mediaType": "text/csv", "downloadURL": f"{base}/{f}"},
                                    {"mediaType": "application/pdf", "downloadURL": f"{base}/dict.pdf"}]}
                    body = json.dumps([
                        ds("aaaa-0001", "Hospital General Information", ["Hospitals"], "a.csv"),
                        ds("bbbb-0002", "Timely & Effective Care", ["Hospitals"], "b.csv"),
                        ds("cccc-0003", "Dialysis Listing", ["Dialysis facilities"], "c.csv"),
                    ]).encode()
                    ctype = "application/json"
                elif self.path == "/a.csv":
                    body, ctype = CSV_A.encode(), "text/csv"
                elif self.path == "/b.csv":
                    body, ctype = CSV_B.encode(), "text/csv"
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}/items"

    def close(self):
        self.server.shutdown()


class IncrementalRunTests(unittest.TestCase):
    def test_three_runs(self):
        mock = MockCMS()
        self.addCleanup(mock.close)
        with tempfile.TemporaryDirectory() as d:
            argv = ["--metastore-url", mock.url, "--output-dir", f"{d}/out",
                    "--state-file", f"{d}/state/state.json", "--retries", "0"]
            out = Path(d, "out")

            # Run 1: both hospital datasets downloaded, dialysis ignored.
            self.assertEqual(job.main(argv), 0)
            files = sorted(p.name for p in out.glob("*.csv"))
            self.assertEqual(files, [
                "hospital_general_information__aaaa-0001.csv",
                "timely_effective_care__bbbb-0002.csv"])
            first = (out / files[0]).read_text(encoding="utf-8")
            self.assertTrue(first.startswith(
                "facility_id,facility_name,patients_rating_of_the_facility_linear_mean_score\n"))
            self.assertNotIn("/c.csv", mock.hits)
            self.assertNotIn("/dict.pdf", mock.hits)

            # Run 2: nothing modified -> no CSV requests at all.
            self.assertEqual(job.main(argv), 0)
            self.assertEqual((mock.hits["/a.csv"], mock.hits["/b.csv"]), (1, 1))

            # Run 3: only one dataset modified -> only it is re-downloaded.
            mock.modified["bbbb-0002"] = "2026-03-01"
            self.assertEqual(job.main(argv), 0)
            self.assertEqual((mock.hits["/a.csv"], mock.hits["/b.csv"]), (1, 2))

            # A deleted output file is detected and restored.
            (out / files[0]).unlink()
            self.assertEqual(job.main(argv), 0)
            self.assertEqual(mock.hits["/a.csv"], 2)

            history = Path(d, "state", "run_history.jsonl").read_text().splitlines()
            counts = [(json.loads(h)["downloaded"], json.loads(h)["unchanged"]) for h in history]
            self.assertEqual(counts, [(2, 0), (0, 2), (1, 1), (1, 1)])

    def test_failure_is_retried_next_run(self):
        mock = MockCMS()
        self.addCleanup(mock.close)
        with tempfile.TemporaryDirectory() as d:
            argv = ["--metastore-url", mock.url, "--output-dir", f"{d}/out",
                    "--state-file", f"{d}/state/state.json", "--retries", "0"]
            # Point one dataset at a missing file by breaking its URL path.
            orig = job.csv_urls
            job.csv_urls = lambda item: [u.replace("b.csv", "missing.csv") for u in orig(item)]
            try:
                self.assertEqual(job.main(argv), 1)  # b fails, a succeeds
            finally:
                job.csv_urls = orig
            state = json.loads(Path(d, "state", "state.json").read_text())
            self.assertEqual(list(state["datasets"]), ["aaaa-0001"])
            self.assertEqual(job.main(argv), 0)  # b is picked up on the next run
            self.assertEqual(sorted(json.loads(Path(d, "state", "state.json").read_text())["datasets"]),
                             ["aaaa-0001", "bbbb-0002"])


if __name__ == "__main__":
    unittest.main()
