"""Make `components/` importable in tests, mirroring Airflow's PYTHONPATH
(where components/ is mounted and on the path). Lets tests do
`from ingest.src import ...` exactly as the DAG does.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent / "components"))
