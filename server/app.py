"""
FastAPI application for the DataOps Environment.
"""

import sys
import os

# Ensure parent directory is in path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from openenv.core.env_server.http_server import create_app
from models import DataOpsAction, DataOpsObservation
from server.dataops_env_environment import DataopsEnvironment


app = create_app(
    DataopsEnvironment,
    DataOpsAction,
    DataOpsObservation,
    env_name="dataops_env",
    max_concurrent_envs=5,
)


def main(host: str = "0.0.0.0", port: int = 8000):
    import uvicorn
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    main()
