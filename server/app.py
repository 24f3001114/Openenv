"""
FastAPI application for the DataOps Environment.
"""

try:
    from openenv.core.env_server.http_server import create_app
except Exception as e:
    raise ImportError(
        "openenv is required. Install with: pip install openenv-core"
    ) from e

try:
    from ..models import DataOpsAction, DataOpsObservation
    from .dataops_env_environment import DataopsEnvironment
except (ImportError, ModuleNotFoundError):
    try:
        from models import DataOpsAction, DataOpsObservation
        from server.dataops_env_environment import DataopsEnvironment
    except (ImportError, ModuleNotFoundError):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    args = parser.parse_args()
    main(port=args.port)
