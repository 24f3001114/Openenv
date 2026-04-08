"""DataOps Environment Client."""

from typing import Dict

from openenv.core import EnvClient
from openenv.core.client_types import StepResult

try:
    from .models import DataOpsAction, DataOpsObservation, DataOpsState
except ImportError:
    from models import DataOpsAction, DataOpsObservation, DataOpsState


class DataopsEnv(EnvClient[DataOpsAction, DataOpsObservation, DataOpsState]):
    """Client for the DataOps Environment."""

    def _step_payload(self, action: DataOpsAction) -> Dict:
        return {
            "tool_name": action.tool_name,
            "arguments": action.arguments,
        }

    def _parse_result(self, payload: Dict) -> StepResult[DataOpsObservation]:
        obs_data = payload.get("observation", {})
        observation = DataOpsObservation(
            result=obs_data.get("result"),
            error=obs_data.get("error"),
            tool_name=obs_data.get("tool_name", ""),
            available_tools=obs_data.get("available_tools"),
            task_prompt=obs_data.get("task_prompt"),
            done=payload.get("done", False),
            reward=payload.get("reward"),
            metadata=obs_data.get("metadata", {}),
        )
        return StepResult(
            observation=observation,
            reward=payload.get("reward"),
            done=payload.get("done", False),
        )

    def _parse_state(self, payload: Dict) -> DataOpsState:
        return DataOpsState(
            episode_id=payload.get("episode_id"),
            step_count=payload.get("step_count", 0),
            current_task=payload.get("current_task", ""),
            task_id=payload.get("task_id", 0),
            tools_called=payload.get("tools_called", []),
            max_steps=payload.get("max_steps", 20),
        )
