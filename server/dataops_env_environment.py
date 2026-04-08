"""
DataOps Environment — Core environment logic.
Simulates a data engineering workspace where agents manage data infrastructure.
"""

from typing import Any, Optional
from uuid import uuid4

from openenv.core.env_server.interfaces import Environment

try:
    from ..models import DataOpsAction, DataOpsObservation, DataOpsState
    from ..seed_data import SEED_FUNCTIONS
    from ..tasks import TASKS
except (ImportError, ModuleNotFoundError):
    try:
        from models import DataOpsAction, DataOpsObservation, DataOpsState
        from seed_data import SEED_FUNCTIONS
        from tasks import TASKS
    except (ImportError, ModuleNotFoundError):
        import sys, os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from models import DataOpsAction, DataOpsObservation, DataOpsState
        from seed_data import SEED_FUNCTIONS
        from tasks import TASKS

from .database import DatabaseManager
from .grader import GRADERS
from .tools import TOOLS, TOOL_DESCRIPTIONS

# Reward tiers based on action type
EXPLORATION_TOOLS = {"list_tables", "describe_table", "list_views", "execute_sql", "explain_query"}
INVESTIGATION_TOOLS = {"run_quality_check", "get_pipeline_status", "check_access", "list_quality_rules"}
ACTION_TOOLS = {
    "create_view", "drop_view", "update_data", "delete_data",
    "add_quality_rule", "update_quality_rule", "delete_quality_rule",
    "create_pipeline", "run_pipeline", "revoke_access",
}

REWARD_EXPLORATION = 0.10
REWARD_INVESTIGATION = 0.20
REWARD_ACTION = 0.30
REWARD_ERROR = -0.15
REWARD_UNKNOWN_TOOL = -0.20
REWARD_INTERNAL_ERROR = -0.25
REWARD_REPEATED_NOOP = 0.02  # almost nothing for repeated identical calls


class DataopsEnvironment(Environment):
    """DataOps environment for training AI agents on data operations tasks."""

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self):
        super().__init__()
        self.db = DatabaseManager()
        self._state = DataOpsState(episode_id=str(uuid4()), step_count=0)
        self._task_id = 1
        self._submitted = False
        self._last_calls = []  # track recent (tool_name, args) for repeat detection
        self.db.create()
        seed_fn = SEED_FUNCTIONS.get(1)
        if seed_fn:
            seed_fn(self.db)

    def _compute_step_reward(self, tool_name: str, arguments: dict, success: bool) -> float:
        """Compute reward based on action type, success, and context."""
        if not success:
            return REWARD_ERROR

        # Check for repeated identical calls (agent spinning in circles)
        call_sig = (tool_name, str(sorted(arguments.items())))
        if call_sig in self._last_calls[-3:]:
            return REWARD_REPEATED_NOOP
        self._last_calls.append(call_sig)

        # Reward based on action category
        if tool_name in EXPLORATION_TOOLS:
            reward = REWARD_EXPLORATION
        elif tool_name in INVESTIGATION_TOOLS:
            reward = REWARD_INVESTIGATION
        elif tool_name in ACTION_TOOLS:
            reward = REWARD_ACTION
        else:
            reward = 0.10

        # Small urgency penalty as steps run out
        task = TASKS.get(self._task_id, TASKS[1])
        steps_used_ratio = self._state.step_count / task["max_steps"]
        if steps_used_ratio > 0.8:
            reward -= 0.05  # running out of time
        elif steps_used_ratio > 0.6:
            reward -= 0.02  # getting there

        return round(reward, 3)

    def _steps_remaining(self) -> int:
        task = TASKS.get(self._task_id, TASKS[1])
        return max(0, task["max_steps"] - self._state.step_count)

    def reset(
        self,
        seed: Optional[int] = None,
        episode_id: Optional[str] = None,
        **kwargs: Any,
    ) -> DataOpsObservation:
        """Reset the environment for a specific task."""
        self._task_id = kwargs.get("task_id", kwargs.get("task", 1))
        if isinstance(self._task_id, str):
            for tid, task in TASKS.items():
                if task["name"] == self._task_id:
                    self._task_id = tid
                    break
            else:
                try:
                    self._task_id = int(self._task_id)
                except ValueError:
                    self._task_id = 1

        if self._task_id not in TASKS:
            self._task_id = 1

        task = TASKS[self._task_id]

        self._state = DataOpsState(
            episode_id=episode_id or str(uuid4()),
            step_count=0,
            current_task=task["name"],
            task_id=self._task_id,
            tools_called=[],
            max_steps=task["max_steps"],
        )
        self._submitted = False
        self._last_calls = []

        self.db.create()
        seed_fn = SEED_FUNCTIONS.get(self._task_id)
        if seed_fn:
            seed_fn(self.db)

        return DataOpsObservation(
            result=None,
            error=None,
            tool_name="reset",
            available_tools=list(TOOL_DESCRIPTIONS.keys()),
            task_prompt=task["prompt"],
            steps_remaining=task["max_steps"],
            done=False,
            reward=0.0,
            metadata={
                "task_id": self._task_id,
                "task_name": task["name"],
                "task_title": task["title"],
                "difficulty": task["difficulty"],
                "max_steps": task["max_steps"],
                "tool_descriptions": TOOL_DESCRIPTIONS,
            },
        )

    def step(
        self,
        action: DataOpsAction,
        timeout_s: Optional[float] = None,
        **kwargs: Any,
    ) -> DataOpsObservation:
        """Execute a tool and return the result."""
        self._state.step_count += 1
        self._state.tools_called.append(action.tool_name)

        task = TASKS.get(self._task_id, TASKS[1])
        remaining = self._steps_remaining()

        # Check if max steps exceeded
        if self._state.step_count > task["max_steps"]:
            grader = GRADERS.get(self._task_id)
            grade_result = grader(self.db) if grader else {"score": 0.05}
            return DataOpsObservation(
                result={"message": "Maximum steps reached. Episode ending.", "grade": grade_result},
                error=None,
                tool_name="system",
                steps_remaining=0,
                done=True,
                reward=grade_result["score"],
                metadata={"reason": "max_steps_exceeded", "grade_details": grade_result},
            )

        # Check if tool exists
        tool_fn = TOOLS.get(action.tool_name)
        if tool_fn is None:
            return DataOpsObservation(
                result=None,
                error=f"Unknown tool: '{action.tool_name}'. Available tools: {list(TOOLS.keys())}",
                tool_name=action.tool_name,
                steps_remaining=remaining,
                done=False,
                reward=REWARD_UNKNOWN_TOOL,
                metadata={"step": self._state.step_count},
            )

        # Execute the tool
        try:
            result = tool_fn(self.db, **action.arguments)

            # Special handling for submit_report
            if action.tool_name == "submit_report":
                self._submitted = True
                grader = GRADERS.get(self._task_id)
                grade_result = grader(self.db) if grader else {"score": 0.05}
                return DataOpsObservation(
                    result={"submission": result, "grade": grade_result},
                    error=None,
                    tool_name=action.tool_name,
                    steps_remaining=remaining,
                    done=True,
                    reward=grade_result["score"],
                    metadata={
                        "step": self._state.step_count,
                        "grade_details": grade_result,
                    },
                )

            reward = self._compute_step_reward(action.tool_name, action.arguments, success=True)
            return DataOpsObservation(
                result=result,
                error=None,
                tool_name=action.tool_name,
                steps_remaining=remaining,
                done=False,
                reward=reward,
                metadata={"step": self._state.step_count, "reward_type": "success"},
            )

        except (ValueError, TypeError, KeyError) as e:
            return DataOpsObservation(
                result=None,
                error=str(e),
                tool_name=action.tool_name,
                steps_remaining=remaining,
                done=False,
                reward=REWARD_ERROR,
                metadata={"step": self._state.step_count, "reward_type": "tool_error"},
            )
        except Exception as e:
            return DataOpsObservation(
                result=None,
                error=f"Internal error: {str(e)}",
                tool_name=action.tool_name,
                steps_remaining=remaining,
                done=False,
                reward=REWARD_INTERNAL_ERROR,
                metadata={"step": self._state.step_count, "reward_type": "internal_error"},
            )

    @property
    def state(self) -> DataOpsState:
        return self._state

    def get_metadata(self):
        from openenv.core.env_server.types import EnvironmentMetadata
        return EnvironmentMetadata(
            name="DataOps Environment",
            description="Train AI agents on real-world data operations: data quality, pipeline management, and incident response.",
            version="1.0.0",
        )

    def close(self) -> None:
        self.db.close()
