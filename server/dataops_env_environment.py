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


class DataopsEnvironment(Environment):
    """DataOps environment for training AI agents on data operations tasks."""

    SUPPORTS_CONCURRENT_SESSIONS: bool = True

    def __init__(self):
        super().__init__()
        self.db = DatabaseManager()
        self._state = DataOpsState(episode_id=str(uuid4()), step_count=0)
        self._task_id = 1
        self._submitted = False
        # Pre-initialize DB with default task so HTTP mode works
        self.db.create()
        seed_fn = SEED_FUNCTIONS.get(1)
        if seed_fn:
            seed_fn(self.db)

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

        # Check if max steps exceeded
        if self._state.step_count > task["max_steps"]:
            grader = GRADERS.get(self._task_id)
            grade_result = grader(self.db) if grader else {"score": 0.0}
            return DataOpsObservation(
                result={"message": "Maximum steps reached. Episode ending.", "grade": grade_result},
                error=None,
                tool_name="system",
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
                done=False,
                reward=-0.2,
                metadata={"step": self._state.step_count},
            )

        # Execute the tool
        try:
            result = tool_fn(self.db, **action.arguments)

            # Special handling for submit_report
            if action.tool_name == "submit_report":
                self._submitted = True
                grader = GRADERS.get(self._task_id)
                grade_result = grader(self.db) if grader else {"score": 0.0}
                return DataOpsObservation(
                    result={"submission": result, "grade": grade_result},
                    error=None,
                    tool_name=action.tool_name,
                    done=True,
                    reward=grade_result["score"],
                    metadata={
                        "step": self._state.step_count,
                        "grade_details": grade_result,
                    },
                )

            reward = 0.3 - 0.01
            return DataOpsObservation(
                result=result,
                error=None,
                tool_name=action.tool_name,
                done=False,
                reward=round(reward, 3),
                metadata={"step": self._state.step_count},
            )

        except (ValueError, TypeError, KeyError) as e:
            return DataOpsObservation(
                result=None,
                error=str(e),
                tool_name=action.tool_name,
                done=False,
                reward=-0.2,
                metadata={"step": self._state.step_count},
            )
        except Exception as e:
            return DataOpsObservation(
                result=None,
                error=f"Internal error: {str(e)}",
                tool_name=action.tool_name,
                done=False,
                reward=-0.3,
                metadata={"step": self._state.step_count},
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
