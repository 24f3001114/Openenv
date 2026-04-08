"""
Data models for the DataOps Environment.

An environment for training AI agents on data operations tasks:
data quality, pipeline management, and incident response.
"""

from typing import Any, Dict, List, Optional

from openenv.core.env_server.types import Action, Observation, State
from pydantic import Field


class DataOpsAction(Action):
    """Action for the DataOps environment — agent calls a tool by name with arguments."""

    tool_name: str = Field(..., description="Name of the tool to call")
    arguments: Dict[str, Any] = Field(
        default_factory=dict, description="Arguments for the tool"
    )


class DataOpsObservation(Observation):
    """Observation from the DataOps environment — tool result or error."""

    result: Any = Field(default=None, description="Tool execution result")
    error: Optional[str] = Field(default=None, description="Error message if tool failed")
    tool_name: str = Field(default="", description="Which tool was called")
    available_tools: Optional[List[str]] = Field(
        default=None, description="List of available tools (returned on reset)"
    )
    task_prompt: Optional[str] = Field(
        default=None, description="Task description (returned on reset)"
    )


class DataOpsState(State):
    """State for the DataOps environment."""

    current_task: str = Field(default="", description="Current task name")
    task_id: int = Field(default=0, description="Current task ID (1, 2, or 3)")
    tools_called: List[str] = Field(
        default_factory=list, description="History of tools called"
    )
    max_steps: int = Field(default=20, description="Maximum steps allowed")
