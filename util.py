"""Utility functions and Constants for loading JSON templates and merging nested dictionaries
used across the 'bundles'
"""
import copy
import json
import os
from typing import Any

DEFAULT_TEMPLATES_PATH = os.path.join(
    os.getenv("DBS_HOME", os.path.join(os.path.abspath(os.path.dirname(__file__)), "..", "..")),
    "templates"
)
CURRENT_ENV = os.getenv("BUNDLE_ENV", "dev")
IS_DATABRICKS_BUNDLE_DEPLOY = os.getenv("DATABRICKS_ADDR") is not None


def load_template_file(file_name: str) -> dict[str, Any]:
    """Loads a template file in a specified location. Used especially for populating defaults"""
    with open(
        os.path.join(DEFAULT_TEMPLATES_PATH, file_name), "r", encoding="utf-8"
    ) as file:
        template_content = json.load(file)
    return template_content


def merge_nested_dict(
    template: dict[str, Any], merged_dict: dict[str, Any] | None = None
) -> dict[str, Any]:
    # Deep copy on a dict.  Eliminated shallow copy nested attribute issues.
    merged_dict = copy.deepcopy(template)
    merged_dict.update(merged_dict)  # Overwrite data.
    return merged_dict