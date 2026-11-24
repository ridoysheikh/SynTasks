"""
Dependency injection for SynTasks.

Vendored from taskiq-dependencies
(https://github.com/taskiq-python/taskiq-dependencies),
Copyright (c) 2024 Pavel Kirilin, MIT License.
"""

from syntasks.dependencies.dependency import Depends
from syntasks.dependencies.graph import DependencyGraph
from syntasks.dependencies.utils import ParamInfo

__all__ = ["DependencyGraph", "Depends", "ParamInfo"]
