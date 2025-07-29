"""mcp_tools package

Contains FastMCP tool servers and helper modules used across workflows.
Importing mcp_tools will automatically expose key submodules in __all__ for convenience.
"""

from importlib import import_module

__all__ = [
    "base_workflow",
    "map_config",
    "mcp_diagnose",
    "mcp_pos",
    "mcp_report",
    "visitor_diagnose_workflow",
]

# Lazy import submodules so that `import mcp_tools as mt; mt.mcp_diagnose` works.
for _name in __all__:
    globals()[_name] = import_module(f"{__name__}.{_name}") 