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

# Debug patch to trace FastMCP parameter passing
try:
    import importlib
    importlib.import_module("mcp_tools.fastmcp_debug_patch")
except Exception as e:
    print("[mcp_tools] Debug patch import failed:", e) 