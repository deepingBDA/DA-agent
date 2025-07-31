"""\nMonkey patch FastMCP.tool to print the exact positional / keyword arguments the runtime passes in.\nThis is safe in dev environments and helps trace parameter-sync issues.\nImport this module **before** any @mcp.tool decorated functions are defined.\n"""

from fastmcp import FastMCP
from functools import wraps

# Store original FastMCP.tool method
_original_tool = FastMCP.tool

def _debug_tool(self, *d_args, **d_kwargs):
    """Replacement for FastMCP.tool that wraps the original decorator and prints args/kwargs."""
    decorator = _original_tool(self, *d_args, **d_kwargs)

    def outer(func):
        wrapped = decorator(func)

        @wraps(wrapped)
        def inner(*args, **kwargs):
            print("\n🔍 [FASTMCP_WRAPPER] tool:", func.__name__)
            print("🔍 [FASTMCP_WRAPPER] positional args:", args)
            print("🔍 [FASTMCP_WRAPPER] keyword args:", kwargs)
            return wrapped(*args, **kwargs)

        return inner

    return outer

# Apply monkey patch only once
if getattr(FastMCP, "_debug_tool_patched", False) is False:
    FastMCP.tool = _debug_tool  # type: ignore
    FastMCP._debug_tool_patched = True  # type: ignore
    print("🔧 FastMCP.tool monkey-patched for debug logging.")