#!/usr/bin/env python3
"""Call underlying function behind FastMCP tool for local debug."""
import mcp_tools.mcp_shelf as ms

# The FastMCP tool object
tool_obj = ms.get_shelf_analysis_flexible

# Underlying callable stored at .func if FastMCP.FunctionTool
underlying = getattr(tool_obj, 'func', None)
if underlying is None:
    print('❌ Could not access underlying function')
    exit(1)

params = dict(
    start_date='2025-06-12',
    end_date='2025-07-12',
    target_shelves=['빵'],
    age_groups=['10대'],
    gender_labels=['여자'],
    exclude_dates=['2025-06-22'],
    exclude_shelves=['진열대없음', '전자렌지'],
)
print('🔍 Calling underlying function with params:')
for k,v in params.items():
    print(f'  {k}: {v}')

result = underlying(**params)
print('\n🔍 Result:')
print(result)
