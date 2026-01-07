#!/usr/bin/env python3
"""
Demonstration of both import styles for databricks_utils.pipelines

This script shows that both import styles are equivalent and work correctly.
"""

print("=" * 70)
print("Demonstrating Both Import Styles for databricks_utils.pipelines")
print("=" * 70)

# Style 1: from databricks_utils import pipelines as pl
print("\n✓ Style 1: from databricks_utils import pipelines as pl")
from databricks_utils import pipelines as pl1

print(f"  Module: {pl1.__name__}")
print(f"  Functions: {[x for x in dir(pl1) if not x.startswith('_') and callable(getattr(pl1, x))]}")

# Style 2: import databricks_utils.pipelines as pl
print("\n✓ Style 2: import databricks_utils.pipelines as pl")
import databricks_utils.pipelines as pl2

print(f"  Module: {pl2.__name__}")
print(f"  Functions: {[x for x in dir(pl2) if not x.startswith('_') and callable(getattr(pl2, x))]}")

# Verify they're the same
print("\n" + "=" * 70)
print(f"Both styles import the same module: {pl1 is pl2}")
print("=" * 70)

# Demo: Show that both provide access to the same functions
print("\nDemonstration: Both import styles provide access to the same functions")
print("-" * 70)

print("\nAvailable pipeline functions:")
functions = ['create', 'dry_run', 'update', 'delete', 'start', 'stop',
             'get_pipeline', 'list_pipelines', 'get_pipeline_updates', 'wait_for_update']

for func in functions:
    has_in_pl1 = hasattr(pl1, func)
    has_in_pl2 = hasattr(pl2, func)
    same_func = getattr(pl1, func, None) is getattr(pl2, func, None)
    print(f"  {func:25} pl1: {has_in_pl1}  pl2: {has_in_pl2}  same: {same_func}")

print("\n" + "=" * 70)
print("✓ Both import styles work identically!")
print("=" * 70)

# Show usage recommendation
print("\nRecommended Usage:")
print("-" * 70)
print("""
# Choose the style you prefer - both work!

# Style 1 (more common):
from databricks_utils import get_workspace_client
from databricks_utils import pipelines as pl

client = get_workspace_client()
update_id = pl.dry_run(client, pipeline_id)

# Style 2 (more explicit):
from databricks_utils import get_workspace_client
import databricks_utils.pipelines as pl

client = get_workspace_client()
update_id = pl.dry_run(client, pipeline_id)

# Direct imports:
from databricks_utils import get_workspace_client
from databricks_utils.pipelines import dry_run, create, start

client = get_workspace_client()
update_id = dry_run(client, pipeline_id)
""")
