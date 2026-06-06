"""DebugMapperService — thin combiner over the debug collaborators.

The original ~1020-line service has been split into focused mixins under
core/debug/ (plus the adapter value-objects in core/debug/adapters.py).  This
module only composes them behind the original name so existing imports keep
working unchanged:

    from core.debug_mapper_service import DebugMapperService

Collaborator responsibilities:
  DebugCoreMixin         — __init__, endpoint/dashboard listing, metadata, shared helpers
  MappingInspectorMixin  — mapping coverage / visualization / SQL resolution
  WayInspectorMixin      — per-way inspection and nearest-way lookup
"""
from core.debug.core_mixin import DebugCoreMixin
from core.debug.mapping_inspector_mixin import MappingInspectorMixin
from core.debug.way_inspector_mixin import WayInspectorMixin


class DebugMapperService(
    DebugCoreMixin,
    MappingInspectorMixin,
    WayInspectorMixin,
):
    """Composed debug mapper service (see core/debug/ for each collaborator)."""
