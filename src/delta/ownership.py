"""File ownership tracking with optimized storage."""

from __future__ import annotations

from collections import Counter

from delta.models import FileInfo, OwnershipData


def compute_ownership(files: list[FileInfo]) -> OwnershipData:
    """Analyze file list and compute optimized ownership data.

    Finds the most common owner, group, and mode across all files and stores
    them as defaults. Files that differ from the defaults are stored as exceptions.
    """
    if not files:
        return OwnershipData()

    # Count occurrences of each owner, group, mode
    owner_counter: Counter[str] = Counter()
    group_counter: Counter[str] = Counter()
    mode_counter: Counter[str] = Counter()

    for f in files:
        if f.is_symlink:
            continue
        if f.owner:
            owner_counter[f.owner] += 1
        if f.group:
            group_counter[f.group] += 1
        if f.mode:
            mode_counter[f.mode] += 1

    default_owner = owner_counter.most_common(1)[0][0] if owner_counter else "root"
    default_group = group_counter.most_common(1)[0][0] if group_counter else "root"
    default_mode = mode_counter.most_common(1)[0][0] if mode_counter else "644"

    # Build exceptions list — only files that differ from the defaults
    exceptions: list[dict[str, str]] = []
    for f in files:
        if f.is_symlink:
            continue
        differs = (
            (f.owner and f.owner != default_owner)
            or (f.group and f.group != default_group)
            or (f.mode and f.mode != default_mode)
        )
        if differs:
            exc: dict[str, str] = {"path": f.path}
            if f.owner and f.owner != default_owner:
                exc["owner"] = f.owner
            if f.group and f.group != default_group:
                exc["group"] = f.group
            if f.mode and f.mode != default_mode:
                exc["mode"] = f.mode
            exceptions.append(exc)

    return OwnershipData(
        default_owner=default_owner,
        default_group=default_group,
        default_mode=default_mode,
        exceptions=exceptions,
    )


def get_file_ownership(path: str, ownership: OwnershipData) -> tuple[str, str, str]:
    """Get (owner, group, mode) for a specific file path from ownership data."""
    owner = ownership.default_owner
    group = ownership.default_group
    mode = ownership.default_mode

    for exc in ownership.exceptions:
        if exc["path"] == path:
            owner = exc.get("owner", owner)
            group = exc.get("group", group)
            mode = exc.get("mode", mode)
            break

    return owner, group, mode
