"""
Utility functions for thinning management.
"""

import re
from typing import Tuple
from pathlib import Path


def estimate_backup_size(path: Path) -> int:
    """Estimate backup size for a directory or file."""
    if not path.exists():
        return 0
    
    total_size = 0
    if path.is_file():
        return path.stat().st_size
    
    # For directories, calculate total size of all files
    for item in path.rglob("*"):
        if item.is_file():
            try:
                total_size += item.stat().st_size
            except (OSError, PermissionError):
                # Skip files we can't access
                continue
    
    return total_size


def bytes_to_human_readable(bytes_value: int) -> str:
    """Convert bytes to human-readable format (e.g., 1.5 GB)."""
    if bytes_value == 0:
        return "0 bytes"
    
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(bytes_value)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} bytes"
    else:
        return f"{size:.1f} {units[unit_index]}"


def human_readable_to_bytes(size_str: str) -> int:
    """Convert human-readable size to bytes (e.g., '1.5 GB' -> bytes)."""
    size_str = size_str.strip().upper()
    
    # Extract number and unit - handle both "1 GB" and "1GB" formats
    parts = size_str.split()
    if len(parts) == 2:
        # Format like "1 GB"
        number_str, unit = parts
    elif len(parts) == 1:
        # Format like "1GB" - need to split number and unit
        import re
        # Use a more specific regex to avoid ReDoS - either integer or decimal with specific format
        match = re.match(r'^(\d+(?:\.\d+)?)([A-Z]+)$', size_str)
        if match:
            number_str, unit = match.groups()
        else:
            # Bare number - assume bytes
            number_str = size_str
            unit = "B"
    else:
        raise ValueError(f"Invalid size format: {size_str}")
    
    try:
        number = float(number_str)
    except ValueError:
        raise ValueError(f"Invalid size format: {size_str}")
    
    # Convert to bytes
    multipliers = {
        "B": 1,
        "BYTES": 1,
        "KB": 1024,
        "MB": 1024 ** 2,
        "GB": 1024 ** 3,
        "TB": 1024 ** 4,
        "PB": 1024 ** 5,
    }
    
    if unit not in multipliers:
        raise ValueError(f"Unknown unit: {unit}")

    return int(number * multipliers[unit])

def calculate_percentage(used: int, total: int) -> float:
    """Calculate percentage of used space."""
    if total == 0:
        return 0.0
    return (used / total) * 100


def format_usage_status(usage_info: dict) -> str:
    """Format usage information for display."""
    used = bytes_to_human_readable(usage_info["used_bytes"])
    total = bytes_to_human_readable(usage_info["total_bytes"])
    percentage = usage_info["usage_percentage"]
    available = bytes_to_human_readable(usage_info["available_bytes"])
    
    return (
        f"Usage: {used} / {total} ({percentage:.1f}%)\n"
        f"Available: {available}"
    )


def validate_thinning_config(max_size_str: str, warning_threshold: float) -> Tuple[int, bool]:
    """Validate thinning configuration parameters."""
    errors = []
    
    try:
        max_size_bytes = human_readable_to_bytes(max_size_str)
        if max_size_bytes <= 0:
            errors.append("Maximum size must be greater than 0")
    except ValueError as e:
        errors.append(f"Invalid maximum size format: {e}")
        max_size_bytes = 0
    
    if not 0 <= warning_threshold <= 1:
        errors.append("Warning threshold must be between 0 and 1")
    
    is_valid = len(errors) == 0
    return max_size_bytes, is_valid
