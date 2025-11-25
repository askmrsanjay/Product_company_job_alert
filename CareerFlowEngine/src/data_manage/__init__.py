"""
Data processing and validation
"""

from .data_manager import (
    validate_dataframe,
    ensure_required_columns,
    process_cumulative_data,
    analyze_duplicates
)

__all__ = [
    'validate_dataframe',
    'ensure_required_columns',
    'process_cumulative_data',
    'analyze_duplicates'
]
