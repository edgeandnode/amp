"""
Configuration management for the Data Loader Framework

Provides connection management, environment variable support,
and configuration schemas for different loader types.
"""

from .connection_manager import ConnectionManager

__all__ = ['ConnectionManager']
