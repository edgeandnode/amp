import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse


class ConnectionManager:
    """Manages named connections for different data loaders"""

    def __init__(self, config_file: Optional[str] = None):
        self.config_file = config_file or os.getenv('FLIGHT_SQL_CONNECTIONS', '~/.flight_sql_connections.json')
        self.connections: Dict[str, Dict[str, Any]] = {}
        self.logger = logging.getLogger(__name__)
        self._load_connections()

    def add_connection(self, name: str, loader: str, config: Dict[str, Any]) -> None:
        """Add a named connection configuration"""
        self.connections[name] = {'loader': loader, 'config': config}
        self._save_connections()
        self.logger.info(f"Added connection '{name}' for loader '{loader}'")

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """Get connection information including loader type and config"""
        if name not in self.connections:
            env_config = self._get_env_config(name)
            if env_config:
                loader_type = self._infer_loader_type_from_name(name)
                return {'loader': loader_type, 'config': env_config}
            raise ValueError(f"Connection '{name}' not found. Available: {list(self.connections.keys())}")

        return self.connections[name]

    def _infer_loader_type_from_name(self, name: str) -> str:
        """Infer loader type from connection name for environment variables"""
        name_lower = name.lower()

        if any(db_name in name_lower for db_name in ['postgres', 'postgresql', 'pg']):
            return 'postgresql'
        elif 'redis' in name_lower:
            return 'redis'
        elif 'snowflake' in name_lower:
            return 'snowflake'
        elif 'delta' in name_lower:
            return 'delta_lake'
        elif 'iceberg' in name_lower:
            return 'iceberg'
        else:
            # Default fallback - could also raise an error
            return 'postgresql'

    def list_connections(self) -> Dict[str, str]:
        """List all configured connections"""
        return {name: conn['loader'] for name, conn in self.connections.items()}

    def remove_connection(self, name: str) -> None:
        """Remove a connection configuration"""
        if name in self.connections:
            del self.connections[name]
            self._save_connections()
            self.logger.info(f"Removed connection '{name}'")
        else:
            raise ValueError(f"Connection '{name}' not found")

    def _load_connections(self) -> None:
        """Load connections from config file"""
        config_path = Path(self.config_file).expanduser()
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    self.connections = json.load(f)
                self.logger.debug(f'Loaded {len(self.connections)} connections from {config_path}')
            except (json.JSONDecodeError, IOError) as e:
                self.logger.warning(f'Failed to load connections from {config_path}: {e}')
                self.connections = {}
        else:
            self.logger.debug(f'No connection file found at {config_path}')

    def _save_connections(self) -> None:
        """Save connections to config file"""
        config_path = Path(self.config_file).expanduser()
        config_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(config_path, 'w') as f:
                json.dump(self.connections, f, indent=2)
            self.logger.debug(f'Saved {len(self.connections)} connections to {config_path}')
        except IOError as e:
            self.logger.warning(f'Failed to save connections to {config_path}: {e}')

    def _get_env_config(self, name: str) -> Optional[Dict[str, Any]]:
        """Get configuration from environment variables"""
        # Support common patterns like DATABASE_URL, REDIS_URL, etc.
        env_patterns = {
            'postgresql': ['DATABASE_URL', 'POSTGRESQL_URL', 'POSTGRES_URL'],
            'redis': ['REDIS_URL'],
            'snowflake': ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD'],
        }

        for loader, env_vars in env_patterns.items():
            if name.startswith(loader) or name.endswith(loader) or name == loader:
                config = {}
                for env_var in env_vars:
                    if env_var in os.environ:
                        if env_var.endswith('_URL'):
                            # Parse connection URL
                            config.update(self._parse_connection_url(os.environ[env_var]))
                        else:
                            # Direct environment variable mapping
                            config[env_var.lower().replace('_', '')] = os.environ[env_var]

                if config:
                    self.logger.debug(f"Found environment config for '{name}' using loader '{loader}'")
                    return config

        return None

    def _parse_connection_url(self, url: str) -> Dict[str, Any]:
        """Parse connection URL into config dict"""
        parsed = urlparse(url)
        config = {}

        if parsed.hostname:
            config['host'] = parsed.hostname
        if parsed.port:
            config['port'] = parsed.port
        if parsed.path and parsed.path != '/':
            config['database'] = parsed.path.lstrip('/')
        if parsed.username:
            config['user'] = parsed.username
        if parsed.password:
            config['password'] = parsed.password

        if parsed.query:
            from urllib.parse import parse_qs

            query_params = parse_qs(parsed.query)
            for key, values in query_params.items():
                if values:
                    config[key] = values[0]

        return {k: v for k, v in config.items() if v is not None}
