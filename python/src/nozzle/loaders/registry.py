import importlib
import logging
import pkgutil
from typing import Any, Dict, List, Type

from .base import DataLoader


class LoaderRegistry:
    """Registry for data loader implementations with auto-discovery"""

    _loaders: Dict[str, Type[DataLoader]] = {}
    _auto_discovered: bool = False
    _logger = logging.getLogger(__name__)

    @classmethod
    def register(cls, name: str, loader_class: Type[DataLoader]) -> None:
        """Register a loader class"""
        if not issubclass(loader_class, DataLoader):
            raise ValueError(f'Loader class {loader_class} must inherit from DataLoader')

        cls._loaders[name] = loader_class
        cls._logger.debug(f'Registered loader: {name}')

    @classmethod
    def get_loader_class(cls, name: str) -> Type[DataLoader]:
        """Get a loader class by name"""
        cls._ensure_auto_discovery()

        if name not in cls._loaders:
            available = list(cls._loaders.keys())
            raise ValueError(f"Loader '{name}' not found. Available loaders: {available}")

        return cls._loaders[name]

    @classmethod
    def create_loader(cls, name: str, config: Dict[str, Any]) -> DataLoader:
        """Create a loader instance"""
        loader_class = cls.get_loader_class(name)
        return loader_class(config)

    @classmethod
    def get_available_loaders(cls) -> List[str]:
        """Get list of available loader names"""
        cls._ensure_auto_discovery()
        return list(cls._loaders.keys())

    @classmethod
    def _ensure_auto_discovery(cls) -> None:
        """Ensure auto-discovery has been performed"""
        if not cls._auto_discovered:
            cls._auto_discover_loaders()
            cls._auto_discovered = True

    @classmethod
    def _auto_discover_loaders(cls) -> None:
        """Auto-discover and register loaders from the implementations directory"""
        try:
            # Get the current package (loaders)
            current_package = __name__.rsplit('.', 1)[0]  # Remove '.registry' to get 'loaders'

            # Look for loaders in the implementations subdirectory
            implementations_package = f'{current_package}.implementations'

            try:
                implementations_module = importlib.import_module(implementations_package)
            except ImportError:
                cls._logger.debug(f'Could not import implementations package {implementations_package}')
                return

            # Look for loader modules in the implementations package
            for importer, modname, ispkg in pkgutil.iter_modules(implementations_module.__path__, implementations_package + '.'):
                if not ispkg and modname.endswith('_loader'):
                    try:
                        module = importlib.import_module(modname)

                        # Look for loader classes
                        for attr_name in dir(module):
                            attr = getattr(module, attr_name)
                            if isinstance(attr, type) and issubclass(attr, DataLoader) and attr != DataLoader:
                                # Auto-generate loader name from class name
                                loader_name = attr_name.lower().replace('loader', '')
                                if not cls._loaders.get(loader_name):
                                    cls.register(loader_name, attr)
                                    cls._logger.debug(f'Auto-discovered loader: {loader_name} from {modname}')
                    except ImportError as e:
                        cls._logger.debug(f'Skipping {modname}: {e}')
                        pass  # Skip if dependencies not available
        except Exception as e:
            cls._logger.warning(f'Auto-discovery failed: {e}')


# Module-level convenience functions
def get_loader_class(name: str) -> Type[DataLoader]:
    return LoaderRegistry.get_loader_class(name)


def create_loader(name: str, config: Dict[str, Any]) -> DataLoader:
    return LoaderRegistry.create_loader(name, config)


def get_available_loaders() -> List[str]:
    return LoaderRegistry.get_available_loaders()
