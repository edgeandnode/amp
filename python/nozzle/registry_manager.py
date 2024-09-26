import json
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import importlib
from typing import Dict, Any, Type

class RegistryManager:
    REGISTRY_DIR = Path(__file__).parent / "registries"
    TEMPLATE_DIR = Path(__file__).parent / "templates"

    @classmethod
    def update_registry(cls, registry_name: str, data: dict):
        cls.REGISTRY_DIR.mkdir(exist_ok=True)
        registry_path = cls.REGISTRY_DIR / f"{registry_name}_registry.json"
        
        # Load existing data
        if registry_path.exists():
            with registry_path.open('r') as f:
                existing_data = json.load(f)
        else:
            existing_data = {}

        # Update data
        cls._deep_update(existing_data, data)

        # Save updated data
        with registry_path.open('w') as f:
            json.dump(existing_data, f, indent=2)

        # Generate Python file
        cls._generate_registry_py(registry_name)

    @classmethod
    def _generate_registry_py(cls, registry_name: str):
        env = Environment(loader=FileSystemLoader(cls.TEMPLATE_DIR))
        template = env.get_template(f"{registry_name}_registry_template.py.jinja")

        registry_path = cls.REGISTRY_DIR / f"{registry_name}_registry.json"
        with registry_path.open('r') as f:
            registry_data = json.load(f)

        rendered_content = template.render(registry=registry_data)

        output_path = Path(__file__).parent / f"{registry_name}_registry.py"
        with output_path.open('w') as f:
            f.write(rendered_content)

    @classmethod
    def reload_registry(cls, registry_name: str):
        importlib.reload(importlib.import_module(f'.{registry_name}_registry', package='nozzle'))

    @classmethod
    def _deep_update(cls, d, u):
        for k, v in u.items():
            if isinstance(v, dict):
                d[k] = cls._deep_update(d.get(k, {}), v)
            else:
                d[k] = v
        return d

    @classmethod
    def get_registry_item(cls, registry_name: str, item_name: str):
        registry_path = cls.REGISTRY_DIR / f"{registry_name}_registry.json"
        if not registry_path.exists():
            raise ValueError(f"Registry {registry_name} does not exist")
        
        with registry_path.open('r') as f:
            registry_data = json.load(f)
        
        keys = item_name.split('.')
        item = registry_data
        for key in keys:
            if key not in item:
                raise ValueError(f"Item {item_name} not found in registry {registry_name}")
            item = item[key]
        
        return cls._create_typed_class(item_name, item)
    
    @classmethod
    def get_registry(cls, registry_name: str):
        registry_path = cls.REGISTRY_DIR / f"{registry_name}_registry.json"
        if not registry_path.exists():
            raise ValueError(f"Registry {registry_name} does not exist")
        
        with registry_path.open('r') as f:
            return json.load(f)
    
    @classmethod
    def _create_typed_class(cls, class_name: str, data: Dict[str, Any]) -> Type:
        def __init__(self):
            for key, value in data.items():
                if isinstance(value, dict):
                    setattr(self, key, cls._create_typed_class(f"{class_name}_{key}", value)())
                else:
                    setattr(self, key, value)
        new_class = type(class_name, (), {"__init__": __init__})
        try:
            return new_class()
        except Exception as e:
            return new_class
