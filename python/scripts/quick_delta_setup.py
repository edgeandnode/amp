#!/usr/bin/env python3
"""
Quick setup script for Delta Lake integration testing.
Run this to set up your local Delta Lake test environment.
"""

import sys
import subprocess
from pathlib import Path


def check_dependencies():
    """Check if required dependencies are installed"""
    print('🔍 Checking dependencies...')

    # Check if deltalake is available
    try:
        import deltalake

        print(f'✅ deltalake: {deltalake.__version__}')
    except ImportError:
        print('❌ deltalake not found')
        print('   Install with: uv sync --group delta_lake')
        return False

    # Check if pyarrow is available
    try:
        import pyarrow

        print(f'✅ pyarrow: {pyarrow.__version__}')
    except ImportError:
        print('❌ pyarrow not found')
        print('   Install with: uv sync --group delta_lake')
        return False

    # Check if pandas is available
    try:
        import pandas

        print(f'✅ pandas: {pandas.__version__}')
    except ImportError:
        print('❌ pandas not found')
        print('   Install with: uv sync --group delta_lake')
        return False

    return True


def setup_environment():
    """Set up the Delta Lake test environment"""
    print('\n🚀 Setting up Delta Lake test environment...')

    try:
        # Import and setup the test environment
        from tests.delta_test_env import setup_test_env

        # Setup with clean environment
        config = setup_test_env(clean=True)

        print(f'✅ Environment setup complete!')
        print(f'   Base path: {config["base_path"]}')
        print(f'   Scenarios: {", ".join(config["scenarios"].keys())}')

        return True

    except Exception as e:
        print(f'❌ Environment setup failed: {e}')
        return False


def run_basic_tests():
    """Run basic Delta Lake tests to verify setup"""
    print('\n🧪 Running basic tests...')

    test_commands = [
        # Unit tests
        ['uv', 'run', 'pytest', 'tests/unit/test_delta_lake_loader.py::TestDeltaLakeLoader::test_init_success', '-v'],
        # Basic integration test
        ['uv', 'run', 'pytest', 'tests/integration/test_delta_lake_loader.py::TestDeltaLakeLoaderIntegration::test_table_lifecycle', '-v', '-s'],
    ]

    for i, cmd in enumerate(test_commands, 1):
        print(f'\n📋 Running test {i}/{len(test_commands)}: {" ".join(cmd[3:])}')

        try:
            result = subprocess.run(cmd, cwd=Path.cwd())
            if result.returncode == 0:
                print(f'✅ Test {i} passed')
            else:
                print(f'❌ Test {i} failed')
                return False
        except Exception as e:
            print(f'❌ Test {i} failed: {e}')
            return False

    return True


def show_usage_examples():
    """Show usage examples"""
    print('\n📚 Usage Examples:')
    print('=' * 50)

    print('\n1. Run unit tests:')
    print('   uv run pytest tests/unit/test_delta_lake_loader.py -v')

    print('\n2. Run integration tests:')
    print('   TEST_INTEGRATION=1 uv run pytest tests/integration/test_delta_lake_loader.py -v')

    print('\n3. Run specific test:')
    print('   uv run pytest tests/integration/test_delta_lake_loader.py::TestDeltaLakeLoaderIntegration::test_partitioning -v')

    print('\n4. Run performance tests:')
    print('   TEST_PERFORMANCE=1 uv run pytest tests/integration/test_delta_lake_loader.py -v -m performance')

    print('\n5. Use Delta Lake loader in code:')
    print("""
from src.nozzle.loaders.implementations.delta_lake_loader import DeltaLakeLoader
from tests.delta_test_env import get_test_env

# Get test environment
env = get_test_env()
config = env.get_scenario_config("basic")

# Create loader
loader = DeltaLakeLoader(config)

with loader:
    # Your Delta Lake operations here
    stats = loader.get_table_stats()
    print(f"Table version: {stats['version']}")
""")


def create_env_script():
    """Create environment activation script"""
    print('\n📝 Creating environment script...')

    script_content = """#!/bin/bash
# activate_delta_env.sh
# Source this script to set up Delta Lake environment variables

export TEST_INTEGRATION=1
export TEST_PERFORMANCE=1
export DELTA_TEST_ENV_SETUP=1

# Set log level for testing
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

echo "🚀 Delta Lake test environment activated"
echo "💡 Run tests with: uv run pytest tests/integration/test_delta_lake_loader.py -v"
"""

    script_path = Path('activate_delta_env.sh')
    with open(script_path, 'w') as f:
        f.write(script_content)

    # Make executable
    script_path.chmod(0o755)
    print(f'✅ Created activation script: {script_path}')
    print('   Source with: source activate_delta_env.sh')


def main():
    """Main setup function"""
    print('🚀 Delta Lake Quick Setup')
    print('=' * 50)

    # Check dependencies
    if not check_dependencies():
        print('\n❌ Setup failed - missing dependencies')
        print('💡 Install with: uv sync --group delta_lake')
        return False

    # Setup environment
    if not setup_environment():
        print('\n❌ Setup failed - environment setup error')
        return False

    # Run basic tests
    if not run_basic_tests():
        print('\n❌ Setup failed - basic tests failed')
        return False

    # Create environment script
    create_env_script()

    # Show usage examples
    show_usage_examples()

    print('\n🎉 Delta Lake setup complete!')
    print('=' * 50)
    print('✅ Dependencies installed')
    print('✅ Test environment created')
    print('✅ Basic tests passed')
    print('✅ Environment script created')

    print('\n🚀 Next steps:')
    print('1. Source the environment: source activate_delta_env.sh')
    print('2. Run integration tests: uv run pytest tests/integration/test_delta_lake_loader.py -v')
    print('3. Explore test scenarios in ~/.nozzle_test/delta_lake/')

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
