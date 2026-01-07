# Installation Guide

## Quick Start

### Install with pip

```bash
# Install in editable mode (for development)
pip install -e .

# Install with dev dependencies
pip install -e ".[dev]"
```

### Install with uv

```bash
# Install in editable mode
uv pip install -e .

# Install with dev dependencies
uv pip install -e ".[dev]"

# Or use uv sync (creates/updates virtual environment)
uv sync
uv sync --extra dev
```

## Verify Installation

```bash
# Verify the package is installed
python -c "import databricks_utils; print(databricks_utils.__version__)"

# Test import of utilities
python -c "from databricks_utils import get_workspace_client; print('Success!')"
```

## Building Distribution Packages

```bash
# Install build tool
pip install build

# Build source and wheel distributions
python -m build

# This creates:
# - dist/databricks_utils-0.1.0.tar.gz (source distribution)
# - dist/databricks_utils-0.1.0-py3-none-any.whl (wheel)
```

## Publishing to PyPI (Optional)

```bash
# Install twine
pip install twine

# Upload to TestPyPI first
twine upload --repository testpypi dist/*

# Upload to PyPI
twine upload dist/*
```

## Using in Another Project

### With pip

```bash
# Install from git repository
pip install git+https://github.com/yourusername/databricks-utils.git

# Or add to requirements.txt
# git+https://github.com/yourusername/databricks-utils.git
```

### With uv

```bash
# Add to your project
uv add git+https://github.com/yourusername/databricks-utils.git

# Or add to pyproject.toml
# [project.dependencies]
# databricks-utils = { git = "https://github.com/yourusername/databricks-utils.git" }
```

## Development Workflow

1. Clone the repository
2. Create a virtual environment
3. Install in editable mode with dev dependencies
4. Make your changes
5. Run tests and linters
6. Commit and push

```bash
# Using pip
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -e ".[dev]"

# Using uv (recommended - much faster)
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv sync --extra dev
```
