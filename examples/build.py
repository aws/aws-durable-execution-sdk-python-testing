#!/usr/bin/env python3

import shutil
import site
from pathlib import Path


def build():
    """Build examples with SDK dependencies from current environment."""
    examples_dir = Path(__file__).parent
    build_dir = examples_dir / "build"
    
    # Clean build directory
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir()
    
    print("Copying SDK from current environment...")
    
    # Copy the SDK from current environment (hatch installs it)
    for site_dir in site.getsitepackages():
        sdk_path = Path(site_dir) / "aws_durable_execution_sdk_python"
        if sdk_path.exists():
            shutil.copytree(sdk_path, build_dir / "aws_durable_execution_sdk_python")
            print(f"Copied SDK from {sdk_path}")
            break
    else:
        print("SDK not found in site-packages")
    
    print("Copying testing SDK source...")
    
    # Copy testing SDK source
    sdk_src = examples_dir.parent / "src" / "aws_durable_execution_sdk_python_testing"
    if sdk_src.exists():
        shutil.copytree(sdk_src, build_dir / "aws_durable_execution_sdk_python_testing")
    
    print("Copying example functions...")
    
    # Copy example source files
    src_dir = examples_dir / "src"
    for py_file in src_dir.glob("*.py"):
        if py_file.name != "__init__.py":
            shutil.copy2(py_file, build_dir)
    
    print(f"Build complete: {build_dir}")


if __name__ == "__main__":
    build()
