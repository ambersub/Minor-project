"""
setup.py — builds the chunkflow C++ extension with pybind11 + OpenMP.
Windows/MinGW compatible version.

Install (editable):
    pip install pybind11 --upgrade
    pip install -e .
"""

import sys
import os
from pathlib import Path
from setuptools import Extension, setup, find_packages

try:
    import pybind11
    pybind11_include = pybind11.get_include()
except ImportError:
    raise SystemExit(
        "pybind11 is required to build chunkflow.\n"
        "Install it with:  pip install pybind11"
    )

# ── compiler / platform flags ──────────────────────────────────────────────

if sys.platform == "win32":
    # MinGW-w64 on Windows
    compile_args = [
        "-O3",
        "-std=c++17",
        "-fopenmp",
        "-DMS_WIN64",                 # tell pybind11 we are on 64-bit Windows
        "-D_hypot=hypot",             # MinGW/MSVC name clash fix
        "-fno-strict-aliasing",
    ]
    link_args = [
        "-fopenmp",
        "-static-libgcc",            # bundle runtime so the .pyd is self-contained
        "-static-libstdc++",
    ]
    # SQLite3: MinGW ships it; point to it explicitly if needed
    libraries  = []
    lib_dirs   = []

elif sys.platform == "darwin":
    # macOS — needs:  brew install libomp
    compile_args = [
        "-O3", "-std=c++17",
        "-Xpreprocessor", "-fopenmp",
    ]
    link_args  = ["-lomp"]
    libraries  = []
    lib_dirs   = []

else:
    # Linux / GCC
    compile_args = ["-O3", "-std=c++17", "-fopenmp"]
    link_args    = ["-fopenmp"]
    libraries    = []
    lib_dirs     = []

# ── extension definition ───────────────────────────────────────────────────

ext = Extension(
    name="chunkflow_core",
    sources=["src/chunkflow_core.cpp"],
    include_dirs=[pybind11_include],
    libraries=libraries,
    library_dirs=lib_dirs,
    extra_compile_args=compile_args,
    extra_link_args=link_args,
    language="c++",
)

# ── package metadata ───────────────────────────────────────────────────────

setup(
    name="chunkflow",
    version="0.1.0",
    description="CSV row split/join and column math; optional SQLite chunking in chunkflow.chunking",
    long_description="chunkflow C++ core extension",
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    packages=find_packages(exclude=["tests*"]),
    ext_modules=[ext],
    install_requires=["pybind11>=2.11"],
    extras_require={
        "dev": ["pytest", "pytest-benchmark"],
    },
)