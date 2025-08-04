#!/usr/bin/env python3

from setuptools import setup, find_packages

# Read README for long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements from Pipfile
install_requires = [
    "falkordb",
    "pandas",
]

setup(
    name="falkordb-graph-exporter",
    version="1.0.0",
    author="FalkorDB inc",
    author_email="info@falkordb.com",
    description="A command-line tool to export FalkorDB graph data to CSV files",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/FalkorDB/graph-exporter",
    license="MIT",
    py_modules=["main"],
    install_requires=install_requires,
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "falkordb-graph-exporter=main:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="falkordb graph database export csv cypher",
    project_urls={
        "Bug Reports": "https://github.com/FalkorDB/graph-exporter/issues",
        "Documentation": "https://docs.falkordb.com",
        "Source": "https://github.com/FalkorDB/graph-exporter",
    },
)