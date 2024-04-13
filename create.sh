#!/bin/bash

base_dir="project1"

# Create the directory structure
mkdir -p ${base_dir}/configs
mkdir -p ${base_dir}/data/input
mkdir -p ${base_dir}/data/output
mkdir -p ${base_dir}/src
mkdir -p ${base_dir}/tests

# Create placeholder files
touch ${base_dir}/configs/etl_config.json
touch ${base_dir}/src/__init__.py
touch ${base_dir}/src/main.py
touch ${base_dir}/src/extractor.py
touch ${base_dir}/src/transformer.py
touch ${base_dir}/src/loader.py
touch ${base_dir}/src/spark.py
touch ${base_dir}/requirements.txt

touch ${base_dir}/requirements.txt
touch ${base_dir}/README.md
