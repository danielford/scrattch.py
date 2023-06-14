# scrattch.py
Single-cell RNA-seq analysis for transcriptomic type characterization, in Python


## Development Info

```sh
# create a virtual environment and activate it on your shell
python3 -m venv .venv
source .venv/bin/activate

# installs package and dependencies as a library
pip install .

# installs in development / editable mode, with dependencies for running tests
pip install -e '.[dev]'

# run all unit tests
pytest

# run adhoc test script
python tests/adhoc/compare_cl_stats.py

```