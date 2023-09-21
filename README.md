# scrattch.py
Single-cell RNA-seq analysis for transcriptomic type characterization, in Python


## Implementation Details

The cluster statistics implemented in cl_stats.py is based on the R implementation in [scrattch.bigcat](https://github.com/AllenInstitute/scrattch.bigcat) but uses Dask in order to process arrays bigger than physical memory.

The DE gene analysis implemented in de_genes.py was copy/pasted from [transcriptomic_clustering](https://github.com/AllenInstitute/transcriptomic_clustering) and then modified slightly to process cluster pairs in parallel (should be easy to contribute the changes back if necessary).

## Development Info

```sh
# first, create a virtual environment and activate it on your shell
python3 -m venv .venv
source .venv/bin/activate

# installs package and dependencies as a library (if you just want to run )
pip install .

# installs in development / editable mode, with dependencies for running tests
pip install -e '.[dev]'

# run all unit tests
pytest

# run adhoc test scripts
python tests/adhoc/compare_cl_stats.py
python tests/adhoc/compare_de_genes_all_pairs.py
```