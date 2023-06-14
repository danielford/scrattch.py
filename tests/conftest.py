import itertools
import pytest
import random
import string

from scipy.sparse import csr_matrix
import pandas as pd
import dask.array as da

@pytest.fixture
def cl():
    num_clusters = 25

    cl = list(itertools.chain(*[[i] * i for i in range(1, num_clusters + 1)]))
    random.shuffle(cl)

    return pd.Categorical(cl)

@pytest.fixture
def genes():
    num_genes = 10

    def random_gene(length=5):
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

    return [random_gene() for _ in range(num_genes)]

@pytest.fixture
def norm_dat(cl, genes):
    rng = da.random.default_rng()
    x = rng.random((len(cl), len(genes)))
    x = x.map_blocks(csr_matrix)

    return x
