import pytest
import random
import pandas as pd

import scrattch.de_genes


def test_de_selected_pairs(norm_dat, cl):
    with pytest.raises(NotImplementedError):
        scrattch.de_genes.de_selected_pairs(norm_dat, cl, [])

def test_generate_pairs():
    cl = [1, 1, 1, 2, 2, 3, 3, 3, 3, 3]
    random.shuffle(cl)
    cl = pd.Categorical(cl)
    pairs = scrattch.de_genes._generate_pairs(cl)
    pairs = sorted([tuple(sorted(pair)) for pair in pairs])
    assert sorted(pairs) == [(1, 2), (1, 3), (2, 3)]

