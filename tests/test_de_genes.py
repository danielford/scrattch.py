import pytest
import random
import pandas as pd

import scrattch.de_genes


def test_de_all_pairs(norm_dat, cl, genes):
    results = scrattch.de_genes.de_all_pairs(norm_dat, cl, genes)
    assert results is not None

def test_de_selected_pairs(norm_dat, cl, genes):
    pairs_subset = random.sample(scrattch.de_genes._generate_pairs(cl), k=3)
    results = scrattch.de_genes.de_selected_pairs(norm_dat, cl, genes, pairs_subset)
    assert results is not None

def test_generate_pairs():
    cl = [1, 1, 1, 2, 2, 3, 3, 3, 3, 3]
    random.shuffle(cl)
    cl = pd.Categorical(cl)
    pairs = scrattch.de_genes._generate_pairs(cl)
    pairs = sorted([tuple(sorted(pair)) for pair in pairs])
    assert sorted(pairs) == [(1, 2), (1, 3), (2, 3)]

