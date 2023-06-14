import itertools
import pandas as pd

def _generate_pairs(cl):
    return list(itertools.combinations(cl.unique(), 2))

def de_selected_pairs(norm_dat, cl, pairs):
    """Compute differential expression summary statistics for the selected pairs of clusters.

    :param norm_dat: a normalized sparse data matrix (cell x gene)
    :param cl: a sequence of cluster assignments (one for each cell/row in 'norm_dat')
    :param pairs: a sequence of 2-tuples (cluster_1, cluster_2) representing each pair
    :return: A (results, summary) tuple."""

    cl = pd.Categorical(cl)
    assert len(cl) == len(norm_dat), "Length of cluster assignments does not match row count of norm_dat!"

    raise NotImplementedError()

def de_all_pairs(norm_dat, cl):
    """Compute differential expression summary statistics for all pairs of clusters.

    See documentation for de_selected_pairs() for more details."""

    cl = pd.Categorical(cl)
    return de_selected_pairs(norm_dat, cl, _generate_pairs(cl))

