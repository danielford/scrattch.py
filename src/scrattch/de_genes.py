import itertools
import pandas as pd
from dataclasses import dataclass, asdict

from scrattch.cl_stats import Statistics, compute_cluster_stats
from scrattch.de_ebayes import de_pairs_ebayes


@dataclass
class DEParams:
    """Parameters for DE gene analysis with sensible defaults"""

    # Sourced from https://github.com/AllenInstitute/scrattch.bigcat/blob/master/R/de.genes.R#L138-L146
    padj_thresh: float = 0.01
    lfc_thresh: float = 1.0
    q1_thresh: float = 0.5
    q2_thresh: float = None
    qdiff_thresh: float = 0.7
    cluster_size_thresh: int = 4


def _generate_pairs(cl):
    return list(itertools.combinations(cl.unique(), 2))

def de_selected_pairs(norm_dat, cl, genes, pairs, params=DEParams()):
    """Compute differential expression summary statistics for the selected pairs of clusters.

    :param norm_dat: a normalized sparse data matrix (cell x gene)
    :param cl: a sequence of cluster assignments (one for each cell/row in 'norm_dat')
    :param pairs: a sequence of 2-tuples (cluster_1, cluster_2) representing each pair
    :return: A (results, summary) tuple."""

    cl = pd.Categorical(cl)
    cl_size = cl.value_counts().to_dict()

    assert len(cl) == len(norm_dat), "Length of cluster assignments does not match row count of norm_dat!"

    cl_present, cl_means, cl_vars = compute_cluster_stats(norm_dat, cl, genes, [Statistics.PRESENT, Statistics.MEANS, Statistics.VARS])

    cl_present = cl_present.T
    cl_means = cl_means.T
    cl_vars = cl_vars.T

    return de_pairs_ebayes(pairs, cl_means, cl_vars, cl_present, cl_size, asdict(params))

def de_all_pairs(norm_dat, cl, genes, params=DEParams()):
    """Compute differential expression summary statistics for all pairs of clusters.

    See documentation for de_selected_pairs() for more details."""

    cl = pd.Categorical(cl)
    return de_selected_pairs(norm_dat, cl, genes, _generate_pairs(cl), params)

