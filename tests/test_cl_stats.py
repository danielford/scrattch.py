import pytest

from scrattch.cl_stats import Statistics, compute_cluster_stats

def test_compute_cluster_stats_happy_path(norm_dat, cl, genes):
    stats = [Statistics.MEANS, Statistics.SQR_MEANS, Statistics.PRESENT]

    means, sqr_means, present = compute_cluster_stats(norm_dat, cl, genes, stats)

    assert means.shape == (len(genes), len(cl.unique()))
    assert sqr_means.shape == (len(genes), len(cl.unique()))
    assert present.shape == (len(genes), len(cl.unique()))

