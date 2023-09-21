#!/usr/bin/env python
"""Quick and dirty test harness for de_genes.de_all_pairs() using Human_MTG dataset."""

import code
import os
import anndata as ad
import pandas as pd
import pyarrow.parquet as pq
import dask.array as da
from dask.distributed import Client, LocalCluster
from scrattch import de_genes

INTERACTIVE = False # drop into a Python CLI after calling compute_cluster_stats()

if __name__ == '__main__':

    # start dask local client
    cluster = LocalCluster(processes=False, n_workers=1, threads_per_worker=5)
    client = Client(cluster)

    data_dir = os.getenv('DATA_DIR', '/Users/danford/Workspace/ai/data/Human_MTG/')

    # input data
    cl_df = pq.read_table(os.path.join(data_dir, 'Human_MTG.cl.df.parquet')).to_pandas()
    cl = pq.read_table(os.path.join(data_dir, 'Human_MTG.cl.parquet')).to_pandas()
    cl = pd.Categorical(cl.cluster).as_ordered()
    norm_dat_anndata = ad.read_h5ad(os.path.join(data_dir, 'Human_MTG.norm.dat.h5ad'))

    # convert anndata to dask array in-memory (unrealistic to do for large datasets, better to convert to zarr first)
    norm_dat = da.from_array(norm_dat_anndata.X)
    genes = norm_dat_anndata.var['gene']

    # results and summary from R version in scrattch.bigcat
    r_results = pq.read_table(os.path.join(data_dir, 'Human_MTG.de_results.parquet')).to_pandas()
    r_summary = pq.read_table(os.path.join(data_dir, 'Human_MTG.de_summary.parquet')).to_pandas()

    # drop irrelevant columns and convert to pandas.DataFrames
    r_results = r_results.drop(columns=['bin.x', 'bin.y', 'P1', 'P2'])
    r_summary = r_summary.drop(columns=['bin.x', 'bin.y', 'P1', 'P2'])

    # run our new python version
    py_results = de_genes.de_all_pairs(norm_dat, cl, genes)

    if INTERACTIVE:
        code.interact(local=locals())  # drop into a Python CLI for troubleshooting

    # results can't be compared easily at the top-level using pd.testing.assert_frame_equal
    # since the python result format is different
    #pd.testing.assert_frame_equal(r_gene_results, py_gene_results)

    py_results['pair'] = [f'{p[0]}_{p[1]}' for p in py_results.index]

    # compare up-genes and down-genes
    up_genes = py_results[['pair', 'up_genes']].explode('up_genes').rename(columns={'up_genes': 'gene'}).assign(sign = 'up').reset_index(drop=True)
    down_genes = py_results[['pair', 'down_genes']].explode('down_genes').rename(columns={'down_genes': 'gene'}).assign(sign = 'down').reset_index(drop=True)
    py_gene_results = pd.concat([up_genes, down_genes]).sort_values(['pair', 'sign', 'gene']).reset_index(drop=True)
    r_gene_results = r_results[['pair', 'gene', 'sign']].sort_values(['pair', 'sign', 'gene']).reset_index(drop=True)

    pairs_matching = []
    pairs_differing = []

    for pair in py_gene_results['pair'].unique():
        print('Checking pair %s...' % pair, end=None)
        py_tmp = py_gene_results.query(f'pair == "{pair}"').reset_index(drop=True)
        r_tmp = r_gene_results.query(f'pair == "{pair}"').reset_index(drop=True)

        try:
            pd.testing.assert_frame_equal(r_tmp, py_tmp)
            pairs_matching.append(pair)
            print(' match')
        except AssertionError:
            pairs_differing.append(pair)
            print(' difference')

    if pairs_differing:
        print('Some cluster pairs did not match: [%s]' % ", ".join(pairs_differing))
    else:
        print('Python DE gene analysis matches output from R version in scrattch.bigcat!')

    if INTERACTIVE:
        code.interact(local=locals())  # drop into a Python CLI for troubleshooting

