#!/usr/bin/env python
"""Quick and dirty test harness for cl_stats.compute_cluster_stats() using Human_MTG dataset."""

import code
import os
import anndata as ad
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import dask.array as da
from dask.distributed import Client, LocalCluster

from scrattch.cl_stats import compute_cluster_stats, Statistics

INTERACTIVE = False # drop into a Python CLI after calling compute_cluster_stats()

# start dask local client
cluster = LocalCluster(processes=False, threads_per_worker=20, n_workers=1)
client = Client(cluster)

data_dir = os.getenv('DATA_DIR', '/Users/danford/Workspace/ai/data/Human_MTG/')

# input data
cl = pq.read_table(os.path.join(data_dir, 'Human_MTG.cl.parquet')).to_pandas()
cl = pd.Categorical(cl.cluster).as_ordered()
norm_dat_anndata = ad.read_h5ad(os.path.join(data_dir, 'Human_MTG.norm.dat.h5ad'))

# convert anndata to dask array in-memory
# FIXME: convert this to zarr? this approach won't work for larger datasets
norm_dat = da.from_array(norm_dat_anndata.X)

genes = norm_dat_anndata.var['gene']
genes_index = pd.CategoricalIndex(genes)

# load the output from the R code and massage it into the same format as compute_cluster_stats() returns
def read_parquet_cl_stats(filename):
    df = pq.read_table(os.path.join(data_dir, filename)).to_pandas()
    df = df.set_index(genes_index)
    df.columns = pd.CategoricalIndex(df.columns.astype(int), name='cluster').as_ordered()
    return df

# cluster stats from R version in scrattch.bigcat
r_cl_means = read_parquet_cl_stats('Human_MTG.cl.means.parquet')
r_cl_present = read_parquet_cl_stats('Human_MTG.cl.present.parquet')
r_cl_sqr_means = read_parquet_cl_stats('Human_MTG.cl.sqr.means.parquet')

# run our new python version
cl_means, cl_present, cl_sqr_means = compute_cluster_stats(norm_dat, cl, genes, [Statistics.MEANS, Statistics.PRESENT, Statistics.SQR_MEANS])

if INTERACTIVE:
    code.interact(local=locals())  # drop into a Python CLI for troubleshooting

# compare results side-by-side
pd.testing.assert_frame_equal(r_cl_means, cl_means, check_categorical=False, check_names=False)
pd.testing.assert_frame_equal(r_cl_present, cl_present, check_categorical=False, check_names=False)
pd.testing.assert_frame_equal(r_cl_sqr_means, cl_sqr_means, check_categorical=False, check_names=False)

print('Python cluster statistics match output from R version in scrattch.bigcat!')
