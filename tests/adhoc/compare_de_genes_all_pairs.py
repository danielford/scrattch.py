#!/usr/bin/env python
"""Quick and dirty test harness for de_genes.de_all_pairs() using Human_MTG dataset."""

import os
import anndata as ad
import pandas as pd
import pyarrow.parquet as pq
import dask.array as da
from dask.distributed import Client, LocalCluster
from scrattch import de_genes

# start dask local client
cluster = LocalCluster(processes=False, threads_per_worker=20, n_workers=1)
client = Client(cluster)

data_dir = os.getenv('DATA_DIR', '/Users/danford/Workspace/ai/data/Human_MTG/')

# input data
cl_df = pq.read_table(os.path.join(data_dir, 'Human_MTG.cl.df.parquet')).to_pandas()
cl = pq.read_table(os.path.join(data_dir, 'Human_MTG.cl.parquet')).to_pandas()
norm_dat_anndata = ad.read_h5ad(os.path.join(data_dir, 'Human_MTG.norm.dat.h5ad'))

# convert anndata to dask array in-memory (unrealistic to do for large datasets, better to convert to zarr first)
norm_dat = da.from_array(norm_dat_anndata.X)

# results and summary from R version in scrattch.bigcat
r_results = pq.read_table(os.path.join(data_dir, 'Human_MTG.de_results.parquet'))
r_summary = pq.read_table(os.path.join(data_dir, 'Human_MTG.de_summary.parquet'))

# drop irrelevant columns and convert to pandas.DataFrames
r_results = r_results.to_pandas().drop(columns=['bin.x', 'bin.y', 'pair'])
r_summary = r_summary.to_pandas().drop(columns=['bin.x', 'bin.y', 'pair'])

# run our new python version
py_results, py_summary = de_genes.de_all_pairs(norm_dat, cl.cluster)

# compare results side-by-side
pd.testing.assert_frame_equal(r_summary, py_summary)
pd.testing.assert_frame_equal(r_results, py_results)
