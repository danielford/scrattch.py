from enum import Enum
from typing import Sequence, Mapping
import itertools
import dask.array as da
import dask.delayed
import pandas as pd

class Statistics(Enum):
    SUMS, MEANS, PRESENT, SQR_SUMS, SQR_MEANS, VARS = range(1, 7)


# returns pandas.DataFrame with columns: cluster, gene, [sum, sqr_sum, present]
def _chunk_statistics(chunk, cl_subset, gene_subset, stats: Sequence[Statistics], precomputed: pd.DataFrame = None):
    df = pd.DataFrame(chunk.todense(), columns=gene_subset)

    sum_df, sqr_sum_df, present_df, var_df = [None] * 4

    if Statistics.SUMS in stats or Statistics.MEANS in stats:
        sum_df = df.groupby(cl_subset).sum()
        sum_df['cluster'] = sum_df.index
        sum_df = sum_df.melt(id_vars=['cluster'], var_name='gene', value_name='sum')

    if Statistics.SQR_SUMS in stats or Statistics.SQR_MEANS in stats:
        sqr_sum_df = (df**2).groupby(cl_subset).sum()
        sqr_sum_df['cluster'] = sqr_sum_df.index
        sqr_sum_df = sqr_sum_df.melt(id_vars=['cluster'], var_name='gene', value_name='sqr_sum')
        sqr_sum_df = sqr_sum_df[sqr_sum_df['sqr_sum'] >= 0]

    if Statistics.PRESENT in stats:
        present_df = (df > 0).groupby(cl_subset).sum()
        present_df['cluster'] = present_df.index
        present_df = present_df.melt(id_vars=['cluster'], var_name='gene', value_name='present')

    if Statistics.VARS in stats:
        if precomputed is None or 'mean' not in precomputed.columns:
            raise RuntimeError('You must compute mean prior to variance')
        
        means_df = precomputed[['cluster', 'gene', 'mean']]
        means_df = means_df.pivot(index='cluster', columns='gene', values='mean')
        means_df = means_df.loc[cl_subset, gene_subset].reset_index()

        var_df = ((df - means_df)**2).groupby(cl_subset).sum()
        var_df['cluster'] = var_df.index
        var_df = var_df.melt(id_vars=['cluster'], var_name='gene', value_name='var_sum')


    all_dfs = [sum_df, sqr_sum_df, present_df, var_df]
    return pd.concat([df for df in all_dfs if df is not None]).groupby(['cluster', 'gene']).sum().reset_index()


# takes two pandas.DataFrame with columns: cluster, gene, sum, sqr_sum
# aggregates along cluster/gene, adding sums together
def _combine_chunk_statistics(df1, df2):
    return pd.concat([df1, df2]).groupby(['cluster', 'gene']).sum().reset_index()


# takes dask array, returns pandas.DataFrame wrapped in dask.delayed
def _delayed_chunk_statistics(norm_dat, cl, genes, chunk_index, stats, precomputed=None):
    chunk = norm_dat.blocks[chunk_index]
    offset = [a*b for (a,b) in zip(chunk_index, norm_dat.chunksize)]
    # metadata objects to pass in to chunk_statistics (methods called via
    # dask.delayed(...) are not supposed to access any global state)
    cl_subset = cl[offset[0]:offset[0]+chunk.shape[0]]
    gene_subset = genes[offset[1]:offset[1]+chunk.shape[1]]
    return dask.delayed(_chunk_statistics)(chunk, cl_subset, gene_subset, stats, precomputed)


# takes a list of pandas.DataFrame wrapped in dask.delayed
# returns a single dask.delayed pandas.DataFrame with the full statistics
def _combine_delayed_results(results):
    def pairs(xs):
        for i in range(0, len(xs), 2):
            yield xs[i:i+2]
    if len(results) == 0:
        return None  # shouldn't happen...
    elif len(results) == 1:
        return results[0]
    else:
        new_results = []
        for pair in pairs(results):
            if len(pair) == 2:
                new_results.append(dask.delayed(_combine_chunk_statistics)(pair[0], pair[1]))
            elif len(pair) == 1:
                new_results.append(dask.delayed(pair[0]))
        return _combine_delayed_results(new_results)

def _compute_statistics(norm_dat, cl, cl_sizes, genes, stats, precomputed=None):
    all_block_inds = list(itertools.product(*map(range, norm_dat.blocks.shape))) 
    delayed_results = [_delayed_chunk_statistics(norm_dat, cl, genes, inds, stats, precomputed) for inds in all_block_inds]
    df = _combine_delayed_results(delayed_results).compute()

    df = df.merge(cl_sizes, left_on='cluster', right_index=True) # add cluster size into 'count' column

    if Statistics.MEANS in stats:
        df['mean'] = df['sum'] / df['count'] 

    if Statistics.PRESENT in stats:
        df['present'] = df['present'] / df['count'] 

    if Statistics.SQR_MEANS in stats:
        df['sqr_mean'] = df['sqr_sum'] / df['count'] 

    if Statistics.VARS in stats:
        df['var'] = df['var_sum'] / df['count']

    return df

def compute_cluster_stats(
    norm_dat: da.Array,
    cl: Sequence[int],
    genes: Sequence[str],
    stats: list[Statistics]) -> list[pd.DataFrame]:

    genes = pd.Categorical(genes)
    cl = pd.Categorical(cl)
    cl_sizes = cl.value_counts()

    assert len(stats) > 0, "Must include at least one statistic to compute"
    assert len(cl) == norm_dat.shape[0], "Length of cluster assignments does not match row count of norm_dat!"
    assert len(genes) == norm_dat.shape[1], "Length of genes does not match column count of norm_dat!"

    
    results = None

    if Statistics.VARS in stats:
        pre_stats = set(stats.copy())
        post_stats = set()

        pre_stats.remove(Statistics.VARS)
        pre_stats.add(Statistics.MEANS)
        post_stats.add(Statistics.VARS)

        pre_results = _compute_statistics(norm_dat, cl, cl_sizes, genes, pre_stats, None)
        post_results = _compute_statistics(norm_dat, cl, cl_sizes, genes, post_stats, pre_results)

        results = pd.merge(pre_results, post_results, on=['cluster', 'gene'])
    else:
        results = _compute_statistics(norm_dat, cl, cl_sizes, genes, stats)

    stat_col_mapping = {
        Statistics.SUMS: 'sum',
        Statistics.MEANS: 'mean',
        Statistics.PRESENT: 'present',
        Statistics.SQR_SUMS: 'sqr_sum',
        Statistics.SQR_MEANS: 'sqr_mean',
        Statistics.VARS: 'var',
    }

    def prepare_results_df(stat):
        return results[['cluster', 'gene', stat_col_mapping[stat]]].pivot(
            index='gene', columns='cluster', values=stat_col_mapping[stat]).reindex(genes)
    
    dfs = [prepare_results_df(stat) for stat in stats]

    if len(dfs) == 1:
        return dfs[0]
    else:
        return tuple(dfs)
