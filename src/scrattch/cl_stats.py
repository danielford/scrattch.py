from enum import Enum
from typing import Sequence
import itertools
import dask.array as da
import dask.delayed
import pandas as pd

class Statistics(Enum):
    SUMS, MEANS, PRESENT, SQR_SUMS, SQR_MEANS = range(1, 6)


# returns pandas.DataFrame with columns: cluster, gene, [sum, sqr_sum, present]
def _chunk_statistics(chunk, cl_subset, gene_subset, stats):
    df = pd.DataFrame(chunk.todense(), columns=gene_subset)

    # FIXME: only compute columns if needed by stats

    sum_df = df.groupby(cl_subset).sum()
    sum_df['cluster'] = sum_df.index
    sum_df = sum_df.melt(id_vars=['cluster'], var_name='gene', value_name='sum')

    sqr_sum_df = (df**2).groupby(cl_subset).sum()
    sqr_sum_df['cluster'] = sqr_sum_df.index
    sqr_sum_df = sqr_sum_df.melt(id_vars=['cluster'], var_name='gene', value_name='sqr_sum')
    sqr_sum_df = sqr_sum_df[sqr_sum_df['sqr_sum'] >= 0]

    present_df = (df > 0).groupby(cl_subset).sum()
    present_df['cluster'] = present_df.index
    present_df = present_df.melt(id_vars=['cluster'], var_name='gene', value_name='present')

    return pd.concat([sum_df, sqr_sum_df, present_df]).groupby(['cluster', 'gene']).sum().reset_index()


# takes two pandas.DataFrame with columns: cluster, gene, sum, sqr_sum
# aggregates along cluster/gene, adding sums together
def _combine_chunk_statistics(df1, df2):
    return pd.concat([df1, df2]).groupby(['cluster', 'gene']).sum().reset_index()


# takes dask array, returns pandas.DataFrame wrapped in dask.delayed
def _delayed_chunk_statistics(norm_dat, cl, genes, chunk_index, stats):
    chunk = norm_dat.blocks[chunk_index]
    offset = [a*b for (a,b) in zip(chunk_index, norm_dat.chunksize)]
    # metadata objects to pass in to chunk_statistics (methods called via
    # dask.delayed(...) are not supposed to access any global state)
    cl_subset = cl[offset[0]:offset[0]+chunk.shape[0]]
    gene_subset = genes[offset[1]:offset[1]+chunk.shape[1]]
    return dask.delayed(_chunk_statistics)(chunk, cl_subset, gene_subset, stats)


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

    all_block_inds = list(itertools.product(*map(range, norm_dat.blocks.shape)))
    delayed_results = [_delayed_chunk_statistics(norm_dat, cl, genes, inds, stats) for inds in all_block_inds]
    results = _combine_delayed_results(delayed_results).compute()

    # add cluster size into 'count' column
    results = results.merge(cl_sizes, left_on='cluster', right_index=True)

    def prepare_results_df(value_colname):
        return results[['cluster', 'gene', value_colname]].pivot(
            index='gene', columns='cluster', values=value_colname).reindex(genes)

    sums_df, means_df, present_df, sqr_sums_df, sqr_means_df = [None] * 5

    if Statistics.SUMS in stats:
        sums_df = prepare_results_df('sum')

    if Statistics.MEANS in stats:
        results['mean'] = results['sum'] / results['count'] 
        means_df = prepare_results_df('mean')

    if Statistics.PRESENT in stats:
        results['present'] = results['present'] / results['count'] 
        present_df = prepare_results_df('present')

    if Statistics.SQR_SUMS in stats:
        sqr_sums_df = prepare_results_df('sqr_sum')

    if Statistics.SQR_MEANS in stats:
        results['sqr_mean'] = results['sqr_sum'] / results['count'] 
        sqr_means_df = prepare_results_df('sqr_mean')

    df_mapping = {
        Statistics.SUMS: sums_df,
        Statistics.MEANS: means_df,
        Statistics.PRESENT: present_df,
        Statistics.SQR_SUMS: sqr_sums_df,
        Statistics.SQR_MEANS: sqr_means_df
    }
    
    return [df_mapping[stat] for stat in stats]
