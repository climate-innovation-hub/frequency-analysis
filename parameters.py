"""Command line program for obtaining probability distribution parameters."""

import logging
import argparse

import xarray as xr
from xclim.indices.generic import select_resample_op
from xclim.indices.stats import frequency_analysis, fit
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress
    
import eva


def get_parameters(da, distribution, mode, month=None, season=None):
    """Calculate return periods"""

    indexer = {}
    if season:
        indexer = {'season': season}
    elif month:
        indexer = {'month': month}
    sub = select_resample_op(
        da,
        op=mode,
        freq='Y',
        **indexer)
    #sub = sub.chunk({'time': -1})
    sub = sub.compute()
    params = fit(sub, dist=distribution)

    return params


def main(args):
    """Run the program."""
    
    if args.local_cluster:
        assert args.dask_dir, "Must provide --dask_dir for local cluster"
        dask.config.set(temporary_directory=args.dask_dir)
        cluster = LocalCluster(n_workers=args.nworkers)
        client = Client(cluster)
        print("Watch progress at http://localhost:8787/status")
    else:
        dask.diagnostics.ProgressBar().register()

    ds = eva.read_data(args.infiles, args.var, dataset_name=args.dataset)
    ds = eva.subset_and_chunk(
        ds,
        args.var,
        time_period=args.time_period,
        lon_chunk_size=args.lon_chunk_size,
    )
    params = get_parameters(
        ds[args.var],
        args.distribution,
        args.mode,
        month=args.month,
        season=args.season,
    )
    output_ds = params.to_dataset()
    
    output_ds.attrs = ds.attrs
    output_ds.attrs['history'] = eva.get_new_log(args.infiles[0], ds.attrs['history'])
    output_ds.to_netcdf(args.outfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    parser.add_argument("infiles", type=str, nargs='*', help="input files")
    parser.add_argument("var", type=str, help="variable name")
    parser.add_argument("outfile", type=str, help="output file name")      
    parser.add_argument(
        "--mode",
        type=str,
        choices=['min', 'max'],
        default='max',
        help='probability of exceedance (max) or non-exceedance (min) [default=max]',
    )
    parser.add_argument(
        "--time_period",
        type=str,
        nargs=2,
        default=None,
        metavar=('START_DATE', 'END_DATE'),
        help='Time period in YYYY-MM-DD format',
    )
    parser.add_argument(
        "--distribution",
        type=str,
        choices=['genextreme', 'gennorm', 'gumbel_r', 'gumbel_l'],
        default='genextreme',
        help='Name of the univariate probability distribution [default=genextreme]',
    )
    parser.add_argument(
        "--season",
        type=str,
        choices=['DJF', 'MAM', 'JJA', 'SON'],
        default=None,
        help='Only process data for a given season [default is annual]',
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        nargs='*',
        help='Only process data from a list of months [default is all months]',
    )
    parser.add_argument(
        "--dataset",
        type=str,
        choices=['AGCD'],
        default=None,
        help='Apply dataset and variable specific metadata fixes for CF compliance',
    )
    parser.add_argument(
        "--local_cluster",
        action="store_true",
        default=False,
        help='Use a local dask cluster',
    )
    parser.add_argument(
        "--dask_dir",
        type=str,
        default=None,
        help='Directory where dask worker space files can be written. Required for local cluster.',
    )
    parser.add_argument(
        "--nworkers",
        type=int,
        default=None,
        help='Number of workers for cluster',
    )
    parser.add_argument(
        "--lon_chunk_size",
        type=int,
        default=None,
        help='Size of longitude chunks (i.e. number of lons in each chunk)',
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)
    with dask.diagnostics.ResourceProfiler() as rprof:
        main(args)
    eva.profiling_stats(rprof)

