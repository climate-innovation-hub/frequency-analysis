"""Command line program for calculating return periods."""

import argparse
import logging

import git
import numpy as np
import xarray as xr
from xclim.indices.stats import frequency_analysis
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress
import cmdline_provenance as cmdprov


def profiling_stats(rprof):
    """Record profiling information."""

    max_memory = np.max([result.mem for result in rprof.results])
    max_cpus = np.max([result.cpu for result in rprof.results])

    logging.debug(f'Peak memory usage: {max_memory}MB')
    logging.debug(f'Peak CPU usage: {max_cpus}%')
    

def get_new_log(infile_name, infile_history):
    """Generate command log for output file."""

    try:
        repo = git.Repo()
        repo_url = repo.remotes[0].url.split(".git")[0]
    except (git.exc.InvalidGitRepositoryError, NameError):
        repo_url = None
    new_log = cmdprov.new_log(
        infile_logs={infile_name: infile_history},
        code_url=repo_url,
    )

    return new_log


def fix_metadata(ds, dataset_name, variable_name):
    """Apply dataset- and variable-specific metdata fixes.

    xclim does CF-compliance checks that some datasets fail
    """

    if (dataset_name == 'AGCD') and (variable_name == 'precip'):
        ds['precip'].attrs = {
            'standard_name': 'precipitation_flux',
            'long_name': 'Precipitation',
            'units': 'mm d-1',
        }
    elif (dataset_name == 'AGCD') and (variable_name == 'tmax'):
        ds['tmax'].attrs = {
            'standard_name': 'air_temperature',
            'long_name': 'Daily Maximum Near-Surface Air Temperature',
            'units': 'degC',
        }
    elif (dataset_name == 'AGCD') and (variable_name == 'tmin'):
        ds['tmin'].attrs = {
            'standard_name': 'air_temperature',
            'long_name': 'Daily Minimum Near-Surface Air Temperature',
            'units': 'degC',
        }
    else:
        ValueError(f'No metadata fixes defined for {dataset} {variable}')

    return ds


def read_data(infiles, variable_name, dataset_name=None):
    """Read the input data file/s."""

    if len(infiles) == 1:
        ds = xr.open_dataset(infiles[0])
    else:
        ds = xr.open_mfdataset(infiles)

    if dataset_name:
        ds = fix_metadata(ds, dataset_name, variable_name)

    try:
        ds = ds.drop('height')
    except ValueError:
        pass

    return ds


def subset_and_chunk(ds, var, time_period=None, lon_chunk_size=None):
    """Subset and chunk a dataset."""

    if time_period:
        start_date, end_date = time_period
        ds = ds.sel({'time': slice(start_date, end_date)})

    chunk_dict = {'time': -1}
    if lon_chunk_size:
        chunk_dict['lon'] = lon_chunk_size
    ds = ds.chunk(chunk_dict)
    logging.debug(f'Array size: {ds[var].shape}')
    logging.debug(f'Chunk size: {ds[var].chunksizes}')

    return ds


def calc_return_periods(da, periods, distribution, mode, month=None, season=None):
    """Calculate return periods"""

    indexer = {}
    if season:
        indexer = {'season': season}
    elif month:
        indexer = {'month': month}
    return_periods = frequency_analysis(
        da,
        t=periods,
        dist=distribution,
        mode=mode,
        **indexer
    )

    return return_periods


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

    ds = read_data(args.infiles, args.var, dataset_name=args.dataset)
    ds = subset_and_chunk(
        ds,
        args.var,
        time_period=args.time_period,
        lon_chunk_size=args.lon_chunk_size,
    )
    return_periods = calc_return_periods(
        ds[args.var],
        args.return_periods,
        args.distribution,
        args.mode,
        month=args.month,
        season=args.season,
    )
    if args.local_cluster:
        return_periods = return_periods.persist()
        progress(return_periods)
    output_ds = return_periods.to_dataset()
    
    output_ds.attrs = ds.attrs
    output_ds.attrs['history'] = get_new_log(args.infiles[0], ds.attrs['history'])
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
        "--return_periods",
        type=float,
        nargs='*',
        required=True,
        help='return periods (in years) to include in outfile [required]',
    )         
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
        "--memory_vis",
        action="store_true",
        default=False,
        help='Visualise memory and CPU usage (creates profile.html)',
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help='Set logging level to DEBUG',
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
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level)
    with dask.diagnostics.ResourceProfiler() as rprof:
        main(args)
    if args.memory_vis:
        rprof.visualize(filename='profile.html')
    profiling_stats(rprof)
