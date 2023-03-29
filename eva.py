"""Command line program for conducting extreme value analysis using the block maxima method."""

import argparse
import logging

import git
import numpy as np
import xarray as xr
import xclim as xc
from xclim.indices.generic import select_resample_op
from xclim.indices.stats import fit, parametric_quantile
import dask.diagnostics
from dask.distributed import Client, LocalCluster, progress
import cmdline_provenance as cmdprov


def profiling_stats(rprof):
    """Record profiling information."""

    max_memory = np.max([result.mem for result in rprof.results])
    max_cpus = np.max([result.cpu for result in rprof.results])

    logging.debug(f'Peak memory usage: {max_memory}MB')
    logging.debug(f'Peak CPU usage: {max_cpus}%')
    

def get_new_log(infile_log=None):
    """Generate command log for output file."""

    try:
        repo = git.Repo()
        repo_url = repo.remotes[0].url.split(".git")[0]
    except (git.exc.InvalidGitRepositoryError, NameError):
        repo_url = None
    new_log = cmdprov.new_log(
        infile_logs=infile_log,
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


def read_data(infiles, variable_name, dataset_name=None, input_units=None, output_units=None):
    """Read the input data file/s."""

    if len(infiles) == 1:
        ds = xr.open_dataset(infiles[0])
    else:
        ds = xr.open_mfdataset(infiles)

    if dataset_name:
        ds = fix_metadata(ds, dataset_name, variable_name)

    if input_units:
        ds[variable_name].attrs['units'] = input_units
    if output_units:
        ds[variable_name] = xc.units.convert_units_to(ds[variable_name], output_units)
        ds[variable_name].attrs['units'] = output_units

    try:
        ds = ds.drop('height')
    except ValueError:
        pass

    try:
        lat_dim = 'latitude' if 'latitude' in ds.dims else 'lat'
        lat_axis = ds[lat_dim].values
        if lat_axis[-1] < lat_axis[0]:
            ds = ds.isel({lat_dim: slice(None, None, -1)})
    except KeyError:
        pass

    return ds


def subset_and_chunk(ds, var, time_period=None, lon_chunk_size=None):
    """Subset and chunk a dataset."""

    if time_period:
        start_date, end_date = time_period
        ds = ds.sel({'time': slice(start_date, end_date)})

    chunk_dict = {'time': -1}
    if lon_chunk_size:
        lon_dim = 'longitude' if 'longitude' in ds.dims else 'lon'
        chunk_dict[lon_dim] = lon_chunk_size
    ds = ds.chunk(chunk_dict)
    logging.debug(f'Array size: {ds[var].shape}')
    logging.debug(f'Chunk size: {ds[var].chunksizes}')

    return ds


def single2list(item, numpy_array=False):
    """Check if item is a list and convert if it is not."""
    
    if type(item) == list or type(item) == tuple or type(item) == numpy.ndarray:
        output = item 
    elif type(item) == str:
        output = [item,]
    else:
        try:
            test = len(item)
        except TypeError:
            output = [item,]

    return output

def aep_to_ari(aep):
    """Convert from aep (%) to ari (years)"""
    assert aep < 100, "aep to be expressed as a percentage (must be < 100)"
    aep = aep/100
    return 1/(-np.log(1-aep))


def ari_to_aep(ari):
    """Convert from ari (years) to aep (%)"""
    return ((np.exp(1/ari) - 1)/np.exp(1/ari)) * 100


def aep_to_quantile(aep, mode):
    """Annual exceedance probability to quantile"""
    
    quantile = ari_to_quantile(aep_to_ari(aep), mode)
    
    return quantile

    
def ari_to_quantile(ari, mode):
    """Annual return interval to quantile"""
    
    if mode == 'max':
        quantile = 1 - (1. / ari)
    elif mode == 'min':
        quantile = 1. / ari
    else:
        raise ValueError('Invalid mode') 
        
    return quantile


def crop_edge_times(da, drop):
    """Crop edges of time axis"""
    
    if drop == 'neither':
        start = None
        stop = None
    elif drop == 'first':
        start = 1
        stop = None
    elif drop == 'last':
        start = None
        stop = -1
    elif drop == 'both':
        start = 1
        stop = -1
    else:
        raise ValueError('Invalid crop time edges')

    return da.isel({'time': slice(start, stop)})


def extreme_value_analysis(
    da, mode, distribution, fit_method, extreme_type, extreme_values, freq='Y', drop_edge_times='neither', month=None, season=None
):
    """Perform extreme value analysis using the block maxima method.

    Parameters
    ----------
    da : xarray DataArray
    mode : {'min', 'max'}
        Look for probability of exceedance (max) or non-exceedance (min)
    distribution : {'genextreme', 'gennorm', 'gumbel_r', 'gumbel_l'} 
        Name of the univariate probability distribution
    fit_method : {'ML', 'PWM'}
        Fitting method, either maximum likelihood (ML) or probability weighted moments (PWM; aka l-moments)
    extreme_type : {'ari', 'aep', 'quantile'}
        Type of extreme to return
        ari = annual return interval
        aep = annual exceedance probability
    extreme_values : Union[float, sequence]
        Extreme values to compute (i.e. ari, aep or quantile values)
    freq : str, default Y
        Time frequency for blocks
    drop_edge_times : {'neither', 'first', 'last', 'both'}, default neither
        Drop first and/or last time step after block aggregation
        (e.g. for freq = A-JUN the first and last year might be incomplete)
    month : list, optional
        Restrict analysis to these months (list of month numbers)
    season : {'DJF', 'MAM', 'JJA', 'SON'}, optional
        Restrict analysis to a particular season
        
    Returns
    -------
    eva_da : xarray DataArray

    Notes
    -----
    Example time frequency (freq) options:
      A-DEC = annual, with date label being last day of year
      A-NOV = annual Dec-Nov, date label being last day of the year
      A-AUG = annual Sep-Aug, date label being last day of the year
    """

    indexer = {}
    if season:
        indexer = {'season': season}
    elif month:
        indexer = {'month': month}
    block_values = select_resample_op(da, op=mode, freq=freq, **indexer)
    block_values = crop_edge_times(block_values, drop_edge_times)
    block_values = block_values.chunk({'time': -1})
    params = fit(block_values, dist=distribution, method=fit_method)

    extreme_values_list = single2list(extreme_values)
    if extreme_type == 'aep':
        quantiles = [aep_to_quantile(x, mode) for x in extreme_values_list]
        extreme_attrs = {'long_name': 'annual exceedance probability', 'units': '%'}
    elif extreme_type == 'ari':
        quantiles = [ari_to_quantile(x, mode) for x in extreme_values_list]
        extreme_attrs = {'long_name': 'annual return interval', 'units': 'years'}
    elif extreme_type == 'quantile':
        quantiles = extreme_values
    else:
        raise ValueError(f'Invalid extreme type: {extreme_type}')
    eva_da = parametric_quantile(params, q=quantiles)
    if not extreme_type == 'quantile':
        eva_da = eva_da.rename({'quantile': extreme_type})
        eva_da = eva_da.assign_coords({extreme_type: np.array(extreme_values_list)})
        eva_da[extreme_type].attrs = extreme_attrs

    return eva_da


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

    ds = read_data(
        args.infiles,
        args.var,
        dataset_name=args.dataset,
        input_units=args.input_units,
        output_units=args.output_units,
    )
    ds = subset_and_chunk(
        ds,
        args.var,
        time_period=args.time_period,
        lon_chunk_size=args.lon_chunk_size,
    )
    eva_da = extreme_value_analysis(
        ds[args.var],
        args.mode,
        args.distribution,
        args.fit_method,
        args.extreme_type,
        args.extreme_values,
        freq=args.time_freq,
        drop_edge_times=args.drop_edge_times,
        month=args.month,
        season=args.season,
    )
    if args.local_cluster:
        eva_da = eva_da.persist()
        progress(eva_da)
    output_ds = eva_da.to_dataset()
    
    output_ds.attrs = ds.attrs
    if 'history' in ds.attrs:
        infile_log = {args.infiles[0]: ds.attrs['history']}
    else:
        infile_log = None
    output_ds.attrs['history'] = get_new_log(infile_log)
    output_ds.to_netcdf(args.outfile)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        argument_default=argparse.SUPPRESS,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )     
    parser.add_argument("infiles", type=str, nargs='*', help="input files")
    parser.add_argument("var", type=str, help="variable name")
    parser.add_argument(
        "extreme_type",
        type=str,
        choices=('ari', 'aep', 'quantile'),
        help="type of extreme to return (annual return interval: ari; annual exceedance probability: aep; or quantile)"
    )
    parser.add_argument("outfile", type=str, help="output file name")
    parser.add_argument(
        "--extreme_values",
        type=float,
        nargs='*',
        required=True,
        help='extreme values to return (i.e. ari, aep or quantile values) [required]',
    )         
    parser.add_argument(
        "--mode",
        type=str,
        choices=['min', 'max'],
        default='max',
        help='probability of exceedance (max) or non-exceedance (min) [default=max]',
    )
    parser.add_argument(
        "--distribution",
        type=str,
        choices=['genextreme', 'gennorm', 'gumbel_r', 'gumbel_l'],
        default='genextreme',
        help='Name of the univariate probability distribution [default=genextreme]',
    )
    parser.add_argument(
        "--fit_method",
        type=str,
        choices=['ML', 'PWM'],
        default='ML',
        help='Fitting method, either maximum likelihood (ML) or probability weighted moments (PWM; aka l-moments) [default=ML]',
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
        "--time_freq",
        type=str,
        default='Y',
        help='Time frequency for blocks (e.g. A-JUN is a July-June year) [default is calendar year]',
    )
    parser.add_argument(
        "--drop_edge_times",
        type=str,
        default='neither',
        choices=['neither', 'first', 'last', 'both'],
        help='Drop first and/or last time step after block aggregation (e.g. for A-JUN the first and last year might be incomplete)',
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
        "--input_units",
        type=str,
        default=None,
        help="input data units (will override any units listed in file metadata)"
    )
    parser.add_argument(
        "--output_units",
        type=str,
        default=None,
        help="output data units"
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
