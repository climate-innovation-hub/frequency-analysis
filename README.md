# Frequency analysis

This directory contains command line programs for frequency analysis.

The programs make use of the frequency analysis capability built into the xclim library,
which is described at:  
https://xclim.readthedocs.io/en/stable/notebooks/frequency_analysis.html

If you're a member of the `wp00` project on NCI
(i.e. if you're part of the CSIRO Climate Innovation Hub),
the easiest way to use the scripts in this directory is to use the cloned copy at `/g/data/wp00/shared_code/frequency-analysis/`.
They can be run using the Python environment at `/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python`.

## Return periods

The `return_period.py` script calculates return period/s of interest.

Running the script at the command line with the `-h` option explains the user options:

```bash
$ /g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/return_period.py -h
```

```
usage: return_period.py [-h] --return_periods [RETURN_PERIODS ...] [--mode {min,max}] [--time_period START_DATE END_DATE]
                        [--distribution {genextreme,gennorm,gumbel_r,gumbel_l}] [--season {DJF,MAM,JJA,SON}] [--month [MONTH ...]]
                        [--dataset {AGCD}] [--memory_vis] [--verbose] [--local_cluster] [--nworkers NWORKERS]
                        [--lon_chunk_size LON_CHUNK_SIZE]
                        [infiles ...] var outfile

Command line program for calculating return periods.

positional arguments:
  infiles               input files
  var                   variable name
  outfile               output file name

options:
  -h, --help            show this help message and exit
  --return_periods [RETURN_PERIODS ...]
                        return periods (in years) to include in outfile [required]
  --mode {min,max}      probability of exceedance (max) or non-exceedance (min) [default=max]
  --time_period START_DATE END_DATE
                        Time period in YYYY-MM-DD format
  --distribution {genextreme,gennorm,gumbel_r,gumbel_l}
                        Name of the univariate probability distribution [default=genextreme]
  --season {DJF,MAM,JJA,SON}
                        Only process data for a given season [default is annual]
  --month [MONTH ...]   Only process data from a list of months [default is all months]
  --dataset {AGCD}      Apply dataset and variable specific metadata fixes for CF compliance
  --memory_vis          Visualise memory and CPU usage (creates profile.html)
  --verbose             Set logging level to DEBUG
  --local_cluster       Use a local dask cluster
  --dask_dir DASK_DIR   Directory where dask worker space files can be written. Required for local cluster.
  --nworkers NWORKERS   Number of workers for cluster
  --lon_chunk_size LON_CHUNK_SIZE
                        Size of longitude chunks (i.e. number of lons in each chunk)
```

### Example 1

FFDI data could be processed to calculate the 20 year return period as follows:

```
/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/return_period.py /g/data/wp00/QQSCALE/GFDL-ESM2M/FFDI/rcp45/2016-2045/ffdi.????.nc FFDI ffdi_ARI_20yr_GFDL-ESM2M-QME_rcp45_2016-2045.nc --return_periods 20 --verbose --lon_chunk_size 5 --local_cluster --dask_dir /g/data/wp00/users/dbi599/
```

In order to run the calculation in parallel,
the data have been chunked along the longitude axis (with 5 latitude values per chunk; `--lon_chunk_size 5`)
and the parallel processing has been coordinated using a local dask cluster (`--local_cluster`)
that writes dask worker files to `/g/data/wp00/users/dbi599/` (`--dask_dir`).

In order to run this as a job on the Gadi job queue,
the job file might look like the following:

```
#!/bin/bash
#PBS -P wp00
#PBS -q normal
#PBS -l walltime=10:00:00
#PBS -l mem=60GB
#PBS -l storage=gdata/wp00
#PBS -l wd
#PBS -l ncpus=5

/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/return_period.py /g/data/wp00/QQSCALE/GFDL-ESM2M/FFDI/rcp45/2016-2045/ffdi.????.nc FFDI /g/data/wp00/users/dbi599/test_space/ffdi_test.nc --return_periods 20 --verbose --lon_chunk_size 5 --local_cluster --dask_dir /g/data/wp00/users/dbi599/
```

### Example 2

AGCD precipitation data could be processed to calculate the 5, 10 and 20 year return period as follows:
```
$ /g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/return_period.py /g/data/zv2/agcd/v1/precip/total/r005/01month/agcd_v1_precip_total_r005_monthly_197* precip /g/data/wp00/users/dbi599/precip_ari_AGCD_1970-1979.nc --return_periods 5 10 20 --dataset AGCD 
```

The AGCD data files are not CF compliant and are thus incompatible with the xclim library,
so the `--dataset AGCD` option triggers some metadata fixes so that xclim can process the data.


## Fit parameters

To assess the validity of return periods it is common to assess the parameters for the
probability distribution that was fitted to the data during the return period calculation.

Those parameters can be obtained using the `parameters.py` script.

Running the script at the command line with the `-h` option explains the user options:

```bash
$ /g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/parameters.py -h
```

```
usage: parameters.py [-h] [--mode {min,max}] [--time_period START_DATE END_DATE] [--distribution {genextreme,gennorm,gumbel_r,gumbel_l}]
                     [--season {DJF,MAM,JJA,SON}] [--month [MONTH ...]] [--dataset {AGCD}]
                     [infiles ...] var outfile

Command line program for obtaining probability distribution parameters.

positional arguments:
  infiles               input files
  var                   variable name
  outfile               output file name

options:
  -h, --help            show this help message and exit
  --mode {min,max}      probability of exceedance (max) or non-exceedance (min) [default=max]
  --time_period START_DATE END_DATE
                        Time period in YYYY-MM-DD format
  --distribution {genextreme,gennorm,gumbel_r,gumbel_l}
                        Name of the univariate probability distribution [default=genextreme]
  --season {DJF,MAM,JJA,SON}
                        Only process data for a given season [default is annual]
  --month [MONTH ...]   Only process data from a list of months [default is all months]
  --dataset {AGCD}      Apply dataset and variable specific metadata fixes for CF compliance
```

### Example 1

```
/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/parameters.py /g/data/wp00/QQSCALE/GFDL-ESM2M/FFDI/rcp45/2016-2045/ffdi.????.nc FFDI param_test.nc
```

