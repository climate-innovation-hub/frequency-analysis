# Frequency analysis

This directory contains command line programs for frequency analysis.

The programs make use of the frequency analysis capability built into the xclim library,
which is described at:  
https://xclim.readthedocs.io/en/stable/notebooks/frequency_analysis.html

## Environment

If you're a member of the `wp00` project on NCI
(i.e. if you're part of the CSIRO Climate Innovation Hub),
the easiest way to use the scripts in this directory is to use the cloned copy at `/g/data/wp00/shared_code/frequency-analysis/`.
They can be run using the Python environment at `/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python`.

If you'd like to run the scripts in your own Python environment,
you'll need to install the following libraries using conda...
```
conda install -c conda-forge xclim cmdline_provenance
```
... and then the following with the python package installer:
```
$ pip install gitpython
$ pip install git+https://github.com/OpenHydrology/lmoments3.git@develop#egg=lmoments3
```

## Extreme value analysis

The `eva.py` script performs extreme value analysis using the block maxima method.

Running the script at the command line with the `-h` option explains the user options:

```bash
$ /g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/eva.py -h
```

```
usage: eva.py [-h] --extreme_values [EXTREME_VALUES ...] [--mode {min,max}] [--distribution {genextreme,gennorm,gumbel_r,gumbel_l}]
              [--fit_method {ML,PWM}] [--time_period START_DATE END_DATE] [--time_freq TIME_FREQ]
              [--drop_edge_times {neither,first,last,both}] [--season {DJF,MAM,JJA,SON}] [--month [MONTH ...]] [--dataset {AGCD}]
              [--memory_vis] [--verbose] [--local_cluster] [--dask_dir DASK_DIR] [--nworkers NWORKERS] [--lon_chunk_size LON_CHUNK_SIZE]
              [infiles ...] var {ari,aep,quantile} outfile

Command line program for conducting extreme value analysis using the block maxima method.

positional arguments:
  infiles               input files
  var                   variable name
  {ari,aep,quantile}    type of extreme to return (annual return interval: ari; annual exceedance probability: aep; or quantile)
  outfile               output file name

options:
  -h, --help            show this help message and exit
  --extreme_values [EXTREME_VALUES ...]
                        extreme values to return (i.e. ari, aep or quantile values) [required]
  --mode {min,max}      probability of exceedance (max) or non-exceedance (min) [default=max]
  --distribution {genextreme,gennorm,gumbel_r,gumbel_l}
                        Name of the univariate probability distribution [default=genextreme]
  --fit_method {ML,PWM}
                        Fitting method, either maximum likelihood (ML) or probability weighted moments (PWM; aka l-moments) [default=ML]
  --time_period START_DATE END_DATE
                        Time period in YYYY-MM-DD format
  --time_freq TIME_FREQ
                        Time frequency for blocks (e.g. A-JUN is a July-June year) [default is calendar year]
  --drop_edge_times {neither,first,last,both}
                        Drop first and/or last time step after block aggregation (e.g. for A-JUN the first and last year might be incomplete)
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

By default,
the script will fit a GEV distribution to the (annual) block maxima of your data using the maximum likelihood fitting method.
You simply need to specify the type of extreme you're looking for (i.e. annual return interval, annual exceedance probability or quantile)
and which values you'd like returned (using the `--extreme_values` option).
The processing will then be done on a single core.
Essentially all aspects of the analysis (e.g. fitting method, distribution, blocks) and processing (i.e. parallel processing using dask)
can be customised using the options listed above.

### Example 1

FFDI data could be processed to calculate the 20 year return period as follows:

```
/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/eva.py /g/data/wp00/QQSCALE/GFDL-ESM2M/FFDI/rcp45/2016-2045/ffdi.????.nc FFDI ari ffdi_ARI_20yr_GFDL-ESM2M-QME_rcp45_2016-2045_A-JUN.nc --time_freq A-JUN --drop_edge_times both --extreme_values 20 --verbose --lon_chunk_size 5 --local_cluster --dask_dir /g/data/wp00/users/dbi599/
```

Instead of using calendar years for each block,
the `--time_freq` option (with value `A-JUN`) has been used to select blocks that run from July to June.
The first and last time step will be incomplete (i.e. they'll only have 6 months of input data)
so the `--drop_edge_times` option (with value `both`) has been used to drop
the first and last time step after block aggregation.

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

/g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/eva.py /g/data/wp00/QQSCALE/GFDL-ESM2M/FFDI/rcp45/2016-2045/ffdi.????.nc FFDI ari /g/data/wp00/users/dbi599/test_space/ffdi_ARI_20yr_GFDL-ESM2M-QME_rcp45_2016-2045_A-JUN.nc --time_freq A-JUN --drop_edge_times both --extreme_values 20 --verbose --lon_chunk_size 5 --local_cluster --dask_dir /g/data/wp00/users/dbi599/
```

### Example 2

AGCD precipitation data could be processed to calculate the 20%, 10% and 5% annual exceedance probabilities as follows:
```
$ /g/data/wp00/users/dbi599/miniconda3/envs/cih/bin/python /g/data/wp00/shared_code/frequency-analysis/eva.py /g/data/zv2/agcd/v1/precip/total/r005/01month/agcd_v1_precip_total_r005_monthly_197* precip aep /g/data/wp00/users/dbi599/precip_aep_AGCD_1970-1979.nc --extreme_values 20 10 5 --dataset AGCD 
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

