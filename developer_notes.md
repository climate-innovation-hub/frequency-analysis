# Overview

Extreme value analysis typically begins by defining a set of extreme values using
either the Block Maxima/Minima or Peaks Over/Under Threshold method:
- **Block Maxima/Minima**: extreme values are extracted by selecting a block size (typically 1 year),
then partitioning your time series data into equal consecutive blocks of this size
and taking the maximum/minimum value from each block.
This is the simpler and more common method.
- **Peaks Over/Under Threshold**: extreme values are extracted by choosing a threshold and selecting values higher or lower
(depending on which extreme process is analysed) than the threshold value.
The selected values are then de-clustered by specifying a minimum distance between adjacent clusters (e.g. 2 days),
which means that the model assumes that clusters of exceedences separated by this distance or larger are independent.

Once you've got your set of extreme values,
the next step is to fit a statistical model to those data:
- The **Generalised Extreme Value Distribution** (GEVD) family of continuous probability distributions is used for block maxima/minima.
Depending on the shape parameter, a Gumbel (shape = 0), Fréchet (shape > 0), or Weibull (shape < 0) distribution will be produced.
The location parameter represents the mean of the GEVD and the scale parameter is the multiplier that scales function (i.e. the spread/width of the distribution).
- The **Generalised Pareto Distribution** (GPD) family of continuous probability distributions is used for peaks over/under threshold.
Depending on the shape parameter, an Exponential (shape = 0), Pareto (shape > 0), or Beta (shape < 0) distribution will be produced.
The location parameter represents the reference value against which the excesses are calculated and the scale parameter is the multiplier that scales function (i.e. the spread/width of the distribution).

The fitting itself is performed using either **Maximum Likelihood Estimation** (MLE) or
**Probability Weighted Moments** (PWM; otherwise known as l-moments).

# Software

In terms of Python packages for performing extreme value analysis,
**xclim** has basic [frequency analysis](https://xclim.readthedocs.io/en/stable/notebooks/frequency_analysis.html)
functionality that scales to large arrays using xarray and dask.
It's presumably built upon the [scipy genextremes](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.genextreme.html) library.
What's missing from xclim is any functionality for assessing extreme value analysis,
such as goodness of fit metrics, confidence intervals,
[QQ-plots](https://en.wikipedia.org/wiki/Q%E2%80%93Q_plot) or
[PP-plots](https://en.wikipedia.org/wiki/P%E2%80%93P_plot).
Much of this is included with [pyextremes](https://georgebv.github.io/pyextremes/),
but it's really only setup to handle a single time series (as opposed to gridded data)
so it's necessary to extract a grid point of interest.

There are a bunch of other libraries that look either incomplete or no longer supported such as
[scikit-extremes](https://scikit-extremes.readthedocs.io/),
[evt](https://evt.readthedocs.io) and
[thresholdmodeling](https://github.com/iagolemos1/thresholdmodeling).
It doesn't appear the [extremes value functionality from NCL](https://www.ncl.ucar.edu/Document/Functions/extval.shtml)
has been implemented in geocat-comp.
