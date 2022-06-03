import functools as ft
from typing import NamedTuple

import numpy as np
import patsy
import scipy
import statsmodels.api as sm
from scipy.stats import skewnorm
import sys


class SkewnormParams(NamedTuple):
    a: np.float64
    loc: np.float64
    scale: np.float64

    @classmethod
    def from_data(cls, z):
        return SkewnormParams._make(skewnorm.fit(z))

    @classmethod
    def from_mean(cls, z, a=1, scale=2):
        return SkewnormParams(a, loc=np.mean(z), scale=scale)

    @classmethod
    def default(cls, a=1, loc=1, scale=2):
        return SkewnormParams(a, loc, scale)

    @classmethod
    def initial_list(cls, z, a=1, loc=1, scale=2):
        return [
            cls.default(a, loc, scale),
            cls.from_mean(z, a, scale),
            cls.from_data(z)
        ]


    
    
#TODO: Descriptions
#TODO: enable parameters passing to the hidden functions
#TODO: Ensure logic is correct
class LocalFdr(NamedTuple):
    """
    This is mostly based on the ideas from this Effrom paper"
    https://efron.ckirby.su.domains//papers/2005LocalFDR.pdf

    f_observed - actual PDF for distribution of observations `z` (from histogram)
    f - estimated (smoothed) PDF for distribution of observations `z`
    f0 - PDF of the null distribution
    p0 - the proportion of the null observations
    """
    x: np.array
    f_observed_y: np.array
    f_y: np.array
    C: np.float64
    f0_params: object
    f0_y: np.array
    p0: np.float64
    local_fdr: np.array

    #No need for this
    def __init__(self):
        pass
        
        
    def _observed_density(self, z, num_bins=120):
        """
        Groups the data into bins to create a density distribution.
        :param z: Input data (pandas Series)
        :param num_bins: Number of bins to aggregate the data
        :return: returns the mid points of the bins and the density data
        """
        z_density, breaks = np.histogram(z, bins=num_bins, density=True)
        breaks_mid_points = (breaks[:-1] + breaks[1:]) / 2
        return breaks_mid_points, z_density


    def _fit_density(self, x, y, df=10):
        """
        Fits the data using a poison function using splines
        :param x: Mid points of the bins from the density distribution
        :param y: Density distribution
        :param df: Number of degrees of freedom for the splines
        :return: returns the smoothed data for the density distribution
        """
        transformed_x = patsy.dmatrix(f"cr(x, df={df})", {"x": x}, return_type='dataframe')
        transformed_x = sm.add_constant(transformed_x)  # Makes no difference but added for consistency
        model = sm.GLM(y, transformed_x, family=sm.families.Poisson()).fit()
        return model.predict(transformed_x)


    def _estimate_p0(self, p):
        """
        Proportion of true null hypothesis
        :param p: Probabilities of the importances
        :return: Minimum p-value
        """
        n = len(p)
        i = np.array(range(1, n + 1))[::-1]
        p = np.sort(p)[::-1]
        q = np.minimum(n / i * p, 1)
        n1 = n + 1
        return np.sum(i * q) / n / n1 * 2


    #TODO
    def _local_fdr(self, f, f0, p0=1):
        """
        Computes the local fdr values.
        :param f: A dictionary containing the fitted splines (from _ff_fit)
        :param x: Array with the importance values
        :param estimates: Array length==3 containing location, scale and skewness (xi, omega, and
            lambda) parameters to compute the density distribution
        :param FUN: Function to compute the probability density. The default is the skew-normal distribution
        :param p0: Maximum pvalue allowed
        :return: Array of the same length as x of FDR corrected p-values
        """
        f_normalised = (np.sum(f0) * f) / np.sum(f)
        return np.minimum((p0 * f0) / f, 1)


    #TODO
    def _estimate_skewnorm_params(self, x, y, initial_params_list=SkewnormParams.default(),
                                  max_nfev=400):
        def _fit_skew_normal(initial_params):
            return scipy.optimize.least_squares(
                lambda p, x, y: skewnorm.pdf(x, a=p[0], loc=p[1], scale=p[2]) - y,
                # skewnorm.pdf residuals
                x0=np.array(initial_params),
                args=[x, y],
                method='lm', max_nfev=max_nfev
            )

        #TODO: Success may be missleading. Do old style?
        def _has_converged(optimisation_result):
            return optimisation_result and optimisation_result.success

        if isinstance(initial_params_list, SkewnormParams):
            initial_params_list = [initial_params_list]

        converged_results = filter(_has_converged, map(_fit_skew_normal, initial_params_list))
        if converged_results:
            best_result = ft.reduce(lambda r1, r2: r2 if r2.cost < r1.cost else r1, converged_results)
            return SkewnormParams._make(best_result.x)
        else:
            raise ValueError('All fittings failed')


    def fit(self, z):
        """
        Core function to estimate the f0, and the local fdr
        :param z: Input values, pandas Series
        """
        z = z + sys.float_info.epsilon
        self.x, self.f_observed_y = self._observed_density(z)
        self.f_y = self._fit_density(self.x, self.f_observed_y)
        #
        # Estimate the tentative of the null distribution (skew-normal)
        # from the normalised histogram data.
        #
        initial_f0_params = self._estimate_skewnorm_params(self.x, self.f_observed_y,
                                                           SkewnormParams.initial_list(z))
        self.C = skewnorm.ppf(0.95, **self.initial_f0_params._asdict())
        self.f0_params = self._estimate_skewnorm_params(self.x[self.x < self.C],
                                                        self.f_observed_y[self.x < self.C],
                                                        initial_f0_params)

        self.f0_y = skewnorm.pdf(self.x, **self.f0_params._asdict())
        self.p0 = self._estimate_p0(skewnorm.cdf(z, **self.f0_params._asdict()))

        self.local_fdr = self._local_fdr(self.f_y, self.f0_y, self.p0)


    def get_pvalues(self, z):
        return 1 - skewnorm.cdf(z, **self.f0_params._asdict())
    
    #TODO
    def get_fdr_cutoff(self, pvalue=0.05):
        start_x = np.argmin(np.abs(obj['x'] - np.mean(imp1)))
        ww = np.argmin(np.abs(obj['fdr'][start_x:119] - cutoff))
        num_sig_genes = np.sum(imp1 > obj['x'].iloc[ww+start_x])
        a1 = imp1 > obj['x'].iloc[ww+start_x]
        ppp_sg = 1-scipy.stats.skewnorm.cdf(imp1[a1], loc=obj['estimates'][0], scale=obj['estimates'][1],
                                   a=obj['estimates'][2])
        cut = 1-scipy.stats.skewnorm.cdf(obj['x'].iloc[ww+start_x], loc=obj['estimates'][0], scale=obj['estimates'][1],
                                   a=obj['estimates'][2])

        FDR = cut*len(imp1)/len(ppp_sg)

    #TODO
    def plot(self, ax):
        sns.histplot(impDfWithLog, ax = ax, stat='density', bins=120, color='purple', label="Binned importances")
        ax.plot(temp['x'],self._my_dsn(temp['estimates'], temp['x']),color='red', label='fitted curve')

        ax.axvline(x=temp['C'], color='blue', label="C")
        ax.axvline(x=temp['cc'], color='green', label="cc")
        ax.axvline(x=temp['q95'], color='purple', label="95% quantile")
        ax.axvline(x=positional_cut, color='lime', label="FDR cutoff")
        ax.axhline(y=fdr_cutoff, color='black', label="p-value")
        ax.set_xlabel("importances", fontsize=14)
        ax.set_ylabel("density",fontsize=14)

        ax.scatter(np.nan, np.nan, color='blue', label = 'fdr') #Adding to the legend
        ax2=ax.twinx()
        ax2.set_ylabel("local FDR",fontsize=14)
        ax2.scatter(temp['zh1'][:-1],temp['fdr'], color='blue', label="fdr")

        ax.legend(loc="upper right")
