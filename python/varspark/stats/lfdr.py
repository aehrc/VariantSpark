import functools as ft

import numpy as np
import patsy
import scipy
import statsmodels.api as sm
from scipy.stats import skewnorm
import sys
import seaborn as sns


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



class LocalFdr:
    """
    This is mostly based on the ideas from this Effrom paper"
    https://efron.ckirby.su.domains//papers/2005LocalFDR.pdf

    z - Inpute data
    x - Breakpoints from the histogram
    f_observed_y - Observed density of the data
    f_y - Fitted density
    C - Cutoff between the f0 and the significant
    f0_params - Parameter estimates for the background distribution
    f0_y - Observed distribution based on function estimates
    p0 - the proportion of the null observations
    local_fdr - FDR array for each position
    """
    z: np.array
    x: np.array
    f_observed_y: np.array
    f_y: np.array
    C: np.float64
    f0_params: object
    f0_y: np.array
    p0: np.float64
    local_fdr: np.array


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


    def _local_fdr(self, f, f0, p0=1):
        """
        Computes the local fdr values.
        :param f: The fitted splines
        :param f0: Probability distribution from the fitted data
        :param p0: Maximum pvalue allowed
        :return: Array of the same length as x of FDR corrected p-values
        """
        f_normalised = (np.sum(f0) * f) / np.sum(f)
        return np.minimum((p0 * f0) / f_normalised, 1)


    def _estimate_skewnorm_params(self, x, y, initial_params_list=SkewnormParams.default(),
                                  max_nfev=400):
        """
        Estimate the best parameters for the observed function
        :param x: x-axis values
        :param y: y-axis values
        :param initial_params_list: Starting initial parameters
        :param max_nfev: Maximum number of function evaluations before the termination.
        :return: Returns SkewnormParams class with the found best fitted parameters
        """
        def _fit_skew_normal(initial_params):
            return scipy.optimize.least_squares(
                lambda p, x, y: skewnorm.pdf(x, a=p[0], loc=p[1], scale=p[2]) - y,
                # skewnorm.pdf residuals
                x0=np.array(initial_params),
                args=[x, y],
                method='lm', max_nfev=max_nfev
            )

        def _has_converged(optimisation_result):
            return optimisation_result and optimisation_result.success and optimisation_result.cost != 0

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
        :param z: Input values
        """
        self.z = z + sys.float_info.epsilon
        self.x, self.f_observed_y = self._observed_density(self.z)
        self.f_y = self._fit_density(self.x, self.f_observed_y)
        #
        # Estimate the tentative of the null distribution (skew-normal)
        # from the normalised histogram data.
        #
        initial_f0_params = self._estimate_skewnorm_params(self.x, self.f_observed_y,
                                                           SkewnormParams.initial_list(z))

        self.C = skewnorm.ppf(0.95, **initial_f0_params._asdict())
        self.f0_params = self._estimate_skewnorm_params(self.x[self.x < self.C],
                                                        self.f_observed_y[self.x < self.C])


        self.f0_y = skewnorm.pdf(self.x, **self.f0_params._asdict())
        self.p0 = self._estimate_p0(skewnorm.cdf(z, **self.f0_params._asdict()))

        self.local_fdr = self._local_fdr(self.f_y, self.f0_y, self.p0)


    def get_pvalues(self):
        """
        Returns the p-values for all elements
        :return: Returns the p-values for each of the elements within the array
        """
        return 1 - skewnorm.cdf(self.z, **self.f0_params._asdict())


    def get_fdr_cutoff(self, pvalue=0.05):
        """
        Returns the FDR corrected p-value threshold
        :param pvalue: Selected threshold for the significant genes
        :return: Returns the corrected p-value threshold
        """
        start_x = scipy.stats.skewnorm.ppf(0.95, **self.f0_params._asdict())

        start_x = np.where(self.x > start_x)[0][0]

        ww = np.argmin(np.abs(self.local_fdr.iloc[start_x:119] - pvalue))
        mask = self.z > self.x[ww+start_x]

        ppp_sg = self.get_pvalues()[mask]
        cut = self.get_pvalues()[ww+start_x]
        print(self.get_pvalues())
        print(cut,len(self.z),len(ppp_sg))
        return cut*len(ppp_sg)/len(self.z)


    def plot(self, ax):
        """
        Returns the built canvas for a sanity check.
        :param ax: Mataplot axis
        :return:
        """
        sns.histplot(self.z, ax=ax, stat='density', bins=120, color='purple', label="Binned "
                                                                                    "importances")
        ax.plot(self.x, skewnorm.pdf(self.x,  **self.f0_params._asdict()), color='red',
                label='fitted curve')

        ax.axvline(x=self.C, color='blue', label="C")
        #ax.axvline(x=self.get_fdr_cutoff(), color='lime', label="FDR cutoff 0.05")
        ax.axhline(y=0.05, color='black', label="p-value 0.05")
        ax.set_xlabel("importances", fontsize=14)
        ax.set_ylabel("density", fontsize=14)

        ax.plot(np.nan, np.nan, color='blue', label = 'fdr') #Adding to the legend
        ax2=ax.twinx()
        ax2.set_ylabel("local FDR", fontsize=14)
        ax2.plot(self.x, self.local_fdr, color='blue')

        ax.legend(loc="upper right")
