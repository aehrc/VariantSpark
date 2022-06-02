import functools as ft
from typing import NamedTuple

import numpy as np
import patsy
import scipy
import statsmodels.api as sm
from scipy.stats import skewnorm


class LocalFdrVs(NamedTuple):
    local_fdr: object
    _df: object
        
    def __init__(self, df):
        """
        Constructor class
        :param p: Takes a pandas dataframe as argument with three columns: variant_id, logImportance and splitCount.
        """
        
        self._df = df.sort_values('logImportance', ascending=False)

    
    @classmethod
    def from_imp_table(cls,impTable):
        """
        Alternative class instantiation from a Hail Table (VariantSpark users).
        :param cls: FdrCalculator class
        :param impTable: Hail table with locus, alleles, importance, and splitCount.
        :return: Initialize class instance. 
        """
        
        impTable = impTable.filter(impTable.splitCount >= 1) 
        impDf  = impTable.to_spark(flatten=False).toPandas()
        df = impDf.assign(logImportance = np.log(impDf.importance))
        df['variant_id'] = df.apply(lambda row: str(row['locus'][0])+'_'+str(row['locus'][1])+'_'+\
                                    str('_'.join(row['alleles'])), axis=1)
        return cls(df)
    
    def plot_log_densities(self, ax, min_split_count = 1, max_split_count=6, find_automatic_best=False, 
                           palette = 'Set1', xLabel = 'log(importance)', yLabel = 'density'):
        """
        Plotting the log densities to visually identify the unimodal distributions.
        :param ax: Matplotlib axis as a canvas for this plot.
        :param min_split_count: n>=1, from which the split count plotting starts.
        :param max_split_count: when to stop the split count filtering.
        :param find_automatic_best: The user may let the computer highlight the potential best option.
        :param palette: Matplotlib color palette used for the plotting.
        :param xLabel: Label on the x-axis of the plot.
        :param yLabel: Label on the y-axis of the plot.
        """
        
        assert min_split_count < max_split_count, 'min_split_count should be smaller than max_split_count'
        assert min_split_count > 0, 'min_split_count should be bigger than 0'
        assert type(palette) == str, 'palette should be a string'
        assert type(xLabel) == str, 'xLabel should be a string'
        assert type(yLabel) == str, 'yLabel should be a string'
        
        n_lines = max_split_count - min_split_count + 1
        colors= sns.mpl_palette(palette, n_lines)
        df = self._df
        for i,c in zip(range(min_split_count, max_split_count + 1), colors):
            sns.kdeplot(df.logImportance[df.splitCount >= i],
                        ax = ax, c=c, bw_adjust=0.5) #bw low show sharper distributions
        
        best_split = list(range(min_split_count,max_split_count+1))
        
        if find_automatic_best:
            potential_best = self.find_split_count_th( min_split_count, max_split_count)
            sns.kdeplot(df.logImportance[df.splitCount >= potential_best],
                            ax = ax, c=colors[potential_best-1], bw_adjust=0.5, lw=8, linestyle=':')
            best_split = [str(x) if x!=potential_best else str(x)+'*' for x in range(min_split_count,max_split_count+1)]
        
        ax.legend(title='Minimum split counts in distribution')
        ax.legend(labels=best_split, bbox_to_anchor=(1,1))
        ax.set_xlabel(xLabel)
        ax.set_ylabel(yLabel)


    def plot_log_hist(self, ax, split_count, bins = 100,
                          xLabel = 'log(importance)', yLabel = 'count'):
        """
        Ploting the log histogram for the choosen split_count
        :param ax: Matplotlib axis as a canvas for this plot.
        :param split_count: Minimum split count threshold for the plot.
        :param bins: Number of bins in the histogram
        :param xLabel: Label on the x-axis of the plot.
        :param yLabel: Label on the y-axis of the plot.
        """
        
        assert bins > 0, 'bins should be bigger than 0'
        assert split_count > 0, 'split_count should be bigger than 0'
        assert type(xLabel) == str, 'xLabel should be a string'
        assert type(yLabel) == str, 'yLabel should be a string'
        
        df = self._df
        sns.histplot(df.logImportance[df.splitCount >= split_count], ax = ax, bins=bins)
        ax.set_xlabel(xLabel)
        ax.set_ylabel(yLabel)
        
        
    # WHAT TODO
    def find_split_count_th(self, min_split_count = 1, max_split_count=6, ntrials=1000):
        """
        
        :param min_split_count: Minimum split count threshold to be tested.
        :param max_split_count: Maximum split count threshold to be tested.
        :param ntrials: Number of trials for each threshold tested.
        :return: an integer with the assumed best threshold for the split count.
        """
        
        assert min_split_count < max_split_count, 'min_split_count should be smaller than max_split_count'
        assert min_split_count > 0, 'min_split_count should be bigger than 0'
        assert ntrials > 0, 'min_split_count should be bigger than 0'
        
        df = self._df
        for splitCountThreshold in range(min_split_count,max_split_count + 1):
            dat = np.msort(df[df['splitCount']>splitCountThreshold]['logImportance'])
            intervals = UniDip(dat, ntrials=ntrials).run() #ntrials can be increased to achieve higher robustness
            if len(intervals) <= 1: 
                break
        return splitCountThreshold

    #TODO
    def compute_fdr(self, countThreshold = 2, fdr_cutoff = 0.05):
        """
        Compute the FDR p-values of the significant SNPs. 
        :param countThreshold: The split count threshold for the SNPs to be considered.
        :param fdr_cutoff: the non-corrected p-value threshold
        :param ax: matplotlib axis to show the distribution and cutoffs
        :return: A tuple with a dataframe containing the SNPs and their corrected p-values, 
                    and the corrected FDR threshold.
        """

        assert countThreshold > 0, 'countThreshold should be bigger than 0'
        assert fdr_cutoff > 0 and fdr_cutoff < 1, 'fdr_cutoff should be between 0 and 1'

        impDfWithLog = self._df[self._df.splitCount >= countThreshold]
        impDfWithLog = impDfWithLog[['variant_id','logImportance']].set_index('variant_id').squeeze()
        impDfWithLog = impDfWithLog + sys.float_info.epsilon
        
        #temp = self._run_it_importances(impDfWithLog)
        #corrected_pvals,frd_result,positional_cut = self._significant_genes(temp,impDfWithLog,fdr_cutoff)
        #resultDf = impDfWithLog.head(len(corrected_pvals)).reset_index()
        
        fdr_model = LocalFdrModel.fit(impDfWithLog)
        corrected_pvals = fdr_model.get_pvalues(z)
        
        return (
            self._df.assign(
                pvalue = corrected_pvals, 
            ), 
            frd_result
        )   
    
    
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
        
        
    def _observed_density(z, num_bins=120):
        z_density, breaks = np.histogram(z, bins=num_bins, density=True)
        breaks_mid_points = (breaks[:-1] + breaks[1:]) / 2
        return breaks_mid_points, z_density


    def _fit_density(x, y, df=10):
        """

        :param x:
        :param y:
        :param df:
        :return:
        """
        transformed_x = patsy.dmatrix(f"cr(x, df={df})", {"x": x}, return_type='dataframe')
        transformed_x = sm.add_constant(transformed_x)  # Makes no difference but added for consistency
        model = sm.GLM(y, transformed_x, family=sm.families.Poisson()).fit()
        return model.predict(transformed_x)


    def _estimate_p0(p):
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


    def _local_fdr(f, f0, p0=1):
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


    def _estimate_skewnorm_params(x, y, initial_params_list=SkewnormParams.default(), max_nfev=400):
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

    @classmethod
    def fit(cls, z):
        x, f_observed_y = _observed_density(z)
        f_y = _fit_density(x, f_observed_y)
        #
        # Estimate the tentative of the null distribution (skew-normal)
        # from the normalised histogram data.
        #
        initial_f0_params = _estimate_skewnorm_params(x, f_observed_y,
                                                      SkewnormParams.initial_list(z))
        C = skewnorm.ppf(0.95, **initial_f0_params._asdict())
        f0_params = _estimate_skewnorm_params(x[x < C], f_observed_y[x < C], initial_f0_params)

        f0_y = skewnorm.pdf(x, **f0_params._asdict())
        p0 = _estimate_p0(skewnorm.cdf(z, **f0_params._asdict()))

        local_fdr = _local_fdr(f_y, f0_y, p0)
        return LocalFdr(x, f_observed_y, f_y, C, f0_params, f0_y, p0,
                             local_fdr)

    #TODO
    def get_pvalues(self, z):
        return 1 - skewnorm.cdf(z, **self.f0_params._asdict())
    
    #TODO
    def get_fdr_cutoff(pvalue=0.05):
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
        ax.bar(self.x, self.f_observed_y, color='purple', label="f norm histogram")
        ax.plot(self.x, self.f0_y, color='red', label='fitted curve')
        ax.axvline(x=self.C, color='blue', label="C")
        # ax.axvline(x=temp['cc'], color='green', label="cc")
        # ax.axvline(x=temp['q95'], color='purple', label="95% quantile")
        # ax.axvline(x=positional_cut, color='lime', label="FDR cutoff")
        # ax.axhline(y=fdr_cutoff, color='black', label="p-value")
        ax.set_xlabel("importances", fontsize=14)
        ax.set_ylabel("density", fontsize=14)

        # ax.scatter(np.nan, np.nan, label = 'fdr') #Adding to the legend
        # ax2=ax.twinx()
        # ax2.set_yticks([])
        ax.plot(self.x, self.local_fdr, label="fdr")
        ax.legend(loc="upper right")
