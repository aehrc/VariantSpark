from varspark.stats.lfdr import *

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
        :return: Initialized class instance.
        """

        impTable = impTable.filter(impTable.splitCount >= 1)
        impDf  = impTable.to_spark(flatten=False).toPandas()
        df = impDf.assign(logImportance = np.log(impDf.importance))
        df['variant_id'] = df.apply(lambda row: str(row['locus'][0])+'_'+str(row['locus'][1])+'_'+ \
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

        #TODO: put the three loops in one
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