import re
import pandas as pd
from varspark.stats.lfdr import *


class LocalFdrVs:
    local_fdr: object
    df_: object

    def __init__(self, df):
        """
        Constructor class
        :param df: Takes a pandas dataframe as argument with three columns: variant_id,
        logImportance and splitCount.
        """
        self.df_ = df.sort_values("logImportance", ascending=True)

    @classmethod
    def from_imp_df(cls, df):
        """
        Alternative class instantiation from a pandas dataframe
        :param cls: LocalFdrVs class
        :param df: Pandas dataframe with columns locus, alleles, importance, and splitCount.
        :return: Initialized class instance.
        """
        df = df[df["splitCount"] >= 1]
        df = df.assign(logImportance=np.log(df.importance))
        # df['variant_id'] = df.apply(
        #    lambda row: str(row['locus'][0]) + '_' + str(row['locus'][1]) + '_' + \
        #                str('_'.join(row['alleles'])), axis=1)
        return cls(df[["variant_id", "logImportance", "splitCount"]])

    def find_split_count_th(
        self, cutoff_list=[1, 2, 3, 4, 5, 10, 15, 20], quantile=0.75, bins=120
    ):
        """
        Finds the ideal threshold for the splitCount. Ideal being the lowest differences between
        the fitted skewed normal distribution vs the real data
        :param cutoff_list: List of all values to be tried for the cutoff in the splitCount
        :param quantile: Quantile to evaluate the distribution
        :param bins: Number of bins for the distribution
        :return: best splitCount threshold
        """
        best_split = [1, np.inf]

        for split in cutoff_list:
            # impDfWithLog = self.df_[self.df_.splitCount >= split]
            impDfWithLog = self.df_[self.df_.splitCount >= split]  # temp
            impDfWithLog = (
                impDfWithLog[["variant_id", "logImportance"]]
                .set_index("variant_id")
                .squeeze()
            )

            local_fdr = LocalFdr()
            local_fdr.bins = bins

            impDfWithLog = impDfWithLog + sys.float_info.epsilon
            x, f_observed_y = local_fdr._observed_density(impDfWithLog)
            f_y = local_fdr._fit_density(x, f_observed_y)

            C = np.quantile(impDfWithLog, q=quantile)

            initial_f0_params = local_fdr._estimate_skewnorm_params(
                x[x < C], f_observed_y[x < C], SkewnormParams.initial_list(impDfWithLog)
            )

            res = (
                skewnorm.pdf(
                    x,
                    a=initial_f0_params.a,
                    loc=initial_f0_params.loc,
                    scale=initial_f0_params.scale,
                )
                - f_observed_y
            )
            res = sum(res[x < C] ** 2)
            if best_split[1] > res:
                best_split[0] = split
                best_split[1] = res

        return best_split[0]

    def plot_log_densities(
        self,
        ax,
        cutoff_list=[1, 2, 3, 4, 5, 10, 15, 20],
        palette="Set1",
        find_automatic_best=False,
        xLabel="log(importance)",
        yLabel="density",
    ):
        """
        Plotting the log densities to visually identify the unimodal distributions.
        :param ax: Matplotlib axis as a canvas for this plot.
        :param cutoff_list: list of potential splitCount thresholds
        :param find_automatic_best: The user may let the computer highlight the potential best option.
        :param palette: Matplotlib color palette used for the plotting.
        :param xLabel: Label on the x-axis of the plot.
        :param yLabel: Label on the y-axis of the plot.
        """
        assert type(palette) == str, "palette should be a string"
        assert type(xLabel) == str, "xLabel should be a string"
        assert type(yLabel) == str, "yLabel should be a string"

        n_lines = len(cutoff_list)
        colors = sns.mpl_palette(palette, n_lines)
        df = self.df_
        for i, c in zip(cutoff_list, colors):
            sns.kdeplot(
                df.logImportance[df.splitCount >= i], ax=ax, c=c, bw_adjust=0.5
            )  # bw low show sharper distributions

        if find_automatic_best:
            potential_best = self.find_split_count_th(cutoff_list=cutoff_list)
            sns.kdeplot(
                df.logImportance[df.splitCount >= potential_best],
                ax=ax,
                c=colors[potential_best - 1],
                bw_adjust=0.5,
                lw=8,
                linestyle=":",
            )
            best_split = [
                str(x) if x != potential_best else str(x) + "*" for x in cutoff_list
            ]
        else:
            best_split = cutoff_list

        ax.legend(title="Minimum split counts in distribution")
        ax.legend(labels=best_split, bbox_to_anchor=(1, 1))
        ax.set_xlabel(xLabel)
        ax.set_ylabel(yLabel)

    def plot_log_hist(
        self, ax, split_count, bins=120, xLabel="log(importance)", yLabel="count"
    ):
        """
        Ploting the log histogram for the chosen split_count
        :param ax: Matplotlib axis as a canvas for this plot.
        :param split_count: Minimum split count threshold for the plot.
        :param bins: Number of bins in the histogram
        :param xLabel: Label on the x-axis of the plot.
        :param yLabel: Label on the y-axis of the plot.
        """

        assert bins > 0, "bins should be bigger than 0"
        assert split_count > 0, "split_count should be bigger than 0"
        assert type(xLabel) == str, "xLabel should be a string"
        assert type(yLabel) == str, "yLabel should be a string"

        df = self.df_
        sns.histplot(df.logImportance[df.splitCount >= split_count], ax=ax, bins=bins)
        ax.set_xlabel(xLabel)
        ax.set_ylabel(yLabel)

    def plot(self, ax):
        self.local_fdr.plot(ax)

    def compute_fdr(self, countThreshold=2, local_fdr_cutoff=0.05, bins=120):
        """
        Compute the FDR and p-values of the SNPs.
        :param countThreshold: The split count threshold for the SNPs to be considered.
        :param local_fdr_cutoff: Threshold of False positives over total of genes
        :param bins: number of bins to which the log importances will be aggregated
        :return: A tuple with a dataframe containing the SNPs and their p-values,
                    and the expected FDR for the significant genes.
        """

        assert countThreshold > 0, "countThreshold should be bigger than 0"
        assert (
            0 < local_fdr_cutoff < 1
        ), "local_fdr_cutoff threshold should be between 0 and 1"

        self.local_fdr_cutoff = local_fdr_cutoff

        impDfWithLog = self.df_[self.df_.splitCount >= countThreshold]
        impDfWithLog = (
            impDfWithLog[["variant_id", "logImportance"]]
            .set_index("variant_id")
            .squeeze()
        )

        self.local_fdr = LocalFdr()
        self.local_fdr.fit(impDfWithLog, bins)
        pvals = self.local_fdr.get_pvalues()
        fdr, cutoff_pvalue, mask = self.local_fdr.get_fdr(local_fdr_cutoff)
        self.fdr = fdr
        self.cutoff_pvalue = cutoff_pvalue
        self.pvalsDF = impDfWithLog.reset_index().assign(
            pvalue=pvals, is_significant=mask
        )
        return (self.pvalsDF, fdr)

    def plot_manhattan_imp(self, gap_size=None):
        """Displays manhattan plot of negative log importances for each feature, as well as significance cutoff.
            Categorises features in respective chromosomes, ordered by locus.
        :param gap_size: The size of gap between each chromosome.
            Included as an adjustable parameter as this value scales with the total number of loci
        """
        pvals = self.pvalsDF
        cutoff_pvalue = self.cutoff_pvalue
        # Estimate appropriate size for gap between chromosomes based on number of loci to plot
        gap_size = (
            gap_size if gap_size is not None else int(np.ceil(pvals.shape[0] / 80))
        )

        def process_variant_id(variant_id):
            """Extracts chromosome, locus, and alleles from the variant_id field using regex
            :param variant_id: Feature label
            """
            pattern = r"(\d+)_([\d]+)_([A-Z]*)_([A-Z]*)"

            match = re.match(pattern, variant_id)

            if match:
                chrom = match.group(1)
                locus = match.group(2)
                alleles = [match.group(3), match.group(4)]
                return pd.Series(
                    [int(chrom), int(locus), alleles],
                    index=["chrom", "locus", "alleles"],
                )
            else:
                return pd.Series(
                    [None, None, None], index=["chrom", "locus", "alleles"]
                )

        pvals[["chrom", "locus", "alleles"]] = pvals["variant_id"].apply(
            process_variant_id
        )
        pvals = pvals.dropna(thresh=1)
        pvals["-logp"] = -np.log10(pvals.pvalue)
        sorted_pvals = pvals.sort_values(by=["chrom", "locus"])
        sorted_pvals.reset_index(inplace=True, drop=True)
        sorted_pvals["i"] = sorted_pvals.index
        sorted_pvals["chrom"] = sorted_pvals["chrom"].astype("category")
        sorted_pvals["chrom_idx"] = sorted_pvals["chrom"].cat.codes.astype(int)
        sorted_pvals["x"] = sorted_pvals["chrom_idx"] * gap_size + sorted_pvals["i"]
        plot = sns.relplot(
            data=sorted_pvals,
            x="x",
            y="-logp",
            aspect=3.7,
            linewidth=0,
            hue="chrom",
            palette="bright",
            legend=None,
        )
        cutoff_logp = -np.log10(cutoff_pvalue)
        plot.ax.axhline(y=cutoff_logp, color="gray", linestyle="--")
        plot.ax.text(
            plot.ax.get_xlim()[1] + 0.1, cutoff_logp, f"Cutoff = {cutoff_pvalue:.6f}"
        )
        chrom_df = sorted_pvals.groupby("chrom")["x"].median()
        plot.ax.set_xlabel("chrom")
        plot.ax.set_xticks(chrom_df)
        plot.ax.set_xticklabels(chrom_df.index)
        plot.figure.suptitle("Manhattan plot of p values")
        return sorted_pvals
