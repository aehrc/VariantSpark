package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.IndexedSplitAggregator;
import au.csiro.variantspark.algo.RegressionSplitAggregator;
import au.csiro.variantspark.algo.SplitInfo;
import au.csiro.variantspark.algo.SubsetSplitInfo;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;

/**
 * Slow fallback splitter for nominal (categorical) variables in regression trees.
 *
 * Levels are sorted by their mean response value (ascending) — provably optimal for
 * MSE (Fisher 1958). For each prefix of the sorted ordering a full O(N) rescan is
 * performed, analogous to {@link JNominalIndexedSplitter} for classification.
 *
 * Selected by {@link au.csiro.variantspark.algo.ThresholdIndexedSplitter} only when
 * the subset is sparse (N / L < qThreshold), so N is small.
 */
public class JNominalRegressionIndexedSplitter extends AbstractIndexedSplitterBase {
    private final byte[] data;
    private final int nLevels;
    private final double[] targetValues;

    public JNominalRegressionIndexedSplitter(IndexedSplitAggregator impurityCalc,
            byte[] data, int nLevels) {
        super(impurityCalc);
        this.data = data;
        this.nLevels = nLevels;
        this.targetValues = ((RegressionSplitAggregator) impurityCalc).values();
    }

    @Override
    public SplitInfo doFindSplit(int[] splitIndices) {

        // O(N): compute per-level sum and count to derive means for sorting.
        double[] levelSumY = new double[nLevels];
        int[] levelCounts = new int[nLevels];
        for (int i : splitIndices) {
            int level = data[i] & 0xFF;
            levelSumY[level] += targetValues[i];
            levelCounts[level]++;
        }

        // Collect non-empty (active) levels and build bitmask.
        int nActive = 0;
        int[] activeLevels = new int[nLevels];
        long activeMask = 0L;
        for (int l = 0; l < nLevels; l++) {
            if (levelCounts[l] > 0) {
                activeLevels[nActive++] = l;
                activeMask |= (1L << l);
            }
        }

        if (nActive < 2) {
            return null;
        }

        // Canonical form: highest active level always stays in Right.
        int highestActiveLevel = activeLevels[nActive - 1];

        // Sort active levels by ascending mean response.
        double[] means = new double[nActive];
        int[] order = new int[nActive];
        for (int j = 0; j < nActive; j++) {
            int l = activeLevels[j];
            means[j] = levelSumY[l] / levelCounts[l];
            order[j] = j;
        }
        DoubleArrays.quickSortIndirect(order, means);

        // For each prefix: O(N) rescan to evaluate the split (slow path).
        SplitInfo result = null;
        double minImpurity = Double.MAX_VALUE;
        long leftMask = 0L;

        for (int j = 0; j < nActive - 1; j++) {
            int level = activeLevels[order[j]];
            leftMask |= (1L << level);

            impurityCalc.init(splitIndices);
            for (int i : splitIndices) {
                int sampleLevel = data[i] & 0xFF;
                if ((leftMask & (1L << sampleLevel)) != 0) {
                    impurityCalc.update(i);
                }
            }

            if (impurityCalc.hasProperSplit()) {
                double thisImpurity = impurityCalc.getValue(leftRightImpurity);
                if (thisImpurity < minImpurity) {
                    long finalMask = leftMask;
                    double leftImp = leftRightImpurity.left();
                    double rightImp = leftRightImpurity.right();
                    if ((finalMask & (1L << highestActiveLevel)) != 0) {
                        finalMask = activeMask ^ finalMask;
                        double tmp = leftImp;
                        leftImp = rightImp;
                        rightImp = tmp;
                    }
                    result = new SubsetSplitInfo(finalMask, thisImpurity, leftImp, rightImp);
                    minImpurity = thisImpurity;
                }
            }
        }
        return result;
    }
}
