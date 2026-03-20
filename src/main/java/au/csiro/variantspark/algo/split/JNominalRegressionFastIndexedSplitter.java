package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.IndexedSplitAggregator;
import au.csiro.variantspark.algo.RegressionLevelAggregator;
import au.csiro.variantspark.algo.SplitInfo;
import au.csiro.variantspark.algo.SubsetSplitInfo;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;

/**
 * Efficient variance-based splitter for nominal (categorical) variables in regression trees.
 *
 * Levels are sorted by their mean response value (ascending) and a linear scan finds the
 * best split — provably optimal for MSE (Fisher 1958), exactly as the class-proportion
 * sort is optimal for binary classification (Breiman et al., CART 1984).
 *
 * Complexity: O(N + L log L) where N = samples, L = levels.
 */
public class JNominalRegressionFastIndexedSplitter extends AbstractIndexedSplitterBase {
    private final byte[] data;
    private final int nLevels;
    private final double[] targetValues;
    private final RegressionLevelAggregator levelAgg;

    public JNominalRegressionFastIndexedSplitter(RegressionLevelAggregator levelAgg,
            IndexedSplitAggregator impurityCalc, byte[] data, int nLevels) {
        super(impurityCalc);
        this.levelAgg = levelAgg;
        this.data = data;
        this.nLevels = nLevels;
        this.targetValues = levelAgg.values();
    }

    @Override
    public SplitInfo doFindSplit(int[] splitIndices) {

        // O(N): populate level aggregators and accumulate per-level sums for mean computation.
        levelAgg.reset(nLevels);
        double[] levelSumY = new double[nLevels];
        int[] levelCounts = new int[nLevels];
        for (int i : splitIndices) {
            int level = data[i] & 0xFF;
            levelAgg.updateAt(level, i);
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

        int[] sortedLevels = new int[nActive];
        for (int i = 0; i < nActive; i++) {
            sortedLevels[i] = activeLevels[order[i]];
        }

        // Re-initialise impurityCalc from level aggregators: O(L) instead of O(N).
        impurityCalc.reset();
        for (int j = 0; j < nActive; j++) {
            impurityCalc.right().add(levelAgg.apply(activeLevels[j]));
        }

        // Linear scan: move one level at a time from Right to Left.
        long mask = 0L;
        SplitInfo result = null;
        double minImpurity = Double.MAX_VALUE;

        for (int j = 0; j < nActive - 1; j++) {
            int level = sortedLevels[j];
            mask |= (1L << level);
            impurityCalc.update(levelAgg.apply(level));

            if (impurityCalc.hasProperSplit()) {
                double thisImpurity = impurityCalc.getValue(leftRightImpurity);
                if (thisImpurity < minImpurity) {
                    long finalMask = mask;
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
