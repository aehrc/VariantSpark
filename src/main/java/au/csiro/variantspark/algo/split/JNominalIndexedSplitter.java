package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.ClassificationSplitAggregator;
import au.csiro.variantspark.algo.IndexedSplitAggregator;
import au.csiro.variantspark.algo.SplitInfo;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;

/**
 * Slow fallback splitter for nominal (categorical) variables.
 *
 * Uses the same CART one-vs-rest heuristic as {@link JNominalFastIndexedSplitter} but does NOT
 * require a pre-allocated {@link au.csiro.variantspark.algo.ConfusionAggregator}. Instead it
 * rescans {@code splitIndices} for each candidate partition, analogous to how
 * {@link JOrderedIndexedSplitter} handles ordinal features.
 *
 * This is acceptable because this splitter is only selected by
 * {@link au.csiro.variantspark.algo.ThresholdIndexedSplitter} when the subset is very sparse
 * (N / L < qThreshold, default 0.02), meaning N is small.
 */
public class JNominalIndexedSplitter extends AbstractIndexedSplitterBase {
    private final byte[] data;
    private final int nLevels;
    private final int nCategories;
    private final int[] labels;

    public JNominalIndexedSplitter(IndexedSplitAggregator impurityCalc, byte[] data,
            int nLevels) {
        super(impurityCalc);
        if (!(impurityCalc instanceof ClassificationSplitAggregator)) {
            throw new IllegalArgumentException(
                    "JNominalIndexedSplitter requires ClassificationSplitAggregator");
        }
        ClassificationSplitAggregator classAgg = (ClassificationSplitAggregator) impurityCalc;
        this.data = data;
        this.nLevels = nLevels;
        this.nCategories = classAgg.nCategories();
        this.labels = classAgg.labels();
    }

    @Override
    public SplitInfo doFindSplit(int[] splitIndices) {

        // Build per-level per-class counts in a single O(N) scan.
        int[][] levelClassCounts = new int[nLevels][nCategories];
        int[] levelTotals = new int[nLevels];
        for (int i : splitIndices) {
            int level = data[i] & 0xFF;
            levelClassCounts[level][labels[i]]++;
            levelTotals[level]++;
        }

        // Collect non-empty (active) levels and build a bitmask of active levels.
        int nActive = 0;
        int[] activeLevels = new int[nLevels];
        int activeMask = 0;
        for (int l = 0; l < nLevels; l++) {
            if (levelTotals[l] > 0) {
                activeLevels[nActive++] = l;
                activeMask |= (1 << l);
            }
        }

        if (nActive < 2) {
            return null; // need at least two populated levels to split
        }

        // Canonical form: the highest active level always stays in Right.
        int highestActiveLevel = activeLevels[nActive - 1];

        SplitInfo result = null;
        double minImpurity = Double.MAX_VALUE;

        double[] proportions = new double[nActive];
        int[] order = new int[nActive];

        for (int k = 0; k < nCategories; k++) {

            // Compute proportion of class k at each active level.
            for (int j = 0; j < nActive; j++) {
                int l = activeLevels[j];
                proportions[j] = (double) levelClassCounts[l][k] / levelTotals[l];
                order[j] = j;
            }

            // Sort levels by ascending proportion of class k using indirect quicksort.
            DoubleArrays.quickSortIndirect(order, proportions);

            // For each prefix of the sorted levels (left set = first j+1 levels):
            //   - reset impurityCalc (init puts everything in Right)
            //   - move samples whose level is in the left bitmask to Left
            //   - evaluate the split
            int leftMask = 0;
            for (int j = 0; j < nActive - 1; j++) {
                int level = activeLevels[order[j]];
                leftMask |= (1 << level);

                // Cost: O(N) rescan per prefix.
                impurityCalc.init(splitIndices);
                for (int i : splitIndices) {
                    int sampleLevel = data[i] & 0xFF;
                    if ((leftMask & (1 << sampleLevel)) != 0) {
                        impurityCalc.update(i);
                    }
                }

                if (impurityCalc.hasProperSplit()) {
                    double thisImpurity = impurityCalc.getValue(leftRightImpurity);
                    if (thisImpurity < minImpurity) {
                        // Normalise: if highestActiveLevel ended up in Left, complement mask.
                        int finalMask = leftMask;
                        double leftImp = leftRightImpurity.left();
                        double rightImp = leftRightImpurity.right();
                        if ((finalMask & (1 << highestActiveLevel)) != 0) {
                            finalMask = activeMask ^ finalMask;
                            double tmp = leftImp;
                            leftImp = rightImp;
                            rightImp = tmp;
                        }
                        result = new SplitInfo(finalMask, thisImpurity, leftImp, rightImp);
                        minImpurity = thisImpurity;
                    }
                }
            }
        }
        return result;
    }
}
