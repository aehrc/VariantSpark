package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.ClassificationSplitAggregator;
import au.csiro.variantspark.algo.ConfusionAggregator;
import au.csiro.variantspark.algo.IndexedSplitAggregator;
import au.csiro.variantspark.algo.SplitInfo;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;

/**
 * Efficient impurity-based splitter for nominal (categorical) variables.
 *
 * Uses the CART one-vs-rest heuristic instead of exhaustive subset enumeration:
 * for each class k, levels are sorted by p(class=k | level) in ascending order and a linear scan
 * (like the continuous splitter) finds the best split in that ordering.
 * The best split across all K orderings is returned.
 *
 * Complexity: O(N + K * L * log L) where N = samples, K = classes, L = levels.
 * This replaces the previous O(2^L) exhaustive mask search.
 *
 * For binary classification (K=2), sorting by class proportion is provably
 * optimal (Breiman et al., CART 1984). For multiclass it is a well-established
 * heuristic equivalent to the twoing rule and used by LightGBM / scikit-learn.
 */
public class JNominalFastIndexedSplitter extends AbstractIndexedSplitterBase {
    private final byte[] data;
    private final int nLevels;
    private final int nCategories;
    private final int[] labels;
    private final ConfusionAggregator confusionAgg;

    public JNominalFastIndexedSplitter(ConfusionAggregator confusionAgg,
            IndexedSplitAggregator impurityCalc, byte[] data, int nLevels) {
        super(impurityCalc);
        if (!(impurityCalc instanceof ClassificationSplitAggregator)) {
            throw new IllegalArgumentException(
                    "JNominalFastIndexedSplitter requires ClassificationSplitAggregator");
        }
        ClassificationSplitAggregator classAgg = (ClassificationSplitAggregator) impurityCalc;
        this.confusionAgg = confusionAgg;
        this.data = data;
        this.nLevels = nLevels;
        this.nCategories = classAgg.nCategories();
        this.labels = classAgg.labels();
    }

    @Override
    public SplitInfo doFindSplit(int[] splitIndices) {

        // Build confusion matrix and per-level per-class counts
        confusionAgg.reset(nLevels);
        int[][] levelClassCounts = new int[nLevels][nCategories];
        int[] levelTotals = new int[nLevels];

        for (int i : splitIndices) {
            int level = data[i];
            confusionAgg.updateAt(level, i);
            levelClassCounts[level][labels[i]]++;
            levelTotals[level]++;
        }

        // Collect non-empty (active) levels and build a bitmask of active levels
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
            return null;  // need at least two populated levels to split
        }

        // Canonical form: the highest active level is always kept in the Right
        // node.  When the scan produces a mask that includes it, we complement
        // the mask (relative to active levels) and swap left/right impurities.
        int highestActiveLevel = activeLevels[nActive - 1];

        // One-vs-rest ordered scan
        // For each class k we sort levels by p(k | level) in ascending order, then
        // sweep from left to right exactly like the ordinal splitter, moving one
        // level at a time from Right to Left and evaluating the weighted impurity.
        SplitInfo result = null;
        double minImpurity = Double.MAX_VALUE;

        int[] sortedLevels = new int[nActive];
        double[] proportions = new double[nActive];
        int[] order = new int[nActive];

        for (int k = 0; k < nCategories; k++) {

            // Compute proportion of class k at each active level
            for (int j = 0; j < nActive; j++) {
                proportions[j] = (double) levelClassCounts[activeLevels[j]][k] / levelTotals[activeLevels[j]];
                order[j] = j;
            }

            // Sort levels by proportion using indirect quicksort (O(L log L)).
            // Consistent with JNaiveContinousIndexedSplitter; correct for all L
            // including covariate features with many levels.
            DoubleArrays.quickSortIndirect(order, proportions);
            for (int i = 0; i < nActive; i++) {
                sortedLevels[i] = activeLevels[order[i]];
            }

            // Re-initialise impurityCalc from confusion matrix: O(L) per class
            // instead of O(N) if re-scanning splitIndices.
            impurityCalc.reset();
            for (int j = 0; j < nActive; j++) {
                impurityCalc.right().add(confusionAgg.apply(activeLevels[j]));
            }

            // Linear scan: accumulate levels into Left in sorted order.
            // Unlike the continuous splitter we do NOT skip levels with equal
            // proportions. Each level is a distinct categorical value, so every
            // boundary is a genuinely different partition.
            int mask = 0;
            for (int j = 0; j < nActive - 1; j++) {
                int level = sortedLevels[j];
                mask |= (1 << level);
                impurityCalc.update(confusionAgg.apply(level));

                if (impurityCalc.hasProperSplit()) {
                    double thisImpurity = impurityCalc.getValue(leftRightImpurity);
                    if (thisImpurity < minImpurity) {
                        // Normalise mask so the highest active level stays in Right
                        int finalMask = mask;
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
