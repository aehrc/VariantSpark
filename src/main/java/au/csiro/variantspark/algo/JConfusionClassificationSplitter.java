package au.csiro.variantspark.algo;

import java.util.Arrays;

/**
 * Fast gini based splitter. NOT MULITHEADED !!! (Caches state to avoid heap
 * allocations)
 * 
 * @author szu004
 *
 */
public class JConfusionClassificationSplitter implements ClassificationSplitter {
	private final int[] leftSplitCounts;
	private final int[] rightSplitCounts;
	private final int[][] confusion;
	private final double[] leftRigtGini = new double[2];
	private final int[] labels;
	private final int nCategories;
	private final int nLevels;

	public JConfusionClassificationSplitter(int[] labels, int nCategories, int nLevels) {
		this.labels = labels;
		this.nCategories = nCategories;
		this.nLevels = nLevels;
		confusion = new int[nLevels][this.nCategories];
		leftSplitCounts = new int[this.nCategories];
		rightSplitCounts = new int[this.nCategories];
	}

	@Override
	public SplitInfo findSplit(double[] data, int[] splitIndices) {
		SplitInfo result = null;
		double minGini = Double.MAX_VALUE;

		if (splitIndices.length < 2) {
	    	return result;
	    }

		for (int sp = 0; sp < confusion.length; sp++) {
			Arrays.fill(confusion[sp], 0);
		}

		for (int i : splitIndices) {
			confusion[(int) data[i]][labels[i]]++;
		}
		Arrays.fill(leftSplitCounts, 0);
		Arrays.fill(rightSplitCounts, 0);
		for (int[] l : confusion) {
			ArrayOps.addEq(rightSplitCounts, l);
		}
		// just try all for now
		for (int sp = 0; sp < nLevels - 1; sp++) {
			ArrayOps.addEq(leftSplitCounts, confusion[sp]);
			ArrayOps.subEq(rightSplitCounts, confusion[sp]);
			double g = FastGini.splitGini(leftSplitCounts, rightSplitCounts, leftRigtGini, true);
			if (g < minGini) {
				result = new SplitInfo(sp, g, leftRigtGini[0], leftRigtGini[1]);
				minGini = g;
			}
		}
		return result;
	}
}
