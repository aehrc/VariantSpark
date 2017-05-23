package au.csiro.variantspark.algo;

import java.util.Arrays;
import java.util.function.BiConsumer;

/**
 * Fast gini based splitter. NOT MULITHREADED !!!
 * Caches state to avoid heap allocations
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
		return dofindSplit(splitIndices, (idx, conf) -> {
			for (int i : idx) {
				conf[(int) data[i]][labels[i]]++;
			}
		});
	}

	@Override
	public SplitInfo findSplit(int[] data, int[] splitIndices) {
		return dofindSplit(splitIndices, (idx, conf) -> {
			for (int i : idx) {
				conf[data[i]][labels[i]]++;
			}
		});
	}

	@Override
	public SplitInfo findSplit(byte[] data, int[] splitIndices) {
		return dofindSplit(splitIndices, (idx, conf) -> {
			for (int i : idx) {
				conf[(int) data[i]][labels[i]]++;
			}
		});
	}
	
	private <T> SplitInfo dofindSplit(int[] splitIndices, BiConsumer<int[], int[][]> confusionCalc) {
		SplitInfo result = null;
		double minGini = Double.MAX_VALUE;

		if (splitIndices.length < 2) {
	    	return result;
	    }

		for (int[] aConfusion : confusion) {
			Arrays.fill(aConfusion, 0);
		}

		confusionCalc.accept(splitIndices, confusion);

		Arrays.fill(leftSplitCounts, 0);
		Arrays.fill(rightSplitCounts, 0);
		for (int[] l : confusion) {
			ArrayOps.addEq(rightSplitCounts, l);
		}

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
