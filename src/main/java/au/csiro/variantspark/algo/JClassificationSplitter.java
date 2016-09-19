package au.csiro.variantspark.algo;

import java.util.Arrays;

/**
 * Fast gini based splitter.
 * NOT MULITHEADED !!! (Caches state to avoid heap allocations)
 * 
 * @author szu004
 *
 */
public class JClassificationSplitter implements ClassificationSplitter {
	private final int[] leftSplitCounts;
	private final int[] rightSplitCounts;
	private final double[] leftRigtGini = new double[2];
	private final int[] labels;
	private final int nLevels;

	/**
	 * The ounbounded version
	 * @param labels
	 * @param nCategories
	 */
	public JClassificationSplitter(int[] labels, int nCategories) {
		this(labels, nCategories, Integer.MIN_VALUE);
	}

	public JClassificationSplitter(int[] labels, int nCategories, int nLevels) {
		this.labels = labels;
		this.leftSplitCounts = new int[nCategories];
		this.rightSplitCounts = new int[nCategories];
		this.nLevels = nLevels;
	}
	
	
	
	@Override
	public SplitInfo findSplit(double[] data,int[] splitIndices) {	
	    SplitInfo result = null;
	    double minGini = Double.MAX_VALUE;
	    if (splitIndices.length < 2) {
	    	return result;
	    }
	 
	    int actualNLevles = (nLevels > 0) ?  nLevels : getLevelCount(data);
	    
		for(int sp = 0 ; sp < actualNLevles - 1; sp ++) {
			Arrays.fill(leftSplitCounts, 0);
			Arrays.fill(rightSplitCounts, 0);
			for(int i:splitIndices) {
				if ((int)data[i] <=sp) {
					leftSplitCounts[labels[i]]++;
				} else {
					rightSplitCounts[labels[i]]++;					
				}
			}
			double g = FastGini.splitGini(leftSplitCounts, rightSplitCounts, leftRigtGini, true);
			if (g < minGini ) {
				result = new SplitInfo(sp, g, leftRigtGini[0], leftRigtGini[1]);
				minGini = g;
			}
		}
		return result;
	 }

	private int getLevelCount(double[] data) {
		int maxLevel = 0;
		for(double d:data) {
			if ((int)d > maxLevel) {
				maxLevel = (int)d;
			}
		}
		return maxLevel+1;
	}

	@Override
	public SplitInfo findSplit(int[] data, int[] splitIndices) {
		throw new RuntimeException("Not implemented yet");
	}

	@Override
	public SplitInfo findSplit(byte[] data, int[] splitIndices) {
		throw new RuntimeException("Not implemented yet");
	}
}
