package au.csiro.variantspark.algo;

import java.util.Arrays;

import au.csiro.variantspark.algo.impurity.FastGini;


/**
 * Fast gini based splitter.
 * NOT MULITHREADED !!! (Caches state to avoid heap allocations)
 * 
 * @author szu004
 *
 */
public class JMaskedClassificationSplitter {
	private final int[] leftSplitCounts;
	private final int[] rightSplitCounts;
	private final double[] leftRightGini = new double[2];
	private final int[] labels;
		
	public JMaskedClassificationSplitter(int[] labels, int nCategories) {
		this.labels = labels;
		this.leftSplitCounts = new int[nCategories];
		this.rightSplitCounts = new int[nCategories];
	}
		 
	 public SplitInfo findSplit(double[] data,int[] splitIndices) {	
	    SplitInfo result = null;
	    double minGini = 1.0;

	    /* TODO (review and test implementation)
         * on the first pass we calculate the splits
         * AND determine which split points are in this dataset
	     * because 0 is most likely we will do 0 as the initial pass */
	    long splitCandidateSet = 0L; 
		for(int i:splitIndices) {
			splitCandidateSet|=(1 << (int)data[i]);
		}
		
		int sp  = 0;
		while(splitCandidateSet != 0L) {
			while (splitCandidateSet != 0L && (splitCandidateSet & 1) == 0) {
				sp ++;
				splitCandidateSet >>= 1;
			}
			splitCandidateSet >>= 1;
			
			if (splitCandidateSet != 0L) {
				Arrays.fill(leftSplitCounts, 0);
				Arrays.fill(rightSplitCounts, 0);
				for(int i:splitIndices) {
					if ((int)data[i] <=sp) {
						leftSplitCounts[labels[i]]++;
					} else {
						rightSplitCounts[labels[i]]++;					
					}
				}
				double g = FastGini.splitGini(leftSplitCounts, rightSplitCounts, leftRightGini);
				if (g < minGini ) {
					result = new SplitInfo(sp, g, leftRightGini[0], leftRightGini[1]);
					minGini = g;
				}
				sp++;
			}
		}
		return result;
	 }
}
