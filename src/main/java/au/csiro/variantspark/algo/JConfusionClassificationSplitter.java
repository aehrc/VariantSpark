package au.csiro.variantspark.algo;

import java.util.Arrays;

import au.csiro.variantspark.metrics.Gini;


/**
 * Fast gini based splitter.
 * NOT MULITHEADED !!! (Caches state to avoid heap allocations)
 * 
 * @author szu004
 *
 */
public class JConfusionClassificationSplitter {
	private final int[] leftSplitCounts = new int[2];
	private final int[] rightSplitCounts = new int[2];	
	private final int[][] confusion;
	private final double[] leftRigtGini = new double[2];
	private final int[] labels;
	private final int nCategories;
	private final int nLevels  = 3;
		
	public JConfusionClassificationSplitter(int[] labels, int nCategories) {
		this.labels = labels;
		this.nCategories = nCategories;
		confusion = new int[nLevels][nCategories];
	}
	
	double gini(int[] counts) {
		switch(counts.length) {
			case 0: return 0.0;
			case 1: return 0.0;
			case 2: {
				int total = counts[0]  + counts[1];
				if (total == 0) return 0.0;
				double p0 = counts[0], p1 = counts[1], pt = total;
				return 1.0 - (p0*p0 + p1*p1)/(pt*pt);
			}
			case 3: {
				int total = counts[0]  + counts[1] + counts[2];
				if (total == 0) return 0.0;
				double p0 = counts[0], p1 = counts[1], p2=counts[2], pt = total;
				return 1.0 - (p0*p0 + p1*p1 + p2*p2)/(pt*pt);
			}	
			case 4: {
				int total = counts[0]  + counts[1] + counts[2] + counts[3];
				if (total == 0) return 0.0;
				double p0 = counts[0], p1 = counts[1], p2=counts[2], p3=counts[3], pt = total;
				return 1.0 - (p0*p0 + p1*p1 + p2*p2 + p3*p3)/(pt*pt);
			}
			default: throw new RuntimeException("Not imlemented");
		}
	 }
	
	int[] addEq(int[] a, int b[]) {
		for(int i = 0 ; i < a.length; i++) {
			a[i]+=b[i];
		}
		return a;
	}

	int[] subEq(int[] a, int b[]) {
		for(int i = 0 ; i < a.length; i++) {
			a[i]-=b[i];
		}
		return a;
	}

	// TODO (Add this only works for two classes !!!) 
	double splitGini(int[] left, int[] right, double[] out) {
		 int lt = left[0] + left[1];
		 int rt = right[0] + right[1];	
		 double leftGini = gini(left);
		 double rightGini = gini(right);
		 out[0] = leftGini;
		 out[1] = rightGini;
		 return (leftGini*lt + rightGini*rt)/(lt+rt);
	 } 
	 
	 public SplitInfo findSplit(double[] data,int[] splitIndices) {	
	    SplitInfo result = null;
	    double minGini = 1.0;
	    // let's think about like this 
	    // on the first pass we both calculate the splits as well as determine which split points are actually present
	    // in this dataset
	    // as 0 are most likely we will  do 0 as the initial pass

	    for(int sp = 0; sp < confusion.length; sp++ ) {
	    	Arrays.fill(confusion[sp],0);
	    }
	    
	    for(int i:splitIndices) {
	    	confusion[(int)data[i]][labels[i]]++;
	    }
		Arrays.fill(leftSplitCounts, 0);
		Arrays.fill(rightSplitCounts, 0);	    
	    for(int[] l: confusion) {
	    	addEq(rightSplitCounts, l);
	    }
		
	    // just try all for now
	    for(int sp = 0 ; sp < nLevels - 1; sp++)  {
	    	addEq(leftSplitCounts, confusion[sp]);
	    	subEq(rightSplitCounts, confusion[sp]);
			double g = splitGini(leftSplitCounts, rightSplitCounts, leftRigtGini);
			if (g < minGini ) {
				result = new SplitInfo(sp, g, leftRigtGini[0], leftRigtGini[1]);
				minGini = g;
			}	    	
	    }	    
		return result;
	 }
}
