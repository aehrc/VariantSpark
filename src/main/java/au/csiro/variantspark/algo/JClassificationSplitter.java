package au.csiro.variantspark.algo;

import java.util.Arrays;

import au.csiro.variantspark.metrics.Gini;

public class JClassificationSplitter {
	private final int[] leftSplitCounts = new int[2];
	private final int[] rightSplitCounts = new int[2];
	private final double[] leftRigtGini = new double[2];
	private final int[] labels;
	private final int nCategories;
		
	public JClassificationSplitter(int[] labels, int nCategories) {
		this.labels = labels;
		this.nCategories = nCategories;
	}
	
	double gini(int[] counts) {
		 if (counts.length == 2) {
		      int c1 = counts[0];
    	      int c2 = counts[1];
    	      int total = (c1+c2);
    	      double p1 = ((double)c1)/total;
    	      double p2 = ((double)c2)/total;
    	      return total == 0?0.0: (1.0 - p1*p1 - p2*p2);			 
		 } else {
			 throw new RuntimeException("Not imlemented");
		 }
	 }
	
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
		for(int sp = 0; sp< 2 ; sp++) {
			Arrays.fill(leftSplitCounts, 0);
			Arrays.fill(rightSplitCounts, 0);
			for(int i:splitIndices) {
				if ((int)data[i] <=sp) {
					leftSplitCounts[labels[i]]++;
				} else {
					rightSplitCounts[labels[i]]++;					
				}
			}
			double g = splitGini(leftSplitCounts, rightSplitCounts, leftRigtGini);
			if (g < minGini ) {
				result = new SplitInfo(sp, g, leftRigtGini[0], leftRigtGini[1]);
				minGini = g;
			}
		}
		return result;
	 }
}
