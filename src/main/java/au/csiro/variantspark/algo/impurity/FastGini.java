package au.csiro.variantspark.algo.impurity;

import au.csiro.variantspark.algo.ArrayOps;

@SuppressWarnings("JavaDoc")
public final class FastGini {

	private FastGini() {}
	
	public static double defaultGini(int[] counts) {
		if (counts.length == 0 || counts.length ==1) {
			return 0.0;
		}
		int total = 0;
		double sumSq = 0.0;
		for (int c:counts) {
			total += c;
			sumSq += ((double)c)*c;
		}
		return (total == 0) ? 0.0 : 1 -sumSq/(((double)total)*total);
	}

	public static double gini(int[] counts) {
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
			default: return defaultGini(counts);
		}
	}
	public static double splitGini(int[] left, int[] right, double[] out) {
		return splitGini(left, right, out, false);
	} 
	
	public static double splitGini(int[] left, int[] right, double[] out, boolean filterEmpty) {
		 int lt = ArrayOps.sum(left);
		 int rt = ArrayOps.sum(right);	
		 if (filterEmpty && ((lt == 0) || (rt ==0))) {
			 return Double.MAX_VALUE;
		 }
		 double leftGini = gini(left);
		 double rightGini = gini(right);
		 out[0] = leftGini;
		 out[1] = rightGini;
		 return (leftGini*lt + rightGini*rt)/(lt+rt);
	 } 
}
