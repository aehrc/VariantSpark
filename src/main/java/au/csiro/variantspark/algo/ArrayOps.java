package au.csiro.variantspark.algo;

import java.util.Arrays;

import it.unimi.dsi.fastutil.doubles.DoubleArrays;

public final class ArrayOps {
	private ArrayOps() {}
	
	public static int[] addEq(int[] a, int b[]) {
		for(int i = 0 ; i < a.length; i++) {
			a[i]+=b[i];
		}
		return a;
	}

	public static int[] subEq(int[] a, int b[]) {
		for(int i = 0 ; i < a.length; i++) {
			a[i]-=b[i];
		}
		return a;
	}

	public static int sum(int[] a) {
		switch (a.length) {
			case 0: return 0;
			case 1: return a[0];
			case 2: return a[0] + a[1];
			default : {
				int s = 0;
				for (int i: a) {
					s+=i;
				}
				return s;
			}
		}
	}
	
	
	/**
	 * Computes dense ranking do a double array. Elements with the same value receive the same rank.
	 * @param data the elements to be ranked
	 * @param out_rank output arrays with rank for each elements
	 * @return the (unique) values corresponding to ranks
	 */
	public static double[] denseRank(double[] data, int[] out_rank) {
		
		int[] order = new int[data.length];
		for(int i=0; i < out_rank.length; i++) {
			order[i] = i;
		}
		DoubleArrays.quickSortIndirect(order, data);
		double[] uniqueDataValues = new double[data.length];
		
		int currentRank = -1;
		double lastValue = Double.NaN;
		
		// the elements of dataOder are indexes of data elements (in ascending order)
		for(int i:order) {
			double currentValue = data[i];
			if (currentValue !=lastValue) {
				uniqueDataValues[++currentRank] = currentValue;
			}
			out_rank[i] = currentRank;
			lastValue = currentValue;
		}
		return Arrays.copyOf(uniqueDataValues, currentRank + 1);
		
	}
}
