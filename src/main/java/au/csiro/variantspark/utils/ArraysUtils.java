package au.csiro.variantspark.utils;

public final class ArraysUtils {

	public static int[] permutate(int data[], int order[]) {
		int result[] = new int[data.length];
		for(int i = 0; i< data.length; i++) {
			result[i] = order[data[i]];
		}
		return result;
	}
}
