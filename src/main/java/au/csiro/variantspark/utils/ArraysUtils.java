package au.csiro.variantspark.utils;

public final class ArraysUtils {

    public static int[] permutate(int data[], int order[]) {
        int result[] = new int[data.length];
        for (int i = 0; i < data.length; i++) {
            result[i] = order[data[i]];
        }
        return result;
    }

    /**
     * Find the first index of the maximum value in an array
     *
     * @param data
     * @return
     */
    public static int maxIndex(int data[]) {
        if (data == null || data.length == 0) {
            throw new IllegalArgumentException("Cannot find max index of an empty array");
        }
        int maxIndex = 0;
        for (int i = 1; i < data.length; i++) {
            if (data[i] > data[maxIndex]) maxIndex = i;
        }
        return maxIndex; // position of the first largest found
    }

}
