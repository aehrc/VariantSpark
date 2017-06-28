package au.csiro.variantspark.algo;

public interface ClassificationSplitter {
	SplitInfo findSplit(double[] data, int[] splitIndices);
	SplitInfo findSplit(int[] data, int[] splitIndices);
	SplitInfo findSplit(byte[] data, int[] splitIndices);
}
