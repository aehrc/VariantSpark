package au.csiro.variantspark.algo;

public interface ClassificationSplitter {
	public SplitInfo findSplit(double[] data,int[] splitIndices);
}
