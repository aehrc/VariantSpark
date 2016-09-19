package au.csiro.variantspark.algo;

public interface ClassificationSplitter {
	public SplitInfo findSplit(double[] data,int[] splitIndices);
	public SplitInfo findSplit(int[] data,int[] splitIndices);
	public SplitInfo findSplit(byte[] data,int[] splitIndices);
}
