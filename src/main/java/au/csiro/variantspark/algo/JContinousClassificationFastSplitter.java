package au.csiro.variantspark.algo;

import java.util.Arrays;

import it.unimi.dsi.fastutil.doubles.DoubleArrays;


/**
 * @author szu004
 * This is a naive implementation of precise (not binning) continous variable splitter
 */
public class JContinousClassificationFastSplitter implements ClassificationSplitter  {

	private final int[] labels;
	private final int noLabels;
	
	public JContinousClassificationFastSplitter(int[] labels, int noLabels) {
		this.labels = labels;
		this.noLabels = noLabels;
	}
	
	@Override
	public SplitInfo findSplit(final double[] data, int[] splitIndices) {		
		if (splitIndices.length < 2) {
			// nothing to split
			return null;
		}		
		// create a dense rank for the data
		// TODO: This needs to be move outside
		int[] denseRank = new int[data.length];
		double rankValues[] = ArrayOps.denseRank(data, denseRank);
		JConfusionClassificationSplitter splitter = new JConfusionClassificationSplitter(this.labels, this.noLabels, rankValues.length);
		SplitInfo split = splitter.findSplit(denseRank, splitIndices);
		// now need to convert the rank to the actual value
		return split == null ? split: new SplitInfo(rankValues[(int)split.splitPoint()], split.gini(), split.leftGini(), split.rightGini());
	
	}

	@Override
	public SplitInfo findSplit(int[] data, int[] splitIndices) {
		throw new UnsupportedOperationException("JContinousClassificationSplitter.findSplit(int[] ...");
	
	}

	@Override
	public SplitInfo findSplit(byte[] data, int[] splitIndices) {
		throw new UnsupportedOperationException("JContinousClassificationSplitter.findSplit(byte[] ...");
	}

}
