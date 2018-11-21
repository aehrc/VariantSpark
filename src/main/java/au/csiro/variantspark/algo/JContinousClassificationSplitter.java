package au.csiro.variantspark.algo;

import it.unimi.dsi.fastutil.doubles.DoubleArrays;


class SplitGiniAggregator {
	private final int[] leftCounts;
	private final int[] rigthCounts;
	
	
	public SplitGiniAggregator(int noLabels) {
		leftCounts = new int[noLabels];
		rigthCounts = new int[noLabels];
	}
	
	public void init(int label) {
		rigthCounts[label] ++;
	}
	
	public void update(int label) {
		leftCounts[label]++;
		rigthCounts[label] --;		
	}
	
	public double getGini(double []outLeftRight) {
		return FastGini.splitGini(leftCounts, rigthCounts, outLeftRight);
	}
	
}

public class JContinousClassificationSplitter implements ClassificationSplitter  {

	private final int[] labels;
	private final int noLabels;
	
	public JContinousClassificationSplitter(int[] labels, int noLabels) {
		this.labels = labels;
		this.noLabels = noLabels;
	}
	
	@Override
	public SplitInfo findSplit(final double[] data, int[] splitIndices) {		
		if (splitIndices.length < 2) {
			// nothing to split
			return null;
		}
		// TODO: Use sorting trick
		// This is a naive implementation sorting eveything at evry split
		
		// essentialy need to indirecly sort split indices by the values of data
		// 
		
		SplitGiniAggregator giniAggregator = new SplitGiniAggregator(noLabels);
		
		double splitValues[] = new double[splitIndices.length];
		int perm[] = new int[splitIndices.length];
		for(int i=0; i < splitIndices.length; i++) {
			splitValues[i] = data[splitIndices[i]];
			perm[i] = i;
			giniAggregator.init(labels[splitIndices[i]]);
		}
		DoubleArrays.quickSortIndirect(perm, splitValues);
		// TODO: This is obviously crazy with dereferencing
		// INFO: a valid split is 
		// - left: v <= splitValue
		// - right: splitValue < v
		// NOTE: continous split only makes sense if there are at least two different values in subset
		// otherwise not split can be done
		// also we can only split at value changes so if there are repeat values we need to continue 
		// and only check for gini improvement if there is a change
		
		double leftRightGini[] = new double[2];
		double minGini = Double.MAX_VALUE;
		double splitValue = Double.NaN;
		double splitLeftGini=Double.NaN, splitRightGini=Double.NaN;
		double lastValue =  splitValues[perm[0]];
		
		for(int i=0; i < splitIndices.length;i++) {
			double currentValue = splitValues[perm[i]];
			int currentLabel = labels[splitIndices[perm[i]]];
			if (currentValue !=lastValue) {
				// possible split treshold
				double lastValueGini = giniAggregator.getGini(leftRightGini);
				if (lastValueGini < minGini) {
					// OK we have got a better split here
					splitValue = lastValue;
					minGini = lastValueGini;
					splitLeftGini = leftRightGini[0];
					splitRightGini = leftRightGini[1];					
				}
			}			
			giniAggregator.update(currentLabel);
			lastValue = currentValue;
		}
		
		// if splitValue is not NaN we seem to have a split here
		return (!Double.isNaN(splitValue))? new SplitInfo(splitValue, minGini, splitLeftGini, splitRightGini):null;
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
