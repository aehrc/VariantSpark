package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.IndexedImpurityCalculator;
import au.csiro.variantspark.algo.IndexedSplitter;
import au.csiro.variantspark.algo.SplitImpurity;
import au.csiro.variantspark.algo.SplitInfo;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;


/**
 * @author szu004
 * This is a naive implementation of precise continouns variable splitter
 */

public class JNaiveContinousIndexedSplitter implements IndexedSplitter {

	private final double[] data;
	public JNaiveContinousIndexedSplitter(double[] data) {
		this.data = data;
	}
	
	@Override
	public SplitInfo findSplit(IndexedImpurityCalculator impurityCalc, int[] splitIndices) {		
		if (splitIndices.length < 2) {
			// nothing to split
			return null;
		}		

		impurityCalc.init(splitIndices);

		// TODO: [Perfomance] this is where the sorting trick might be useful 
		// this is all to sort the subset indexes in ascending order by the values the refer to
		// using available Java functions
		double splitValues[] = new double[splitIndices.length];
		int order[] = new int[splitIndices.length];
		for(int i=0; i < splitIndices.length; i++) {
			splitValues[i] = data[splitIndices[i]];
			order[i] = i;
		}
		DoubleArrays.quickSortIndirect(order, splitValues);
		int[] sortedSplitIndices = new int[splitIndices.length];
		
		for(int i=0; i < order.length; i++) {
			sortedSplitIndices[i] = splitIndices[order[i]];
		}

		// INFO: a valid split is 
		// - left: v <= splitValue
		// - right: splitValue < v
		// NOTE: continous split only makes sense if there are at least two different values in subset
		// otherwise not split can be done
		// also we can only split at value changes so if there are repeat values we need to continue 
		// and only check for gini improvement if there is a change
		
		SplitImpurity leftRightImpurity = new SplitImpurity();
		double minImpurity = Double.MAX_VALUE;
		double splitValue = Double.NaN;
		double splitLeftImpurity = Double.NaN, splitRightImpurity=Double.NaN;
		double lastValue = data[sortedSplitIndices[0]];
		
		// we now go through the subset starting from the smallest values 
		// (sortedSplitIndices are sorted by ascending values the refer to)
		
		for(int i:sortedSplitIndices) {
			double currentValue = data[i];
			if (currentValue !=lastValue) {
				// possible split treshold
				double lastValueImpurity = impurityCalc.getImpurity(leftRightImpurity);
				if (lastValueImpurity < minImpurity) {
					// OK we have got a better split here
					splitValue = lastValue;
					minImpurity = lastValueImpurity;
					splitLeftImpurity = leftRightImpurity.left();
					splitRightImpurity = leftRightImpurity.right();					
				}
			}			
			impurityCalc.update(i);
			lastValue = currentValue;
		}
		// if splitValue is not NaN we seem to have a split here
		return (!Double.isNaN(splitValue))? new SplitInfo(splitValue, minImpurity, splitLeftImpurity, splitRightImpurity):null;
	}
}
