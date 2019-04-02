package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.ConfusionAggregator;
import au.csiro.variantspark.algo.ImpurityAggregator;
import au.csiro.variantspark.algo.IndexedSplitAggregator;
import au.csiro.variantspark.algo.SplitInfo;

/**
 * @author szu004
 * Fast gini based splitter. NOT MULITHREADED !!!
 * Caches state to avoid heap allocations
 * 
 * Why does it appear to be only relevant for classification tasks
 * when the number of labels is known? 
 * (how can this trick then be used for regression on other variables)
 */



public class JOrderedFastIndexedSplitter extends AbstractIndexedSplitterBase {
	private final byte[] data;
	private final int nLevels;
	private final  ConfusionAggregator confusionAgg;

	public JOrderedFastIndexedSplitter(ConfusionAggregator confusionAgg, IndexedSplitAggregator impurityCalc, byte[] data, int nLevels) {
		super(impurityCalc);
		this.confusionAgg = confusionAgg;
		this.data = data;
		this.nLevels = nLevels;
	}

	@Override
	protected SplitInfo doFindSplit(int[] splitIndices) {
	    SplitInfo result = null;
	    double minImpurity = Double.MAX_VALUE;
	    // compute the confusion matrix
	    confusionAgg.reset(nLevels);
	    for(int i:splitIndices) {
	    	confusionAgg.updateAt(data[i], i);
	    }
	    // find the best split using the confusion matrix
		for (int sp = 0; sp < nLevels - 1; sp++) {
			// sp i the current split value
			// get it from the confusion matrix
			ImpurityAggregator thisAggregator = confusionAgg.apply(sp);
			if (!thisAggregator.isEmpty()) {
				// only consider value that appeared at least once in the split
				impurityCalc.update(thisAggregator);
				double thisImpurity = impurityCalc.getValue(leftRightImpurity);
				if (thisImpurity < minImpurity) {
					result = new SplitInfo(sp, thisImpurity, leftRightImpurity.left(), leftRightImpurity.right());
					minImpurity = thisImpurity;
				}
			}
		}
		return result;	
	}
}
