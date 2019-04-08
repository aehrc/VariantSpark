package au.csiro.variantspark.algo.split;

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



public class JOrderedIndexedSplitter extends  AbstractIndexedSplitterBase {
	private final byte[] data;
	private final int nLevels;
	

	public JOrderedIndexedSplitter(IndexedSplitAggregator impurityCalc, byte[] data, int nLevels) {
		super(impurityCalc);
		this.data = data;
		this.nLevels = nLevels;
	}

	private int getLevelCount(byte[] data) {
		int maxLevel = 0;
		for(byte d:data) {
			if ((int)d > maxLevel) {
				maxLevel = (int)d;
			}
		}
		return maxLevel+1;
	}

	@Override
	public SplitInfo doFindSplit(int[] splitIndices) {
	    SplitInfo result = null;
	    double minImpurity = Double.MAX_VALUE;
	    int actualNLevels = (nLevels > 0) ?  nLevels : getLevelCount(data);
		for(int sp = 0 ; sp < actualNLevels - 1; sp ++) {
			//TODO: Performnce (remember the state) rather then compute each time
			impurityCalc.init(splitIndices);
			for(int i:splitIndices) {
				if ((int)data[i] <=sp) {
					impurityCalc.update(i);
				} 
			}
			double g = impurityCalc.getValue(leftRightImpurity);
			if (g < minImpurity ) {
				result = new SplitInfo(sp, g, leftRightImpurity.left(), leftRightImpurity.right());
				minImpurity = g;
			}
		}
		return result;	
	}
}
