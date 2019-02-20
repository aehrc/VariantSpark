package au.csiro.variantspark.algo.split;

import au.csiro.variantspark.algo.IndexedSplitAggregator;
import au.csiro.variantspark.algo.IndexedSplitter;
import au.csiro.variantspark.algo.SplitImpurity;
import au.csiro.variantspark.algo.SplitInfo;

public abstract class AbstractIndexedSplitterBase implements IndexedSplitter {

	
	protected final SplitImpurity leftRightImpurity = new SplitImpurity();
	protected IndexedSplitAggregator impurityCalc;
	public AbstractIndexedSplitterBase(IndexedSplitAggregator impurityCalc) {
		this.impurityCalc = impurityCalc;
	}	
	
	@Override
	public SplitInfo findSplit(int[] splitIndices) {
		if (splitIndices.length < 2) {
			// nothing to split
			return null;
		}		
		impurityCalc.init(splitIndices);
		return doFindSplit(splitIndices);
	}

	protected abstract SplitInfo doFindSplit(int[] splitIndices);

}
