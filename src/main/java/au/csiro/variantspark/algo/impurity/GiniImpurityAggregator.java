package au.csiro.variantspark.algo.impurity;

import java.util.Arrays;

import au.csiro.variantspark.algo.ArrayOps;
import au.csiro.variantspark.algo.ClassificationImpurityAggregator;
import au.csiro.variantspark.algo.ImpurityAggregator;
import au.csiro.variantspark.algo.SplitImpurity;

/**
 * @author szu004
 * Gini impurity metric for classificaiton decision trees.
 * See: https://en.wikipedia.org/wiki/Decision_tree_learning#Metrics
 */
public class GiniImpurityAggregator implements ClassificationImpurityAggregator {
	private final int[] labelCounts;
	private int count = 0;
	
	public GiniImpurityAggregator(int noLabels) {
		labelCounts = new int[noLabels];
	}
	@Override
	public void reset() {
		Arrays.fill(labelCounts, 0);
		count = 0;
	}

	@Override
	public boolean isEmpty() {
		return count == 0;
	}
		
	@Override
	public void addLabel(int label) {
		labelCounts[label]++;
		count ++;
	}

	@Override
	public void subLabel(int label) {
		labelCounts[label]--;
		count --;
	}	
	
	@Override
	public void add(ImpurityAggregator other) {
		if (!other.isEmpty()) {
			GiniImpurityAggregator otherGini = (GiniImpurityAggregator)other;
			count += otherGini.count;
			ArrayOps.addEq(labelCounts, otherGini.labelCounts);
		}
	}

	@Override
	public void sub(ImpurityAggregator other) {
		if (!other.isEmpty()) {
			GiniImpurityAggregator otherGini = (GiniImpurityAggregator)other;
			count -= otherGini.count;
			ArrayOps.subEq(labelCounts, otherGini.labelCounts);
		}
	}
	
	@Override
	public double getValue() {
		return FastGini.gini(labelCounts);
	}
	@Override
	public int getCount() {
		return count;
	}
	
	@Override
	public double splitValue(ImpurityAggregator other, SplitImpurity outSplitImp) {
		GiniImpurityAggregator otherGini = (GiniImpurityAggregator)other;
		outSplitImp.set(getValue(), otherGini.getValue());
		return (outSplitImp.left()*getCount() + outSplitImp.right()*otherGini.getCount())/(getCount() + otherGini.getCount());   
	}

}