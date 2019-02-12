package au.csiro.variantspark.algo.impurity;

import java.util.Arrays;

import au.csiro.variantspark.algo.ClassificationImpurityAggregator;

/**
 * @author szu004
 * Gini impurity metric for classificaiton decision trees.
 * See: https://en.wikipedia.org/wiki/Decision_tree_learning#Metrics
 */
public class GiniImpurityAggregator implements ClassificationImpurityAggregator {
	private final int[] leftCounts;
	private final int[] rigthCounts;
	
	
	public GiniImpurityAggregator(int noLabels) {
		leftCounts = new int[noLabels];
		rigthCounts = new int[noLabels];
	}
	
	@Override
	public void initLabel(int label) {
		rigthCounts[label] ++;
	}
	
	@Override
	public void updateLabel(int label) {
		leftCounts[label]++;
		rigthCounts[label] --;		
	}
	
	@Override
	public double getValue(double []outLeftRight) {
		return FastGini.splitGini(leftCounts, rigthCounts, outLeftRight);
	}

	@Override
	public void reset() {
		Arrays.fill(leftCounts, 0);
		Arrays.fill(rigthCounts, 0);
	}	
}