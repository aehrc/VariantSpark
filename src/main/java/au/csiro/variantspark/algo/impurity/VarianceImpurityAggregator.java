package au.csiro.variantspark.algo.impurity;

import au.csiro.variantspark.algo.RegressionImpurityAggregator;



/**
 * @author szu004
 * Variance impurity metric for regression decision trees.
 * See: https://en.wikipedia.org/wiki/Decision_tree_learning#Metrics
 */
public class VarianceImpurityAggregator implements RegressionImpurityAggregator {

	double leftSumOfValues = 0;
	double leftSumOfSquares = 0;
	int leftCount = 0;
	double rightSumOfValues = 0;
	double rightSumOfSquares = 0;
	int rightCount = 0;
	
	@Override
	public double getValue(double[] outLeftRight) {
		outLeftRight[0] = (leftSumOfSquares - leftSumOfValues*leftSumOfValues)/leftCount;
		outLeftRight[1] = (rightSumOfSquares - rightSumOfValues*rightSumOfValues)/rightCount;
		return 	outLeftRight[0] + outLeftRight[1];	
	}

	@Override
	public void initValue(double value) {
		rightSumOfValues+= value;
		rightSumOfSquares+= (value*value);
		rightCount = 0;
	}

	@Override
	public void updateValue(double value) {
		leftSumOfValues+= value;
		leftSumOfSquares+=(value*value);
		leftCount++;
		rightSumOfValues-= value;
		rightSumOfSquares-= (value*value);
		rightCount--;
	}

	@Override
	public void reset() {
		leftSumOfValues = 0;
		leftSumOfSquares = 0;
		leftCount = 0;
		rightSumOfValues = 0;
		rightSumOfSquares = 0;
		rightCount = 0;		
	}	
}