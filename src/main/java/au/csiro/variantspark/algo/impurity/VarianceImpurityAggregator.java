package au.csiro.variantspark.algo.impurity;

import au.csiro.variantspark.algo.ImpurityAggregator;
import au.csiro.variantspark.algo.RegressionImpurityAggregator;



/**
 * @author szu004
 * Variance impurity metric for regression decision trees.
 * See: https://en.wikipedia.org/wiki/Decision_tree_learning#Metrics
 */
public class VarianceImpurityAggregator implements RegressionImpurityAggregator {

	double sumOfValues = 0;
	double sumOfSquares = 0;
	int count = 0;
	
	@Override
	public void reset() {
		sumOfValues = 0;
		sumOfSquares = 0;
		count = 0;
	}

	@Override
	public boolean isEmpty() {
		return count == 0;
	}
	
	@Override
	public int getCount() {
		return count;
	}	
	
	@Override
	public void add(ImpurityAggregator other) {
		if (!other.isEmpty()) {
			VarianceImpurityAggregator otherVariance = (VarianceImpurityAggregator)other;
			sumOfValues+= otherVariance.sumOfValues;
			sumOfSquares+=otherVariance.sumOfSquares;
			count+=otherVariance.count;
		}
	}

	@Override
	public void sub(ImpurityAggregator other) {
		if (!other.isEmpty()) {
			VarianceImpurityAggregator otherVariance = (VarianceImpurityAggregator)other;
			sumOfValues-= otherVariance.sumOfValues;
			sumOfSquares-=otherVariance.sumOfSquares;
			count-=otherVariance.count;
		}
	}

	@Override
	public double getValue() {
		//TODO: [REVIEW] Need to be reviewed when implementing regresion trees
		// but technically the Var(X) = E(X^2) - E(X)^2
		//return sumOfSquares/count - (sumOfValues/count)*(sumOfValues/count);
		throw new UnsupportedOperationException("Needs review. Defered until regression trees implementaiton");
	}

	@Override
	public void addValue(double value) {
		sumOfValues+= value;
		sumOfSquares+=(value*value);
		count++;
	}

	@Override
	public void subValue(double value) {
		sumOfValues-= value;
		sumOfSquares-=(value*value);
		count--;
	}
}