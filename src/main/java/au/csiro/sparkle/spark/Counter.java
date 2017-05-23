package au.csiro.sparkle.spark;

import java.io.Serializable;

import org.apache.spark.Accumulator;

// TODO: (Update) to use V2
// import org.apache.spark.util.AccumulatorV2;

public class Counter implements Serializable {
	
    private static final long serialVersionUID = 1L;

    private final Accumulator<Integer> accumulator;

	public Counter(Accumulator<Integer> accumulator) {
	    this.accumulator = accumulator;
    }

	public <T> T inc(T t) {
		accumulator.add(1);
		return t;
	}

	
	public long value() {
		return accumulator.value();
    }

	public void add(int value) {
		accumulator.add(value);
	}

	public void set(int newValue) {
		accumulator.setValue(newValue);
    }
	
}
