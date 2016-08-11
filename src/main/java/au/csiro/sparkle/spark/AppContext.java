package au.csiro.sparkle.spark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaSparkContext;

public class AppContext {

	private final JavaSparkContext sc;
	private final Map<Enum<?>, Counter> counters = new LinkedHashMap<Enum<?>, Counter>();
	
	public AppContext(JavaSparkContext sc) {
	    super();
	    this.sc = sc;
    }

	public JavaSparkContext getSparkContext() {
		return sc;
	}
	public synchronized Counter getCounter(Enum<?> counterKey) {
		 return counters.computeIfAbsent(counterKey, k-> new Counter(sc.accumulator(0, k.toString())));
	}
	

	public static <T> Function<T,T> countInto(final Accumulator<Integer> counter) {
		return new Function<T,T>() {

			@Override
            public T apply(T t) {
				counter.add(1);
				return t;
            }
		};
	}
	
	public synchronized void printAllCounters() {
		counters.forEach( (k,v) -> System.out.format("%s:%s = %d\n",k.getClass().getName(), k, v.value()));
    }

	public void close() {
		sc.close();   
    }
}
