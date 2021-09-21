package au.csiro.sparkle.spark;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

public class AppContext {

  private final JavaSparkContext sc;
  private final Map<Enum<?>, LongAccumulator> counters = new LinkedHashMap<>();

  public AppContext(JavaSparkContext sc) {
    super();
    this.sc = sc;
  }

  public JavaSparkContext getSparkContext() {
    return sc;
  }

  public synchronized LongAccumulator getCounter(Enum<?> counterKey) {
    return counters.computeIfAbsent(counterKey, k -> sc.sc().longAccumulator(k.toString()));
  }


  public static <T> Function<T, T> countInto(final LongAccumulator counter) {
    return t -> {
      counter.add(1);
      return t;
    };
  }

  public synchronized void printAllCounters() {
    counters
        .forEach((k, v) -> System.out.format("%s:%s = %d\n", k.getClass().getName(), k, v.value()));
  }

  public void close() {
    sc.close();
  }
}
