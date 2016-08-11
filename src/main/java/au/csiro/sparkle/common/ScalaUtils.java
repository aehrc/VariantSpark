package au.csiro.sparkle.common;

import scala.Tuple2;

public final class ScalaUtils {
	public static <K,V> Tuple2<K,V> t2(K k, V v) {
		return new Tuple2<K,V>(k,v); 
	}
}
