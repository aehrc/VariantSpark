package au.csiro.sparkle.common;

import java.util.function.Function;

public abstract class ThrowingFunction<T, R> {
	public abstract R apply(T t) throws Exception;

	public static <T,R> Function<T,R> rethrow(final ThrowingFunction<T,R> c) {
		return new Function<T,R>() {

			@Override
            public R apply(T t) {
				try {
					return c.apply(t);
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		};
	}
}
