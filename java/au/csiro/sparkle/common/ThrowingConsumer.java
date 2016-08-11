package au.csiro.sparkle.common;

import java.util.function.Consumer;

public interface ThrowingConsumer<T> {
	void accept(T t) throws Exception;

	public static <T> Consumer<T> wrap(final ThrowingConsumer<T> c) {
		return new Consumer<T>() {

			@Override
			public void accept(T t) {
				try {
					c.accept(t);
				} catch (Exception ex) {
					throw new RuntimeException(ex);
				}
			}
		};
	}
}
