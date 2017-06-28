package au.csiro.sparkle.common;

import java.util.function.Consumer;

public abstract class ThrowingConsumer<T> {
	public abstract void accept(T t) throws Exception;

	public static <T> Consumer<T> wrap(final ThrowingConsumer<T> c) {
		return t -> {
            try {
                c.accept(t);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        };
	}
}
