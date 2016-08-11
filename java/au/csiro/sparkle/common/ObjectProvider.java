package au.csiro.sparkle.common;

public interface ObjectProvider<T> {
	T provide();

	@SuppressWarnings("unchecked")
    static <T> T get(Object obj) {
		return (obj instanceof ObjectProvider)?((ObjectProvider<T>)obj).provide():(T)obj;
	}
}
