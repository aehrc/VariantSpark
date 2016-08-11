package au.csiro.sparkle.common;

public abstract class ObjectProvider<T> {
	public abstract T provide();

	@SuppressWarnings("unchecked")
    public static <T> T get(Object obj) {
		return (obj instanceof ObjectProvider)?((ObjectProvider<T>)obj).provide():(T)obj;
	}
}
