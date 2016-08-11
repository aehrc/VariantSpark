package au.csiro.sparkle.common;

public interface ObjectFactory<T> {
	T create(String clazzName);
}
