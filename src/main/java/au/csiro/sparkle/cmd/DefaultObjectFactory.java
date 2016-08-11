package au.csiro.sparkle.cmd;

import java.util.HashMap;
import java.util.Map;

import au.csiro.sparkle.common.ObjectFactory;
import au.csiro.sparkle.common.ObjectProvider;

public class DefaultObjectFactory<T> implements ObjectFactory<T>{

	static class ClassProvider<T> extends ObjectProvider<T>{
		private final Class<? extends T> clazz;

		public ClassProvider(Class<? extends T> clazz) {
	        this.clazz = clazz;
        }
		@SuppressWarnings("unchecked")
        public ClassProvider(String className) {
	        try {
	            this.clazz = (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException ex) {
            	throw new RuntimeException(ex);
            }
	    //    if (!CmdApp.class.isAssignableFrom(clazz)) {
	    //    	throw new IllegalArgumentException("Class: " + className  + " is not a CmdApp");
	    //    }
        }

		@Override
        public T provide() {
			try {
	           return  clazz.newInstance();
            } catch (InstantiationException ex) {
            	throw new RuntimeException(ex);
            } catch (IllegalAccessException ex) {
            	throw new RuntimeException(ex);
            }
        }
	}	
	protected Map<String, ObjectProvider<T>> modules  = new HashMap<>();
	
	public void registerClass(String name, Class<? extends T> clazz) {
		register(name, new ClassProvider<T>(clazz));
	}
	public  void register(String name, ObjectProvider<T> provider) {
		modules.put(name, provider);
	}
	@Override
    public T create(String clazzName) {
		return modules.computeIfAbsent(clazzName, name->  new ClassProvider<T>(name)).provide();
    }
	
}
