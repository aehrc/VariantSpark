package au.csiro.sparkle.cmd;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import au.csiro.sparkle.common.ObjectProvider;

public abstract class MultiCmdApp extends CmdApp {
	
	static class ClassProvider<T extends CmdApp> extends ObjectProvider<T>{
		private final Class<T> clazz;

		public ClassProvider(Class<T> clazz) {
	        this.clazz = clazz;
        }
		@SuppressWarnings("unchecked")
        public ClassProvider(String className) {
	        try {
	            this.clazz = (Class<T>) Class.forName(className);
            } catch (ClassNotFoundException ex) {
            	throw new RuntimeException(ex);
            }
	        if (!CmdApp.class.isAssignableFrom(clazz)) {
	        	throw new IllegalArgumentException("Class: " + className  + " is not a CmdApp");
	        }
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
	private final Map<String, ObjectProvider<? extends CmdApp>> modules  = new HashMap<>();
	
	public  <T extends CmdApp> void registerClass(String name, Class<T> clazz) {
		register(name, new ClassProvider<T>(clazz));
	}
	public  <T extends CmdApp> void register(String name, ObjectProvider<T> provider) {
		modules.put(name, provider);
	}
	
	@Override
	public void run(String[] args) {
		if (args.length < 1) {
			usage();
		} else {
			runCommandOrClass(args[0], Stream.of(args).skip(1).toArray(n -> new String[n]));
		}
	}
	private void runCommandOrClass(String cmdOrClass, String[] args) {
	    CmdApp.runApp(args,
	    	modules.computeIfAbsent(cmdOrClass, clazz-> new ClassProvider<CmdApp>(clazz)).provide());
    }
	
	protected String getAppName() {
		return getClass().getName();
	}
	
	@Override
	public void usage() {
	   System.out.println("Usage:");
	   System.out.format("\t%s [command|className] args*\n", getAppName());
	   System.out.println("Available commands are:");
	   modules.keySet().stream().sorted().forEach(cmd -> System.out.format("\t%s\n", cmd));
	   System.out.println("For help on a specific command use:");
	   System.out.format("\t<app-nane> command -h\n", getAppName());	   
    }
}
