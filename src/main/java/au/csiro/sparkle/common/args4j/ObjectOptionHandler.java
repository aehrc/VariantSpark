package au.csiro.sparkle.common.args4j;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.OptionDef;
import org.kohsuke.args4j.spi.OptionHandler;
import org.kohsuke.args4j.spi.Parameters;
import org.kohsuke.args4j.spi.Setter;

import au.csiro.sparkle.common.ObjectFactory;
import au.csiro.sparkle.common.ObjectProvider;

public class ObjectOptionHandler<T> extends OptionHandler<T>{

	
	private final static Map<Class<?>,ObjectFactory<?>> classFactories = new HashMap<Class<?>,ObjectFactory<?>>();
	
	public static synchronized void registerFactoryForClass(Class<?> clazz, ObjectFactory<?> factory) {
		classFactories.put(clazz, factory);
	}
	
	private static synchronized ObjectFactory<?> getFactory(Class<?> clazz) {
		ObjectFactory<?> factory = classFactories.get(clazz);
		if (factory == null) {
			throw new RuntimeException("Cannot find factory for class: " + clazz);
		}
		return factory;
	}

	
	public ObjectOptionHandler(CmdLineParser parser, OptionDef option, Setter<? super Object> setter) {
	    super(parser, option, setter);
	    // TODO Auto-generated constructor stub
    }

	@SuppressWarnings("unchecked")
	@Override
    public int parseArguments(Parameters params) throws CmdLineException {
		int paramIndex = 0;
		String clazzName = params.getParameter(paramIndex++);
		Object bean = getFactory(setter.getType()).create(clazzName);
		// find '--' in arg list
		List<String> args = new ArrayList<String>(params.size());
		while(paramIndex < params.size() && !"--".equals(params.getParameter(paramIndex++))) {
			args.add(params.getParameter(paramIndex-1));
		}	
		new CmdLineParser(bean).parseArgument(args);
		setter.addValue(ObjectProvider.<T>get(bean));
		return paramIndex;
    }

	@Override
    public String getDefaultMetaVariable() {
	    return String.format("%s --", setter.getType().getSimpleName());
    }
	public void printUsage(OutputStream out) {
		PrintStream pout = new PrintStream(out);
		pout.format("Usage for %s:\n", setter.getType().getSimpleName());
		ObjectFactory<?> factory = getFactory(setter.getType());
		if (factory instanceof HasUsage){
			((HasUsage)factory).printUsage(out);
		}
	}
}
