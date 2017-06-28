package au.csiro.sparkle.common.args4j;

import java.util.Date;
import java.util.LinkedHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import au.csiro.sparkle.cmd.CmdApp;

public abstract class ArgsApp extends CmdApp {

	static {
		CmdLineParser.registerHandler(Date.class, DateOptionHandler.class);		
	}
	
	public static CmdLineParser buildParser(ArgsApp app) {
		return new CmdLineParser(app);
	}
	
	protected abstract void run() throws Exception;
	
	@Override
	protected  void usage() {
		CmdLineParser parser = buildParser(this);
		System.out.println("Usage:");
		System.out.format("%s ", this.getClass().getSimpleName());
		parser.printSingleLineUsage(System.out);
		System.out.println();
		parser.printUsage(System.out);
		parser.getOptions().stream()
			.filter(oh -> oh instanceof ObjectOptionHandler<?>)
			.collect(Collectors.toMap(h -> h.setter.getType(),
					Function.identity(), (h1,h2) -> h1, ()->new LinkedHashMap<>()))
			.values().stream()
			.forEach(oh -> ((ObjectOptionHandler<?>)oh).printUsage(System.out));
	}

    @Override
	protected  void run(String[] args)  throws Exception {
        CmdLineParser parser = buildParser(this);
        try {
        	parser.parseArgument(args);
            run();        	
        } catch (CmdLineException ex) {
        	System.out.println("ERROR: " + ex.getMessage());
        	usage();
        	System.exit(1);
        }
	}
	
}
