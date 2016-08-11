package au.csiro.sparkle.common.args4j;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.stream.Collectors;

import org.kohsuke.args4j.CmdLineParser;

import au.csiro.sparkle.cmd.DefaultObjectFactory;

public class ArgsObjectFactory<T> extends DefaultObjectFactory<T> implements HasUsage {

	@Override
    public String getShortUsage() {
		return modules.keySet().stream().collect(Collectors.joining("|"));
    }

	@Override
    public void printUsage(OutputStream out) {
		final PrintStream pout = new PrintStream(out);
		modules.forEach((name,provider) -> {
			pout.print(name);
			CmdLineParser parser = new CmdLineParser(provider.provide());
			parser.printSingleLineUsage(out);
			pout.println();
			parser.printUsage(out);
		});
    }
}
