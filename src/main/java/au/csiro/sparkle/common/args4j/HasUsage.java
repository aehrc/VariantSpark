package au.csiro.sparkle.common.args4j;

import java.io.OutputStream;

public interface HasUsage {
	String getShortUsage();
	void printUsage(OutputStream out);
}
