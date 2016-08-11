package au.csiro.sparkle.spark;

import java.io.File;
import java.io.IOException;

public interface SparkUtils {

	public static String mkOutputDir(String pathToDir) {
		return mkOutputDir(pathToDir, true);
	}

	public static String mkOutputDir(String pathToDir, boolean createParents) {
		return mkOutputDir(pathToDir, createParents, Boolean.parseBoolean(System.getProperty("sparkle.output.delete", "false")));
	}
	
	public static String mkOutputDir(String pathToDir, boolean createParents, boolean deleteIfExits) {
		final File outputDir = new File(pathToDir);
		final File parentDir = outputDir.getParentFile();
		if (parentDir.exists() && !parentDir.isDirectory()) {
			throw new RuntimeException("Parent dir of: " + pathToDir + " exists and is not a directory");
		}
		if (parentDir.exists() && createParents) {
			parentDir.mkdirs();
		}
		if (outputDir.exists() && deleteIfExits) {
			if (outputDir.isDirectory()) {
				try {
					org.apache.commons.io.FileUtils.deleteDirectory(outputDir);
				} catch (IOException ex) {
					throw new RuntimeException("Cannot delete directory: " + outputDir, ex);
				}
			} else {
				outputDir.delete();
			}
		}
		return outputDir.getAbsolutePath();
	}
}
