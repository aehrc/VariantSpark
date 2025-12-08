package au.csiro.variantspark.utils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.io.IOException;
import htsjdk.samtools.util.BlockCompressedInputStream; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileUtils {
	
	/**
	 * 
	 * @param file: an input file
	 * @return true if input file is a valid BGZIP 
	 */	
	public static boolean isBGZFile(String filePath, Configuration conf) {
		Path path = new Path(filePath);
		try (FileSystem fs = FileSystem.get(conf)) {
			if (!fs.exists(path)) {
				return false;
			}
			try (BufferedInputStream bis = new BufferedInputStream(fs.open(path))) {
				return BlockCompressedInputStream.isValidFile(bis);
			}
		} catch (IOException e) {
			return false;
		}
	}
}
