package au.csiro.variantspark.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.IOException;
import htsjdk.samtools.util.BlockCompressedInputStream; 

public class FileUtils {
	
	/**
	 * 
	 * @param file: an input file
	 * @return true if input file is BGZIP by check the first two byte of input file 
	 */	
	public static boolean isInputBGZ(final File file) {
		
		try(final BlockCompressedInputStream bgzInputStream = new BlockCompressedInputStream(file)) {
			BufferedReader reader = new BufferedReader(new InputStreamReader(bgzInputStream));
            String line = reader.readLine();
            return line != null && !line.isEmpty();
		} catch (IOException e) {
			//file is not .vcf.bgz file 
			return false;
		}
	}

}
