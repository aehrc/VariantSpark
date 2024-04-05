package au.csiro.variantspark.utils;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.io.IOException;
import htsjdk.samtools.util.BlockCompressedInputStream; 

public class FileUtils {
	
	/**
	 * 
	 * @param file: an input file
	 * @return true if input file is BGZIP by check the first two byte of input file 
	 */	
	public static boolean isBGZFile(String filePath) {
		/**
		 * .vcf.bgz is type of GZP file, work well with BlockCompressedInputStream
		 * .vcf.gz is also GZP file but get java.lang.OutOfMemoryError at java.io.InputStreamReader.read(InputStreamReader.java:184)
		 * .vcf.bz2 is not GZP file and get java.lang.OutOfMemoryError at java.io.InputStreamReader.read(InputStreamReader.java:184)
		 * .vcf is not GZP file and get htsjdk.samtools.SAMFormatException: at header from java.io.BufferedReader.readLine(BufferedReader.java:389)
		*/
	    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(filePath))) {
	        boolean isValid = BlockCompressedInputStream.isValidFile(bufferedInputStream);
	        return isValid;
	    } catch (IOException e) {
	        //handle exception for non proper bgzip file
	        return false;
	    }
	}
}
