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
	        //bufferedInputStream.mark(100); // mark the current position
	        boolean isValid = BlockCompressedInputStream.isValidFile(bufferedInputStream);
	        //bufferedInputStream.reset(); // reset back to the marked position
	        return isValid;
	    } catch (IOException e) {
	        // Handle the exception
	        return false;
	    }
	}
	
	/**
	 * 
	 * @param file: an input file
	 * @return true if input file is Gzip by check the first two byte of input file 
	 * @throws IOException
	 */
	public static boolean isInputGZip(final File file) throws IOException {
		//final PushbackInputStream pb = new PushbackInputStream(input, 2);
		
		try(final InputStream input = new FileInputStream(file)){
			int header = input.read(); //read ID1
	        if(header == -1)   return false;	        
	
	        int b = input.read(); //read ID2
	        if(b == -1)  return false;
	        
	        //ID2 * 256 + ID1 = 35615
	        if( ( (b << 8) | header) == GZIPInputStream.GZIP_MAGIC) 
	            return true;	         
		}
	     
		return false;		
	} 
}
