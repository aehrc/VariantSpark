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
	public static boolean isInputBGZ(final File file) {
		 
		//.vcf.bgz is type of GZP file
		//.vcf.gz is also GZP file but get java.lang.OutOfMemoryError at java.io.InputStreamReader.read(InputStreamReader.java:184)
		//.vcf.bz2 is not GZP file and get java.lang.OutOfMemoryError at java.io.InputStreamReader.read(InputStreamReader.java:184)
		//.vcf is not GZP file and get htsjdk.samtools.SAMFormatException: at header from java.io.BufferedReader.readLine(BufferedReader.java:389)
		
		boolean isGzip = false; 
		try {			
			isGzip = isInputGZip(file);	//ture if .bgz or .gz		  		 
		} catch (IOException e) {}
		
		
		//if not gzip file, do following check
		if(isGzip) {
			
		    try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file))) {
		        bufferedInputStream.mark(100); // mark the current position
		        boolean isValid = BlockCompressedInputStream.isValidFile(bufferedInputStream);
		        bufferedInputStream.reset(); // reset back to the marked position
		        return isValid;
		    } catch (IOException e) {
		        // Handle the exception
		        return false;
		    }
					
//			try(final BlockCompressedInputStream bgzInputStream = new BlockCompressedInputStream(file)) {
//				System.out.println(" inside try block: start bufferReader ...");
//				BufferedReader reader = new BufferedReader(new InputStreamReader(bgzInputStream));
//				System.out.println(" inside try block: reader.readLine()... ");
//	            String line = reader.readLine();
//	            return line != null && !line.isEmpty();
//			} catch (Exception e) {
//				//file is not .vcf.bgz file 
//				//it will throw any type exception according to file type
//				//hence we try to catch any type exception
//				e.printStackTrace();
//				return false;
//			}
		}		
		
		return false; 

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
