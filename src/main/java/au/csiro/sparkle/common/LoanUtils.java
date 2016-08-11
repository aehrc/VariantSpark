package au.csiro.sparkle.common;

import java.io.Closeable;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.io.IOUtils;

public final class LoanUtils {

	public static <C extends Closeable> void withCloseable(final C in, ThrowingConsumer<C> func) {
		try {
			func.accept(in);
		} catch (Exception ex) {
			throw new RuntimeException(ex);		
		} finally {
			IOUtils.closeQuietly(in);
		}		
	}

	public static <C extends Closeable, T> T withCloseableFunc(final C in, ThrowingFunction<C,T> func) {
		try {
			return func.apply(in);
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		} finally {
			IOUtils.closeQuietly(in);
		}		
	}
	
	public static void withPrintWriter(final File in, ThrowingConsumer<PrintWriter> func) {
		try {
	        LoanUtils.withCloseable(new PrintWriter(new FileWriter(in)), func);
        } catch (IOException ex) {
	        throw new RuntimeException(ex);
        }		
	}

}
