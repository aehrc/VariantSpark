package au.csiro.sparkle.spark;

import org.apache.commons.lang.builder.ReflectionToStringBuilder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import au.csiro.sparkle.common.args4j.ArgsApp;

public abstract class SparkleJavaApp extends ArgsApp {

	@Override
	protected void run() throws Exception {
		
		long startTime = System.currentTimeMillis();
		Logger.getLogger(this.getClass())
			.info("Running: {}: " + ReflectionToStringBuilder.toString(this));
		
		JavaSparkContext sc = null;
		try {
			sc = new JavaSparkContext(new SparkConf().setAppName(getClass().getName()));
			runWithSpark(sc);
		} finally {
			if (sc != null) {
				sc.stop();
			}
		}
		System.out.println("App took: " + (System.currentTimeMillis() - startTime) + "ms");
	}
	
	protected abstract void runWithSpark(JavaSparkContext sc) throws Exception;
}
