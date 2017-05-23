package au.csiro.variantspark.work

import org.apache.spark.sql.SparkSession

object DataSetTest {
    def main(argv:Array[String]) {
      val spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate() 
      import spark.implicits._
      val ds = spark.createDataset(List((1,3), (3,3)))
      ds.printSchema()
      val dd = ds.rdd.zipWithIndex().toDS()
      ds.persist()
  }
}