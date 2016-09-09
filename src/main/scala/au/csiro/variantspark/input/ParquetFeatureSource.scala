package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.FileInputStream
import com.github.tototoshi.csv.CSVReader
import au.csiro.pbdava.ssparkle.spark.SparkUtils._
import com.github.tototoshi.csv.CSVFormat
import com.github.tototoshi.csv.CSVParser
import com.github.tototoshi.csv.DefaultCSVFormat
import org.apache.hadoop.fs.FileSystem
import au.csiro.pbdava.ssparkle.common.utils.SerialUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SQLContext



case class ParquetFeatureSource(inputPath:String)(implicit sc: SparkContext) extends FeatureSource {

  override lazy val sampleNames:List[String] = {
    val fs  = FileSystem.get(sc.hadoopConfiguration)
    SerialUtils.read(fs.open(new Path(inputPath, "_columns")))
  }

  def features():RDD[Feature] = {
     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
     // this is used to implicitly convert an RDD to a DataFrame.
     import sqlContext.implicits._ 
     val rawDF = sqlContext.read.parquet(inputPath)
     rawDF.rdd.map( r => Feature(r.getString(0), r.getAs(1)))
  }
}