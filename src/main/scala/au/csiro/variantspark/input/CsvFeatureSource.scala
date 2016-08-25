package au.csiro.variantspark.input

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import java.io.FileInputStream
import com.github.tototoshi.csv.CSVReader

class CsvFeatureSource(val fileName:String)(implicit sc:SparkContext) extends FeatureSource {
  //TODO just for now to it an in memory thing
  lazy val headerAndLines:(List[String], Seq[Feature]) = {
    LoanUtils.withCloseable(CSVReader.open(fileName)) { reader => 
      // drop the first element is it's empty 
      val header = reader.readNext().get.tail
      val data = reader.iterator.map(l => Feature(l.head, l.tail.map(_.toInt).toArray)).toList
      (header, data)
    }
  }
  def sampleNames:List[String] = headerAndLines._1
  def features():RDD[Feature] = sc.parallelize(headerAndLines._2)
}