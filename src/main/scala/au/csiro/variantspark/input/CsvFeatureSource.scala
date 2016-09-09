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

class DefaultCSVFormatSpec extends DefaultCSVFormat with Serializable

case object DefaultCSVFormatSpec extends DefaultCSVFormatSpec

case class CsvFeatureSource(data:RDD[String], csvFormat:CSVFormat = DefaultCSVFormatSpec) extends FeatureSource {
  lazy val fileHeader:String = data.first
  lazy val br_header = data.context.broadcast(fileHeader)
  def sampleNames:List[String] = new CSVParser(csvFormat).parseLine(fileHeader).get.tail
  def features():RDD[Feature] = {
    val local_br_header = this.br_header
    data.mapPartitions { it =>
       val header = local_br_header.value
       val csvParser = new CSVParser(csvFormat)
       it.filter(!_.equals(header)).map(csvParser.parseLine(_).get).map(l => Feature(l.head, l.tail.map(_.toByte).toArray))
    }
  }
}