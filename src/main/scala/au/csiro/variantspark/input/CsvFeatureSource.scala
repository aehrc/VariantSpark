package au.csiro.variantspark.input

import com.github.tototoshi.csv.{CSVFormat, CSVParser, DefaultCSVFormat}
import org.apache.spark.rdd.RDD

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