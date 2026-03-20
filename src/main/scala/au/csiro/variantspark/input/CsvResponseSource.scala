package au.csiro.variantspark.input

import java.io.InputStreamReader
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import org.apache.hadoop.conf.Configuration
import au.csiro.variantspark.utils.HdfsPath
import scala.reflect.ClassTag

class CsvResponseSource(val fileName: String, val columnName: String)(
    implicit hadoopConf: Configuration)
    extends ResponseSource {

  lazy val rawResponseMap: Map[String, String] = {
    LoanUtils.withCloseable(CSVReader.open(new InputStreamReader(HdfsPath(fileName).open()))) {
      reader =>
        // we expect this to be small
        // so local read should be fine
        val header = reader.readNext().get
        val columnIndex = header.indexOf(columnName)
        reader.iterator.map(row => (row.head, row(columnIndex))).toMap
    }
  }

  def getResponses[T: ClassTag](sampleIds: Seq[String], convert: String => T): Array[T] =
    sampleIds.map(id => convert(rawResponseMap(id))).toArray
}
