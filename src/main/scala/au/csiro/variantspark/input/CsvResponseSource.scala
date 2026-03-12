package au.csiro.variantspark.input

import java.io.InputStreamReader
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import org.apache.hadoop.conf.Configuration
import au.csiro.variantspark.utils.HdfsPath
import scala.reflect.ClassTag

class CsvResponseSource[T: ClassTag](val fileName: String, val columnName: String,
    val convert: String => T)(implicit hadoopConf: Configuration)
    extends ResponseSource[T] {

  lazy val responseMap: Map[String, T] = {
    LoanUtils.withCloseable(CSVReader.open(new InputStreamReader(HdfsPath(fileName).open()))) {
      reader =>
        // we expect this to be small
        // so local read should be fine
        val header = reader.readNext().get
        val columnIndex = header.indexOf(columnName)
        reader.iterator.map(row => (row.head, convert(row(columnIndex)))).toMap
    }
  }

  def getResponses(sampleIds: Seq[String]): Array[T] = sampleIds.map(responseMap(_)).toArray
}
