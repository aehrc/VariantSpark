package au.csiro.variantspark.input

import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStreamReader

class CsvLabelSource(val fileName:String, val columnName:String)(implicit fs:FileSystem) extends LabelSource {
  
  lazy val labelMap = {
    LoanUtils.withCloseable(CSVReader.open(new InputStreamReader(fs.open(new Path(fileName))))) { reader => 
      val header = reader.readNext().get
      println(header)
      val columnIndex = header.indexOf(columnName)
      reader.iterator.map(row => (row(0), row(columnIndex).toInt)).toMap
    }
  }
  
  def getLabels(labels:Seq[String]):Array[Int] = labels.map(labelMap(_)).toArray
}