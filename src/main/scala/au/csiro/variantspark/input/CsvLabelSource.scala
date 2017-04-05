package au.csiro.variantspark.input

import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.File
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.InputStreamReader
import org.apache.hadoop.conf.Configuration

class CsvLabelSource(val fileName:String, val columnName:String)(implicit hadoopConf:Configuration) extends LabelSource {
  
  lazy val labelMap = {
    val path  = new Path(fileName)
    val fs = path.getFileSystem(hadoopConf)
    LoanUtils.withCloseable(CSVReader.open(new InputStreamReader(fs.open(path)))) { reader => 
     
      // we expect this to be small 
      // so local read should be fine
            
      val header = reader.readNext().get
      val columnIndex = header.indexOf(columnName)
      reader.iterator.map(row => (row(0), row(columnIndex).toInt)).toMap
    }
  }
  
  def getLabels(labels:Seq[String]):Array[Int] = labels.map(labelMap(_)).toArray
}