package au.csiro.variantspark.input

import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.File

class CsvLabelSource(val fileName:String, val columnName:String) extends LabelSource {
  
  lazy val labelMap = {
    LoanUtils.withCloseable(CSVReader.open(new File(fileName))) { reader => 
      val columnIndex = reader.readNext().get.indexOf(columnName)
      reader.iterator.map(row => (row(0), row(columnIndex).toInt)).toMap
    }
  }
  
  def getLabels(labels:Seq[String]):Array[Int] = labels.map(labelMap(_)).toArray
}