package au.csiro.pbdava.ssparkle.common.utils

import com.github.tototoshi.csv.CSVWriter
import java.io.InputStream
import java.io.OutputStream
import com.github.tototoshi.csv.DefaultCSVFormat
import java.io.File

object CSVUtils {
  
  implicit object MyFormat extends DefaultCSVFormat {
    override val lineTerminator = System.lineSeparator()
  }
  
  def withStream(os:OutputStream)(func: CSVWriter => Unit) {
    LoanUtils.withCloseable(CSVWriter.open(os))(func)  
  }

  def withPath(path:String)(func: CSVWriter => Unit) {
    LoanUtils.withCloseable(CSVWriter.open(path))(func)  
  }

  def withFile(file:File)(func: CSVWriter => Unit) {
    LoanUtils.withCloseable(CSVWriter.open(file))(func)  
  }

  
}