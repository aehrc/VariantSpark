package au.csiro.variantspark.test

import org.saddle.io.CsvParser
import org.saddle.io.CsvFile
import org.saddle.Frame


object TestCsvUtils {
  def readFrame(fileName:String):Frame[String, String, String] = CsvParser.parse(CsvFile(fileName)).withRowIndex(0).withColIndex(0)
}