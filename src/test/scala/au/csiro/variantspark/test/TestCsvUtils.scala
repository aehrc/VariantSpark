package au.csiro.variantspark.test

import java.io.FileReader
import java.lang.{Double => JDouble, Integer => JInteger}

import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import org.apache.commons.csv.CSVFormat

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

object TestCsvUtils {

  private val csvFormat: CSVFormat = CSVFormat.DEFAULT
    .builder()
    .setAllowMissingColumnNames(true)
    .setHeader()
    .setSkipHeaderRecord(true)
    .build()

  def readColumnToArray[T](fileName: String, columnName: String, converter: String => T)(
      implicit ct: ClassTag[T]): Array[T] = {
    LoanUtils.withCloseable(new FileReader(fileName)) { reader =>
      csvFormat.parse(reader).asScala.map(_.get(columnName)).map(converter).toArray
    }
  }

  def readColumnToIntArray(fileName: String, columnName: String): Array[Int] = {
    readColumnToArray(fileName, columnName, JInteger.parseInt)
  }

  def readColumnToDoubleArray(fileName: String, columnName: String): Array[Double] = {
    readColumnToArray(fileName, columnName, JDouble.parseDouble)
  }

  def readColumnToMap[T](fileName: String, columnName: String, converter: String => T)(
      implicit ct: ClassTag[T]): Map[String, T] = {
    LoanUtils.withCloseable(new FileReader(fileName)) { reader =>
      csvFormat.parse(reader).asScala.map(r => (r.get(0), converter(r.get(columnName)))).toMap
    }
  }

  def readColumnToDoubleMap(fileName: String, columnName: String): Map[String, Double] = {
    readColumnToMap(fileName, columnName, JDouble.parseDouble)
  }

}
