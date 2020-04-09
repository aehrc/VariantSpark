package au.csiro.variantspark.test

import java.io.File

/**
  * Access to test data files
  */
trait TestData {
  import TestData._
  def subdir: String

  def dataDir = rawDataFile(subdir)
  def actualDir = rawActualFile(subdir)

  def expected(fileName: String): String = new File(dataDir, fileName).getPath
  def data(fileName: String): String = new File(dataDir, fileName).getPath
  def actual(fileName: String): String = new File(actualDir, fileName).getPath
}

object TestData {
  val RootDataDir = new File("src/test/data")
  val RootActualDir = new File("target/")

  def rawDataFile(fileName: String): File = new File(RootDataDir, fileName)
  def rawActualFile(fileName: String): File = new File(RootActualDir, fileName)
}
