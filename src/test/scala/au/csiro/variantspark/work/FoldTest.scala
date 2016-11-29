package au.csiro.variantspark.work

import org.junit.Assert._
import org.junit.Test;
import au.csiro.variantspark.test.SparkTest
import org.apache.spark.TaskContext



object FoldTest {
   def add(i:Long, j:Long):Long = {
    println(s"Agg: ${i}, ${j}")
    i
  } 
}

case class Add() {
  
  lazy val k = {
    println(s"K: ${TaskContext.get()}")
    0
  }
  def apply(i:Long, j:Long):Long  = {
    k
  }
}


class FoldTest extends SparkTest {
 
  
  @Test
  def testFolds() {
    val func = Add()
    val result = sc.range(0, 1000, numSlices = 10).fold(0L)(func.apply)
    println("Result: "  + result)
  }
}