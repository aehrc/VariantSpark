package au.csiro.variantspark.algo

import au.csiro.pbdava.ssparkle.spark.SparkApp
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

object TestWideKMeans extends SparkApp {
  conf.setAppName("VCF cluster")

  
  def main(args:Array[String]) {
    println("Testign WideKMeans")
    
    
    val dims = 1000
    val centersNo = 5
    val samples = 30
    val clusterVariance = 0.005
    val centers = sc.parallelize(Range(0,dims).map(i => Vectors.dense(Array.fill(centersNo)(Math.random()))), 10)
    //centers.foreach(println)
    val clusterAssignment = Range(0,samples).map(i => Math.floor(Math.random()*centersNo).toInt).toList
    println(clusterAssignment)
    
    val data:RDD[Vector] = centers.map(v =>
      Vectors.dense(clusterAssignment.map(c => v(c) + (Math.random() * clusterVariance - clusterVariance/2)).toArray).toSparse
    )
    
    val test = data.cache().count()
    println(test)
    
    val kmeans = new WideKMeans(centersNo, 30)
    val result  = kmeans.run(data)
    
    println(kmeans.assignClusters(data, centers).toList)
    println(kmeans.assignClusters(data, result).toList)
    
    
  }
}