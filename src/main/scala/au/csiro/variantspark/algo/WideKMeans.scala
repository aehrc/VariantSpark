package au.csiro.variantspark.algo

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

object WideKMeans {
  def square(d:Double) = d*d
}

/**
  * A class to implement a ''Wide K-Means''.
  *
  * Specify the `k` and `iterations`
  * then access the fields like this:

  * {{{
  * val wKM = WideKMeans(5, 10)
  * val clusterCenters = wKM.run(data)
  * val resultingClusters = wKM.assignClusters(data, clusterCenters)
  * }}}
  *
  *
  *
  * @run Computes values in each cluster
  * @assignClusters Assigns the values found in the cluster to an output Array
  * @param k Number of desired clusters
  * @param iterations The number of iterations requested
  */

class WideKMeans(k:Int, iterations:Int) {

  /**
    * Specify the input data
    *
    * 1. Splits the vectors into k dense vectors
    * 2. Finds the Euclidean distance between the new center and the values on the graph
    * 3. Assigns values with lowest distances to the clusters
    * 4. Creates new dense vectors with the values found
    * 5. Repeats till the number of iterations is met
    *
    * @param data Input an RDD[Vector]
    * @return Returns the cluster centers as a RDD[Vector]
    */
  def run(data: RDD[Vector]):RDD[Vector] = {

    val numOfClusters = k
    val iter = iterations
    
    val dimension = data.first().size

    val initialCenters = data.map { vector =>
        val array = vector.toArray
        Vectors.dense( Range(0,numOfClusters).map(i => array(i)).toArray)
    }

    var clusterCenters = initialCenters

    for(i <- Range(0,iter)) {

      val clusterAssignment = data.zip(clusterCenters)
        .aggregate(Array.fill(dimension)(Array.fill(numOfClusters)(0.0)))(

            (distances, vectorsAndCenters) => {

              val dv = vectorsAndCenters._1.toArray
              for(i <- Range(0,dimension); j<- Range(0,numOfClusters)) {
                distances(i)(j) += WideKMeans.square(dv(i) - vectorsAndCenters._2(j))
              }
              distances

            } ,

            (distance1, distance2) => {

              for(i <- Range(0,dimension); j<- Range(0,numOfClusters)) {
                distance1(i)(j) += distance2(i)(j)
              }
              distance1

            }

        )
      .map(v => v.zipWithIndex.min._2)
      
      val clusterSizes = Array.fill(numOfClusters)(0)
      clusterAssignment.foreach(c=> clusterSizes(c) += 1)
      
      val br_clusterAssignment = data.context.broadcast(clusterAssignment)
      val br_clusterSizes = data.context.broadcast(clusterSizes)

      val newClusterCenters = data.map( vector => {

          val contributions:Array[Double] = Array.fill(numOfClusters)(0.0)
          val clusterAssignment =  br_clusterAssignment.value
          val clusterSizes = br_clusterSizes.value

          Range(0,dimension).foreach(i => contributions(clusterAssignment(i)) += vector(i)/clusterSizes(clusterAssignment(i)))
          Vectors.dense(contributions)

        })

      clusterCenters = newClusterCenters
    }
    clusterCenters
  }

  /**
    * Assigns points to specific clusters using vectors found from clusterCentres
    *
    *
    * @param data original data input
    * @param clusterCenters result of the run function
    * @return returns the cluster assignments
    */
  def assignClusters(data:RDD[Vector], clusterCenters:RDD[Vector]): Array[Int] = {

     val dimension = data.first().size
     val k = clusterCenters.first().size

     val clusterAssignment = data.zip(clusterCenters)
        .aggregate(Array.fill(dimension)(Array.fill(k)(0.0)))(

            (distances, vectorsAndCenters) => {

              val dv = vectorsAndCenters._1.toArray
              for(i <- Range(0,dimension); j<- Range(0,k)) {
                distances(i)(j) += WideKMeans.square(dv(i) - vectorsAndCenters._2(j))
              }
              distances

            } ,

            (distance1, distance2) => {

              for(i <- Range(0,dimension); j<- Range(0,k)) {
                distance1(i)(j) += distance2(i)(j)
              }
              distance1

            }

        )
      .map(v => v.zipWithIndex.min._2)

      clusterAssignment
  }
  
}