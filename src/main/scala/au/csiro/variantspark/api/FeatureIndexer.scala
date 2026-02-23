package au.csiro.variantspark.api

import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.utils.MurMur3Hash
import org.apache.spark.{Partitioner, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/** Partitioner that assigns features to partitions based on a deterministic
  * MurMur3 hash of the feature label. This ensures the same feature always
  * lands in the same partition regardless of input ordering or Spark task
  * scheduling, which is critical for reproducible zipWithIndex assignments.
  */
class LabelHashPartitioner(override val numPartitions: Int) extends Partitioner {
  require(numPartitions > 0, s"Number of partitions must be positive, got $numPartitions")

  override def getPartition(key: Any): Int = {
    val label = key.asInstanceOf[String]
    val hash = MurMur3Hash.hashString(label)
    // Ensure non-negative mod
    val partition = ((hash % numPartitions) + numPartitions) % numPartitions
    partition
  }

  override def equals(other: Any): Boolean = other match {
    case p: LabelHashPartitioner => p.numPartitions == numPartitions
    case _ => false
  }

  override def hashCode(): Int = numPartitions
}

/** Utility for deterministically indexing features. Ensures that the same
  * features always receive the same indices by using a MurMur3 hash-based
  * partitioner on the feature label, sorting within partitions, then
  * assigning sequential indices via zipWithIndex.
  */
object FeatureIndexer {

  /** Deterministically repartitions and indexes features.
    *
    * The operation sequence is:
    *   1. keyBy(_.label) - extract the feature label as the key
    *   2. repartitionAndSortWithinPartitions(LabelHashPartitioner)
    *      - hash-based partitioning + lexicographic sort within each partition
    *   3. values - drop the key, keep Feature
    *   4. zipWithIndex - assign sequential Long indices
    *   5. persist(MEMORY_AND_DISK) - cache for reuse across tree batches
    *
    * @param features the unindexed feature RDD
    * @param nPartitions number of partitions (0 = use 2× defaultParallelism)
    * @return persisted RDD[(Feature, Long)] with deterministic index mapping
    */
  def index(features: RDD[Feature], nPartitions: Int = 0): RDD[(Feature, Long)] = {
    val sc = features.sparkContext
    val targetPartitions = if (nPartitions > 0) nPartitions else sc.defaultParallelism * 2

    implicit val labelOrdering: Ordering[String] = Ordering.String

    val indexed = features
      .keyBy(_.label)
      .repartitionAndSortWithinPartitions(new LabelHashPartitioner(targetPartitions))
      .values
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK)
    indexed
  }
}
