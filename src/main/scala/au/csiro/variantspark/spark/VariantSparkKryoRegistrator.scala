package au.csiro.variantspark.spark

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

/**
  * Kryo registrator for VariantSpark classes.
  *
  * Configure in SparkSession:
  *   .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  *   .config("spark.kryo.registrator", "au.csiro.variantspark.spark.VariantSparkKryoRegistrator")
  */
class VariantSparkKryoRegistrator extends KryoRegistrator {
  // scalastyle:off classforname
  override def registerClasses(kryo: Kryo): Unit = {

    // === Input/VCF classes ===
    kryo.register(classOf[au.csiro.variantspark.input.Variant])
    kryo.register(classOf[au.csiro.variantspark.input.HeaderAndVersion])
    kryo.register(classOf[au.csiro.variantspark.input.DefVariantToFeatureConverter])
    kryo.register(classOf[au.csiro.variantspark.input.ModeImputationStrategy])
    kryo.register(Class.forName("au.csiro.variantspark.input.ZeroImputationStrategy$"))
    kryo.register(Class.forName("au.csiro.variantspark.input.DisabledImputationStrategy$"))

    // === Data classes (heavily serialized in RDDs) ===
    kryo.register(classOf[au.csiro.variantspark.data.StdFeature])
    kryo.register(classOf[au.csiro.variantspark.data.BoundedOrdinalVariable])
    kryo.register(classOf[au.csiro.variantspark.data.BoundedNominalVariable])
    kryo.register(Class.forName("au.csiro.variantspark.data.NominalVariable$"))
    kryo.register(Class.forName("au.csiro.variantspark.data.OrdinalVariable$"))
    kryo.register(Class.forName("au.csiro.variantspark.data.ContinuousVariable$"))
    kryo.register(Class.forName("au.csiro.variantspark.data.DiscreteVariable$"))
    kryo.register(classOf[au.csiro.variantspark.data.VectorData])
    kryo.register(classOf[au.csiro.variantspark.data.IntArrayData])
    kryo.register(classOf[au.csiro.variantspark.data.ByteArrayData])

    // === Decision Tree / Random Forest (serialized in model and during training) ===
    kryo.register(classOf[au.csiro.variantspark.algo.SplitInfo])
    kryo.register(classOf[au.csiro.variantspark.algo.SubsetInfo])
    kryo.register(classOf[au.csiro.variantspark.algo.VarSplitInfo])
    kryo.register(classOf[au.csiro.variantspark.algo.LeafNode])
    kryo.register(classOf[au.csiro.variantspark.algo.SplitNode])
    kryo.register(classOf[au.csiro.variantspark.algo.DecisionTreeModel])
    kryo.register(classOf[au.csiro.variantspark.algo.DecisionTreeParams])
    kryo.register(classOf[au.csiro.variantspark.algo.RandomForestModel])
    kryo.register(classOf[au.csiro.variantspark.algo.RandomForestMember])
    kryo.register(classOf[au.csiro.variantspark.algo.RandomForestParams])
    kryo.register(classOf[au.csiro.variantspark.algo.VotingAggregator])
    kryo.register(classOf[au.csiro.variantspark.algo.DeterministicMerger])
    kryo.register(classOf[au.csiro.variantspark.algo.RandomizingMergerMurmur3])
    kryo.register(classOf[au.csiro.variantspark.algo.StdVariableSplitter])
    kryo.register(classOf[au.csiro.variantspark.algo.AirVariableSplitter])
    kryo.register(classOf[au.csiro.variantspark.algo.ThresholdIndexedSplitter])
    kryo.register(Class.forName("au.csiro.variantspark.algo.GiniImpurity$"))

    // === External model classes (for serialization/export) ===
    kryo.register(classOf[au.csiro.variantspark.external.Leaf])
    kryo.register(classOf[au.csiro.variantspark.external.Split])
    kryo.register(classOf[au.csiro.variantspark.external.OOBInfo])
    kryo.register(classOf[au.csiro.variantspark.external.Tree])
    kryo.register(classOf[au.csiro.variantspark.external.Forest])

    // === Utility classes ===
    kryo.register(classOf[au.csiro.variantspark.utils.FactorVariable])
    kryo.register(classOf[au.csiro.variantspark.algo.PairWiseAggregator])

    // === Primitive arrays (very common) ===
    kryo.register(classOf[Array[Byte]])
    kryo.register(classOf[Array[Int]])
    kryo.register(classOf[Array[Long]])
    kryo.register(classOf[Array[Double]])
    kryo.register(classOf[Array[Float]])
    kryo.register(classOf[Array[String]])
    kryo.register(classOf[Array[Array[Byte]]])
    kryo.register(classOf[Array[Array[Int]]])
    kryo.register(classOf[Array[Array[Double]]])

    // === Array of VariantSpark types ===
    kryo.register(classOf[Array[au.csiro.variantspark.input.Variant]])
    kryo.register(classOf[Array[au.csiro.variantspark.data.StdFeature]])
    kryo.register(classOf[Array[au.csiro.variantspark.algo.RandomForestMember]])

    // === Scala standard types ===
    kryo.register(classOf[scala.collection.immutable.List[_]])
    kryo.register(classOf[scala.collection.immutable.::[_]])
    kryo.register(classOf[scala.collection.mutable.ArrayBuffer[_]])
    kryo.register(classOf[scala.collection.immutable.Map[_, _]])
    kryo.register(classOf[scala.collection.immutable.Set[_]])
    kryo.register(Class.forName("scala.None$"))
    kryo.register(Class.forName("scala.collection.immutable.Nil$"))
    kryo.register(classOf[Some[_]])
    kryo.register(classOf[scala.Tuple2[_, _]])
    kryo.register(classOf[scala.Tuple3[_, _, _]])

    // === Spark ML Vector types ===
    kryo.register(classOf[org.apache.spark.ml.linalg.DenseVector])
    kryo.register(classOf[org.apache.spark.ml.linalg.SparseVector])
  }
  // scalastyle:on classforname
}
