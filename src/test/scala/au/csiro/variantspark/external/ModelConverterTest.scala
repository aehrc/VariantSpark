package au.csiro.variantspark.external

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.algo.RandomForestMember
import au.csiro.variantspark.algo.DecisionTreeModel
import au.csiro.variantspark.algo.LeafNode
import au.csiro.variantspark.algo._

class ModelConverterTest {

  val oobIndexes = Array(1, 2)
  val oobPredictions: Array[Any] = Array(2, 3)
  val oobErrors = List(0.35, 0.33)

  val classCounts0 = Array(1, 2, 0, 3)
  val classCounts0_0 = Array(0, 0, 0, 1)
  val classCounts0_1 = Array(1, 0, 0, 0)
  val classCounts1 = Array(0, 2, 0, 0)

  val rfModel =
    RandomForestModel(List(RandomForestMember(DecisionTreeModel(SplitNode(ClassificationStats(0.6,
                4, classCounts0), 0L, ThresholdSplitCriteria(1.0), 0.3,
              LeafNode(ClassificationStats(0.0, 1, classCounts0_0)),
              LeafNode(ClassificationStats(0.1, 1, classCounts0_1)))), oobIndexes,
          oobPredictions),
        RandomForestMember(DecisionTreeModel(LeafNode(ClassificationStats(0.5, 2, classCounts1))), null, null)),
      VotingAggregatorFactory(3), oobErrors, null)

  @Test
  def testConvertsSimpleModelCorrectlyWithEmptyMapping() {

    val expectedRepresntation = Forest(None,
      Seq(Tree(Split(ClassificationStats(0.6, 4, classCounts0), null, 0L, false, ThresholdSplitCriteria(1.0), 0.3, Leaf(ClassificationStats(0.0, 1, classCounts0_0)), Leaf(ClassificationStats(0.1, 1, classCounts0_1))), Some(OOBInfo(oobIndexes, oobPredictions))), Tree(Leaf(ClassificationStats(0.5, 2, classCounts1)), None)),
      Some(oobErrors))

    val representation = new ModelConverter(Map.empty).toExternal(rfModel)
    assertEquals(expectedRepresntation, representation)
  }

  @Test
  def testConvertsSimpleModelCorrectlyWithExistingMapping() {
    val expectedRepresntation = Forest(None,
      Seq(Tree(Split(ClassificationStats(0.6, 4, classCounts0), "VAR_0", 0L, false, ThresholdSplitCriteria(1.0), 0.3, Leaf(ClassificationStats(0.0, 1, classCounts0_0)), Leaf(ClassificationStats(0.1, 1, classCounts0_1))), Some(OOBInfo(oobIndexes, oobPredictions))), Tree(Leaf(ClassificationStats(0.5, 2, classCounts1)), None)),
      Some(oobErrors))

    val representation = new ModelConverter(Map(0L -> "VAR_0")).toExternal(rfModel)
    assertEquals(expectedRepresntation, representation)
  }
}
