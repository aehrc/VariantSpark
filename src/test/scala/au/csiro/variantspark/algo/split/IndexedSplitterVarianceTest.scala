package au.csiro.variantspark.algo.split

import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.algo.IndexedSplitter
import au.csiro.variantspark.algo.{ThresholdSplitInfo, SubsetSplitInfo}
import au.csiro.variantspark.algo.IndexedSplitAggregator
import au.csiro.variantspark.algo.VarianceImpurity
import au.csiro.variantspark.algo.RegressionSplitAggregator
import au.csiro.variantspark.algo.RegressionLevelAggregator

abstract class IndexedSplitterVarianceTest {

  def splitterFromAgg(agg: IndexedSplitAggregator, levelAgg: RegressionLevelAggregator,
      data: Array[Double]): IndexedSplitter

  def splitter(data: Array[Double], values: Array[Double]): IndexedSplitter = {
    val levelAgg = new RegressionLevelAggregator(VarianceImpurity, 10, values)
    splitterFromAgg(RegressionSplitAggregator(VarianceImpurity, values), levelAgg, data)
  }

  @Test
  def testEmptySplit() {
    val splitInfo = splitter(Array(0.0), Array(0.0)).findSplit(Array[Int]())
    assertNull(splitInfo)
  }

  @Test
  def testOneElementSplit() {
    val splitInfo = splitter(Array(0.0), Array(0.0)).findSplit(Array(0))
    assertNull(splitInfo)
  }

  @Test
  def testConstantsLabelSplit() {
    // All target values identical → any split yields variance 0; splitter picks first threshold
    val splitInfo =
      splitter(Array(0.0, 1.0, 2.0, 3.0), Array(1.0, 1.0, 1.0, 1.0))
        .findSplit(Range(0, 4).toArray)
    assertEquals(ThresholdSplitInfo(0.0, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  def testConstantsValuesSplit() {
    // All feature values identical → no valid split threshold
    val splitInfo =
      splitter(Array(1.0, 1.0, 1.0, 1.0), Array(0.0, 1.0, 0.0, 1.0))
        .findSplit(Range(0, 4).toArray)
    assertNull(splitInfo)
  }

  @Test
  def testActualSplit() {
    // feature=[0,2,1,2], target=[0,1,0,1]
    // Sorted by feature: (f=0,t=0),(f=1,t=0),(f=2,t=1),(f=2,t=1)
    // Split at threshold=1.0: left targets=[0,0] var=0, right targets=[1,1] var=0 → perfect
    val splitInfo =
      splitter(Array(0.0, 2.0, 1.0, 2.0), Array(0.0, 1.0, 0.0, 1.0))
        .findSplit(Range(0, 4).toArray)
    assertEquals(ThresholdSplitInfo(1.0, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  def testActualSplitWithSubset() {
    val splitInfo =
      splitter(Array(0.0, 2.0, 1.0, 2.0, 2.0, 2.0), Array(0.0, 1.0, 0.0, 1.0, 0.0, 0.0))
        .findSplit(Range(0, 4).toArray)
    assertEquals(ThresholdSplitInfo(1.0, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  def testVarianceWithComplexSplit() {
    // feature=[0,0,1,1,2,3,3], target=[0,1,0,0,1,1,0]
    // Best split at threshold=1.0:
    //   Left (f≤1.0): targets=[0,1,0,0], mean=0.25, var = 3/16
    //   Right (f>1.0): targets=[1,1,0],  mean=2/3,  var = 2/9
    val splitInfo =
      splitter(Array(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 3.0), Array(0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0))
        .findSplit(Range(0, 7).toArray)
    val leftChildVar = 3.0 / 16.0
    val rightChildVar = 2.0 / 9.0
    assertEquals(
      ThresholdSplitInfo(
        1.0,
        (4.0 * leftChildVar + 3.0 * rightChildVar) / 7.0,
        leftChildVar,
        rightChildVar),
      splitInfo)
  }
}

class JNaiveContinousIndexedSplitterVarianceTest extends IndexedSplitterVarianceTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, levelAgg: RegressionLevelAggregator,
      data: Array[Double]): IndexedSplitter =
    new JNaiveContinousIndexedSplitter(agg, data)
}

class JOrderedIndexedSplitterVarianceTest extends IndexedSplitterVarianceTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, levelAgg: RegressionLevelAggregator,
      data: Array[Double]): IndexedSplitter =
    new JOrderedIndexedSplitter(agg, data.map(_.toByte), 4)
}

class JOrderedFastIndexedSplitterVarianceTest extends IndexedSplitterVarianceTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, levelAgg: RegressionLevelAggregator,
      data: Array[Double]): IndexedSplitter =
    new JOrderedFastIndexedSplitter(levelAgg, agg, data.map(_.toByte), 4)
}

abstract class NominalSplitterVarianceTest extends IndexedSplitterVarianceTest {

  @Test
  override def testActualSplit() {
    // feature=[0,2,1,2] as levels [0,2,1,2], target=[0,1,0,1]
    // Level 0→[0], Level 1→[0], Level 2→[1,1]
    // Best: mask=3 (levels 0 and 1 go left) → left targets=[0,0] var=0, right targets=[1,1] var=0
    val splitInfo =
      splitter(Array(0.0, 2.0, 1.0, 2.0), Array(0.0, 1.0, 0.0, 1.0))
        .findSplit(Range(0, 4).toArray)
    assertEquals(SubsetSplitInfo(3L, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  override def testActualSplitWithSubset() {
    val splitInfo =
      splitter(Array(0.0, 2.0, 1.0, 2.0, 2.0, 2.0), Array(0.0, 1.0, 0.0, 1.0, 0.0, 0.0))
        .findSplit(Range(0, 4).toArray)
    assertEquals(SubsetSplitInfo(3L, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  override def testConstantsLabelSplit() {
    // All targets identical → variance 0 everywhere; splitter picks first valid mask (level 0 left)
    val splitInfo =
      splitter(Array(0.0, 1.0, 2.0, 3.0), Array(1.0, 1.0, 1.0, 1.0))
        .findSplit(Range(0, 4).toArray)
    assertEquals(SubsetSplitInfo(1L, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  override def testVarianceWithComplexSplit() {
    // feature levels=[0,0,1,1,2,3,3], target=[0,1,0,0,1,1,0]
    // Level 0→targets=[0,1] mean=0.5, Level 1→[0,0] mean=0, Level 2→[1] mean=1, Level 3→[1,0] mean=0.5
    // Best: mask=2 (level 1 goes left)
    //   Left (level 1): targets=[0,0], leftImpurity=0
    //   Right (levels 0,2,3): targets=[0,1,1,1,0], mean=0.6, var = 6/25
    val splitInfo =
      splitter(Array(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 3.0), Array(0.0, 1.0, 0.0, 0.0, 1.0, 1.0, 0.0))
        .findSplit(Range(0, 7).toArray)
    val rightChildVar = 6.0 / 25.0
    assertEquals(SubsetSplitInfo(2L, 5.0 * rightChildVar / 7.0, 0.0, rightChildVar), splitInfo)
  }
}

class JNominalRegressionIndexedSplitterTest extends NominalSplitterVarianceTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, levelAgg: RegressionLevelAggregator,
      data: Array[Double]): IndexedSplitter =
    new JNominalRegressionIndexedSplitter(agg, data.map(_.toByte), 4)
}

class JNominalRegressionFastIndexedSplitterTest extends NominalSplitterVarianceTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, levelAgg: RegressionLevelAggregator,
      data: Array[Double]): IndexedSplitter =
    new JNominalRegressionFastIndexedSplitter(levelAgg, agg, data.map(_.toByte), 4)
}
