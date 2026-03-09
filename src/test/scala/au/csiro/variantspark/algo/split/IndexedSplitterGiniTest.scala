package au.csiro.variantspark.algo.split

import au.csiro.pbdava.ssparkle.common.utils.Logging
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.algo.IndexedSplitter
import au.csiro.variantspark.algo.SplitInfo
import au.csiro.variantspark.algo.IndexedSplitAggregator
import au.csiro.variantspark.algo.GiniImpurity
import au.csiro.variantspark.algo.ClassificationSplitAggregator
import au.csiro.variantspark.algo.ConfusionAggregator

abstract class IndexedSplitterGiniTest {

  def splitterFromAgg(agg: IndexedSplitAggregator, confAgg: ConfusionAggregator,
      data: Array[Double]): IndexedSplitter
  def splitter(data: Array[Double], labels: Array[Int], nLabels: Int = 2): IndexedSplitter = {

    val confusionAgg = new ConfusionAggregator(GiniImpurity, 10, nLabels, labels)
    splitterFromAgg(ClassificationSplitAggregator(GiniImpurity, labels, nLabels), confusionAgg,
      data)
  }

  @Test
  def testEmptySplit() {
    val splitInfo = splitter(Array(0.0), Array(1)).findSplit(Array[Int]())
    assertNull(splitInfo)
  }

  @Test
  def testOneElementSplit() {
    val splitInfo = splitter(Array(0.0), Array(1)).findSplit(Array(0))
    assertNull(splitInfo)
  }
  @Test
  def testConstantsLabelSplit() {
    val splitInfo =
      splitter(Array(0.0, 1.0, 2.0, 3.0), Array(1, 1, 1, 1)).findSplit(Range(0, 4).toArray)
    assertEquals(SplitInfo(0, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  def testConstantsValuesSplit() {
    val splitInfo =
      splitter(Array(1.0, 1.0, 1.0, 1.0), Array(0, 1, 0, 1)).findSplit(Range(0, 4).toArray)
    assertNull(splitInfo)
  }
  @Test
  def testActualSplit() {
    val splitInfo =
      splitter(Array(0.0, 2.0, 1.0, 2.0), Array(0, 1, 0, 1)).findSplit(Range(0, 4).toArray)
    assertEquals(SplitInfo(1, 0.0, 0.0, 0.0), splitInfo)
  }
  @Test
  def testActualSplitWithSubset() {
    val splitInfo = splitter(Array(0.0, 2.0, 1.0, 2.0, 2.0, 2.0), Array(0, 1, 0, 1, 0, 0))
      .findSplit(Range(0, 4).toArray)
    assertEquals(SplitInfo(1, 0.0, 0.0, 0.0), splitInfo)
  }
  @Test
  def testGiniWithComplexSplit() {
    val splitInfo = splitter(Array(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 3.0), Array(0, 1, 0, 0, 1, 1, 0))
      .findSplit(Range(0, 7).toArray)
    val rightGini = 1.0 - (0.25 * 0.25 + 0.75 * 0.75)
    val leftGini = 1 - 5.0 / 9.0 // 1 - (1/3^ + 2/3^2)
    assertEquals(
      SplitInfo(1, (4.0 * rightGini + 3.0 * leftGini) / 7.0, rightGini, leftGini),
      splitInfo)
  }
}

class JNaiveContinousIndexedSplitterTest extends IndexedSplitterGiniTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, confAgg: ConfusionAggregator,
      data: Array[Double]): IndexedSplitter = new JNaiveContinousIndexedSplitter(agg, data)
}

class JOrderedIndexedSplitterTest extends IndexedSplitterGiniTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, confAgg: ConfusionAggregator,
      data: Array[Double]): IndexedSplitter =
    new JOrderedIndexedSplitter(agg, data.map(_.toByte), 4)
}

class JOrderedFastIndexedSplitterTest extends IndexedSplitterGiniTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, confAgg: ConfusionAggregator,
      data: Array[Double]): IndexedSplitter =
    new JOrderedFastIndexedSplitter(confAgg, agg, data.map(_.toByte), 4)
}

abstract class NominalSplitterGiniTest extends IndexedSplitterGiniTest {
  @Test
  override def testActualSplit() {
    val splitInfo =
      splitter(Array(0.0, 2.0, 1.0, 2.0), Array(0, 1, 0, 1)).findSplit(Range(0, 4).toArray)
    // Nominal splitter should return mask 3 (binary 011 -> levels 0 and 1 go left)
    assertEquals(SplitInfo(3.0, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  override def testActualSplitWithSubset() {
    val splitInfo = splitter(Array(0.0, 2.0, 1.0, 2.0, 2.0, 2.0), Array(0, 1, 0, 1, 0, 0))
      .findSplit(Range(0, 4).toArray)
    assertEquals(SplitInfo(3.0, 0.0, 0.0, 0.0), splitInfo)
  }

  @Test
  override def testConstantsLabelSplit() {
    val splitInfo =
      splitter(Array(0.0, 1.0, 2.0, 3.0), Array(1, 1, 1, 1)).findSplit(Range(0, 4).toArray)
    // Nominal splitter finds any valid split. Since impurity is 0 everywhere,
    // it picks the first valid mask (1.0 -> Level 0 goes Left).
    assertEquals(SplitInfo(1.0, 0.0, 0.0, 0.0), splitInfo)
  }

  // The complex split test expects an ordered split point (1.0).
  // Nominal splitters can group arbitrary levels, often finding better splits.
  // Data: Levels(0->Mixed, 1->Pure0, 2->Pure1, 3->Mixed).
  // Optimal Nominal Split: Group Level 1 (Pure 0) vs Rest.
  // Mask 2 (Binary 0010) separates Level 1.
  @Test
  override def testGiniWithComplexSplit() {
    val splitInfo = splitter(Array(0.0, 0.0, 1.0, 1.0, 2.0, 3.0, 3.0), Array(0, 1, 0, 0, 1, 1, 0))
      .findSplit(Range(0, 7).toArray)

    // Expected: Mask 2.0 (Level 1 Left, Rest Right)
    // Left (Level 1): [0,0] -> Gini 0.0
    // Right (Levels 0,2,3): [0,1, 1, 1,0] -> 2 zeros, 3 ones. Gini = 1 - (4/25 + 9/25) = 12/25 = 0.48
    // Weighted Gini: (2/7)*0 + (5/7)*0.48 = 2.4/7 = 0.342857...
    assertEquals(SplitInfo(2.0, 0.34285714285714286, 0.0, 0.48), splitInfo)
  }
}

class JNominalIndexedSplitterTest extends NominalSplitterGiniTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, confAgg: ConfusionAggregator,
      data: Array[Double]): IndexedSplitter =
    new JNominalIndexedSplitter(agg, data.map(_.toByte), 4)
}

class JNominalFastIndexedSplitterTest extends NominalSplitterGiniTest {
  override def splitterFromAgg(agg: IndexedSplitAggregator, confAgg: ConfusionAggregator,
      data: Array[Double]): IndexedSplitter =
    new JNominalFastIndexedSplitter(confAgg, agg, data.map(_.toByte), 4)
}
