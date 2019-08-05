package au.csiro.variantspark.external


import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.algo.RandomForestModel
import au.csiro.variantspark.algo.RandomForestMember
import au.csiro.variantspark.algo.DecisionTreeModel
import au.csiro.variantspark.algo.LeafNode
import au.csiro.variantspark.algo._

class ModelConverterTest {
 
  val oobIndexes = Array(1,2)
  val oobPredictions = Array(2,3)
  val oobErrors  = List(0.35, 0.33)
  
  val rfModel = RandomForestModel(
    List(
        RandomForestMember(DecisionTreeModel(
            SplitNode(1, 4, 0.6, 0L, 1.0, 0.3,
              LeafNode(0, 1, 0.0),
              LeafNode(3, 1, 0.1)
            ))
            ,oobIndexes, oobPredictions),
        RandomForestMember(DecisionTreeModel(LeafNode(1, 2, 0.5)), null, null)          
    ),
    3, oobErrors, null)
  
  @Test
  def testConvertsSimpleModelCorrectlyWithEmptyMapping() {
        
    val expectedRepresntation  = Forest(None, Seq(
        Tree(
            Split(1, 4, 0.6, null, 0L, false, 1.0, 0.3,
              Leaf(0, 1, 0.0), 
              Leaf(3, 1, 0.1)
            ),
            Some(OOBInfo(oobIndexes, oobPredictions))),
        Tree(Leaf(1, 2, 0.5), None)
        ), Some(oobErrors))
    
    val representation = new ModelConverter(Map.empty).toExternal(rfModel)
    assertEquals(expectedRepresntation, representation)    
  }

  @Test
  def testConvertsSimpleModelCorrectlyWithExistingMapping() {
    val expectedRepresntation  = Forest(None, Seq(
        Tree(
            Split(1, 4, 0.6, "VAR_0", 0L, false, 1.0, 0.3,
              Leaf(0, 1, 0.0), 
              Leaf(3, 1, 0.1)
            ),
            Some(OOBInfo(oobIndexes, oobPredictions))),
        Tree(Leaf(1, 2, 0.5), None)
        ), Some(oobErrors))
    
    val representation = new ModelConverter(Map(0l -> "VAR_0")).toExternal(rfModel)
    assertEquals(expectedRepresntation, representation)    
  }
}