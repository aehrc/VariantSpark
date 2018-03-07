package au.csiro.variantspark.genomics

import org.junit.Assert._
import org.junit.Test

class PedigreeTest {

  def testLoadFromPedFile()  {
    val trios = FamilyTrio.loadPed("data/relatedness/g1k_ceu_family_15_2.ped")
    trios.foreach { println _ }
  }
  
  @Test
  def testPedTree()  {
    val tree = PedigreeTree.loadPed("data/relatedness/g1k_ceu_family_15_2.ped")
    println(tree.graph)
    println("Is connectd: " + tree.graph.isConnected)
    println("Degree sets: " + tree.graph.degreeNodesMap)
    println(tree.graph.nodes.filter(n => n.diPredecessors.isEmpty))
    val topoOrder = tree.graph.topologicalSort.fold(cycleNode => throw new RuntimeException(), order => order)
    println(topoOrder)
    println(topoOrder.toList)
  }
  
}