package au.csiro.variantspark.pedigree

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
    println(tree.tree)
    println("Is connectd: " + tree.tree.isConnected)
    println("Degree sets: " + tree.tree.degreeNodesMap)
    println(tree.tree.nodes.filter(n => n.diPredecessors.isEmpty))
    val topoOrder = tree.tree.topologicalSort.fold(cycleNode => throw new RuntimeException(), order => order)
    println(topoOrder)
    println(topoOrder.toList)
  }
  
}