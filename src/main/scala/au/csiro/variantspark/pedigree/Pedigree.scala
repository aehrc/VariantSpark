package au.csiro.variantspark.pedigree

import scalax.collection.Graph
import scalax.collection.edge.LDiEdge     // labeled directed edge
import scalax.collection.edge.Implicits._ // shortcuts



class Individual() {
  def id:String = ???
}

case class Founder(override val id:String) extends Individual()
case class Offspring(override val id:String, val father: Individual, val mother: Individual) extends Individual()

case class FamilyTrio(val child:IndividualID, val mother:IndividualID, val father:IndividualID)


/**
 * Looks like this best coiuld be represented by a graph.
 * Graph library from: http://www.scala-graph.org/guides/core-introduction.html
 */
class PedigreeTree(tree: Graph[IndividualID, LDiEdge]) {
  
  /**
   * Get all the founders that is individuals who are not children in this tree
   */
  def founders:Seq[IndividualID] = ???
  
  /**
   * The othe thing I need is essentialy to be able to
   * - create the OffsringSpecs for all all the offspring in this set  
   * - validate that all offspring can be created (all assuming that all the founders are present)
   * - sort the Individuals in topological order so that I can either creatre the full IBD tree or 
   * - run offspring generation on the entire set
   */
  
}

object PedigreeTree {
  
  def apply(trios: Seq[FamilyTrio]): PedigreeTree =  {
    
    val edges = trios.flatMap(t => List(LDiEdge(t.father, t.child)("father"), LDiEdge(t.father, t.child)("mother")))
    new PedigreeTree(Graph(edges.toArray:_*)) 
  }
}



