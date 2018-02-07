package au.csiro.variantspark.pedigree

import scalax.collection.Graph
import scalax.collection.edge.LDiEdge     // labeled directed edge
import scalax.collection.edge.Implicits._ // shortcuts
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.FileReader
import com.github.tototoshi.csv.CSVFormat

object ParentalRole extends Enumeration {
  type ParentalRole = Value
  val Father, Mother = Value
}

import ParentalRole._

object IndividualID {
  def fromPedId(pedId: String):Option[IndividualID] = if (pedId == "0") None else Some(pedId)
}

object Gender extends Enumeration {
  type Gender = Value
  val Male, Female  = Value
  def fromPedCode(pedCode:Int) =  pedCode match {
    case 1 => Male
    case 2 =>  Female
    case _ =>  throw new IllegalArgumentException(s"Invalid ped gender code ${pedCode}")
  }
}

import Gender._
import com.github.tototoshi.csv.DefaultCSVFormat
import com.github.tototoshi.csv.TSVFormat

trait Individual {
  def id:IndividualID
  def gender: Gender.Gender
}

case class FamilyTrio(val id:IndividualID, val gender: Gender, 
    paternalId:Option[IndividualID] = None, maternalId: Option[IndividualID] = None) extends Individual {
  
  def isFounder = paternalId.isEmpty && maternalId.isEmpty
  def isFullOffspring = paternalId.isDefined && maternalId.isDefined
  
  def parents:List[(IndividualID, ParentalRole)] = List(
      paternalId.map(id=> (id,Father)), 
      maternalId.map(id=> (id,Mother))
    ).flatMap(_.toList)
}

    
object FamilyTrio {
  
  def fromPedLine(splitLine: Seq[String]):FamilyTrio = {
    val pl = splitLine.toIndexedSeq 
    FamilyTrio(id = pl(0) ,
        gender = Gender.fromPedCode(pl(3).toInt), 
        paternalId = IndividualID.fromPedId(pl(1)),
        maternalId = IndividualID.fromPedId(pl(2)))     
  }
  
  /**
   * for now just assume:
   * - header 'Individual ID' 'Parental ID'  'Mathernal ID', 'Gender'
   * - tab separated
   */
  def loadPed(pathToPedFile: String):List[FamilyTrio] = {
    implicit object PedTSVFormat extends TSVFormat {
    }
    
    LoanUtils.withCloseable(CSVReader.open(new FileReader(pathToPedFile))) { reader => 
      val header = reader.readNext().get
      println(s"PED header: ${header}")
      reader.iterator.map(fromPedLine).toList
    }
  }  
}

/**
 * Looks like this best coiuld be represented by a graph.
 * Graph library from: http://www.scala-graph.org/guides/core-introduction.html
 */
class PedigreeTree(val trios: Seq[FamilyTrio]) {
  
  /**
   * Get all the founders that is individuals who are not children in this tree
   */ 
  lazy val triosById:Map[IndividualID, FamilyTrio] = trios.map(t => (t.id, t)).toMap
  lazy val graph: Graph[IndividualID, LDiEdge] = {
        val edges = trios.flatMap(t => t.parents.map(p => LDiEdge(p._1, t.id)(p._2)))
        Graph(edges.toArray:_*)
  }
  lazy val orderedTrioIds = graph.topologicalSort.fold(cycleNode => throw new RuntimeException(),
          order => order).map(_.value).toList
          
  lazy val orderedTrios = orderedTrioIds.map(triosById(_))
  
  def founders:Seq[IndividualID] = ???

  
  //lazy val topoOrder = 
  
  /**
   * The othe thing I need is essentialy to be able to
   * - create the OffsringSpecs for all all the offspring in this set  
   * - validate that all offspring can be created (all assuming that all the founders are present)
   * - sort the Individuals in topological order so that I can either creatre the full IBD tree or 
   * - run offspring generation on the entire set
   */ 
}

object PedigreeTree {
  
  import ParentalRole._
   
  def apply(trios: Seq[FamilyTrio]): PedigreeTree =  {
    new PedigreeTree(trios) 
  }
  
  def loadPed(pathToPedFile: String): PedigreeTree = {
    PedigreeTree(FamilyTrio.loadPed(pathToPedFile))
  }
  
}



