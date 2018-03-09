package au.csiro.variantspark.genomics

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
    val Seq(_, individualID, paternalID, maternalID, gender) =  splitLine.take(5)
    FamilyTrio(id = individualID ,
        gender = Gender.fromPedCode(gender.toInt), 
        paternalId = IndividualID.fromPedId(paternalID),
        maternalId = IndividualID.fromPedId(maternalID))     
  }
  
  /**
   * Loads a ped file in the LINKAGE format (see; https://www.broadinstitute.org/haploview/input-file-formats) 
   * except that that it expects a header line: 'Family ID', 'Individual ID' 'Parental ID'  'Maternal ID', 'Gender',...
   * Fields after `Gender` are ignored
   */
  def loadPed(pathToPedFile: String):List[FamilyTrio] = {
    implicit object PedTSVFormat extends TSVFormat {
    }
    LoanUtils.withCloseable(CSVReader.open(new FileReader(pathToPedFile))) { reader => 
      //TODO: add auto detection if header is presnet
      val header = reader.readNext().get
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



