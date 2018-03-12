package au.csiro.variantspark.genomics.family


import scalax.collection.Graph
import scalax.collection.edge.LDiEdge
import scalax.collection.edge.Implicits._
import au.csiro.pbdava.ssparkle.common.utils.LoanUtils
import com.github.tototoshi.csv.CSVReader
import java.io.FileReader
import au.csiro.variantspark.genomics._
import com.github.tototoshi.csv.TSVFormat

object ParentalRole extends Enumeration {
  type ParentalRole = Value
  val Father, Mother = Value
}

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
import ParentalRole._
import java.io.Reader

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
  def loadPed(reader: Reader):List[FamilyTrio] = {
    implicit object PedTSVFormat extends TSVFormat 
    val csvReader = CSVReader.open(reader)
    //TODO: add auto detection if header is present
    val header = csvReader.readNext().get
    csvReader.iterator.filter(_.size > 1).map(fromPedLine).toList
  }  
}

/**
 * Looks like this best coiuld be represented by a graph.
 * Graph library from: http://www.scala-graph.org/guides/core-introduction.html
 */
case class PedigreeTree(val trios: Seq[FamilyTrio]) {
  
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
     
  def loadPed(pathToPedFile: String): PedigreeTree = {
    LoanUtils.withCloseable(new FileReader(pathToPedFile)) { reader =>
      PedigreeTree(FamilyTrio.loadPed(reader))
    }
  }
}



