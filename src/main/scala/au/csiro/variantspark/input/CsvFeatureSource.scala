package au.csiro.variantspark.input

import com.github.tototoshi.csv.{CSVFormat, CSVParser, DefaultCSVFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data._
import au.csiro.variantspark.data.DataBuilder
import org.apache.spark.broadcast.Broadcast

class DefaultCSVFormatSpec extends DefaultCSVFormat with Serializable

case object DefaultCSVFormatSpec extends DefaultCSVFormatSpec

case class CsvFeatureSource(data: RDD[String], defaultType: VariableType = ContinuousVariable,
    optVariableTypes: Option[RDD[String]] = None, csvFormat: CSVFormat = DefaultCSVFormatSpec)
    extends FeatureSource {

  lazy val fileHeader: String = data.first
  lazy val br_header: Broadcast[String] = data.context.broadcast(fileHeader)

  def sampleNames: List[String] = new CSVParser(csvFormat).parseLine(fileHeader).get.tail

  def features: RDD[Feature] = {

    // TODO: Possibly move make a class parameter
    val representationFactory = DefRepresentationFactory
    // TODO: extract the mapping to object
    val local_br_header = this.br_header
    val br_types = data.context.broadcast(optVariableTypes.map(parseTypes))

    data.mapPartitions { it =>
      val header = local_br_header.value
      val csvParser = new CSVParser(csvFormat)
      val types = br_types.value
      it.filter(!_.equals(header))
        .map(csvParser.parseLine(_).get)
        .flatMap {
          case label :: stringValues =>
            // If types are specified, only include variables in the type map
            // Otherwise, use default type for all variables
            types match {
              case Some(typeMap) =>
                typeMap.get(label).map { variableType =>
                  StdFeature(label, variableType,
                    representationFactory.createRepresentation(variableType, stringValues))
                }
              case None =>
                Some(StdFeature(label, defaultType,
                    representationFactory.createRepresentation(defaultType, stringValues)))
            }
        }
    }
  }

  def parseTypes(typeRDD: RDD[String]): Map[String, VariableType] = {
    typeRDD
      .mapPartitions { it =>
        val csvParser = new CSVParser(csvFormat)
        it.map(csvParser.parseLine(_).get).map(l => (l.head, VariableType.fromString(l.last)))
      }
      .collect()
      .toMap
  }

  def featuresAs[V](implicit cr: DataBuilder[V]): RDD[Feature] = {
    // TODO: extract the mapping to object
    val local_br_header = this.br_header
    val br_types = data.context.broadcast(optVariableTypes.map(parseTypes))

    data.mapPartitions { it =>
      val header = local_br_header.value
      val csvParser = new CSVParser(csvFormat)
      val types = br_types.value
      // format: off
      it.filter(!_.equals(header))
        .map(csvParser.parseLine(_).get)
        .flatMap { l =>
          // If types are specified, only include variables in the type map
          // Otherwise, use default type for all variables
          types match {
            case Some(typeMap) =>
              typeMap.get(l.head).map { variableType =>
                StdFeature.from[V](l.head, variableType, l.tail)
              }
            case None =>
              Some(StdFeature.from[V](l.head, defaultType, l.tail))
          }
        }
        // format: on
    }
  }
}
