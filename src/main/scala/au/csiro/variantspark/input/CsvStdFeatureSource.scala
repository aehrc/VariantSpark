package au.csiro.variantspark.input

import com.github.tototoshi.csv.{CSVFormat, CSVParser, DefaultCSVFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Accumulable
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map => MutableMap}
import au.csiro.variantspark.data.VariableType
import au.csiro.variantspark.data.ContinuousVariable
import au.csiro.variantspark.data.Feature
import au.csiro.variantspark.data.FeatureBuilder
import au.csiro.variantspark.data.DataBuilder
import au.csiro.variantspark.data.StdFeature
import org.apache.spark.broadcast.Broadcast

class MapAccumulator
    extends AccumulatorV2[(Int, Array[String]), Array[String]] {

  val buffer: ArrayBuffer[Array[String]] = ArrayBuffer[Array[String]]()

  def add(v: (Int, Array[String])): Unit = {
    println("Add: " + v._1 + "," + v._2.toList)

    while (buffer.size <= v._1) {
      buffer.append(null)
    }
    buffer(v._1) = v._2
  }

  def isZero: Boolean = {
    return buffer.isEmpty
  }

  def reset(): Unit = {
    buffer.reduceToSize(0)
  }

  def copy(): AccumulatorV2[(Int, Array[String]), Array[String]] = {
    val copy = new MapAccumulator()
    buffer.copyToBuffer(copy.buffer)
    return copy
  }

  def merge(other: AccumulatorV2[(Int, Array[String]), Array[String]]): Unit = {
    other match {
      case otherMap: MapAccumulator =>
        println("Merge: " + buffer + ", " + otherMap.buffer)
        for (i <- 0 until otherMap.buffer.size) {
          if (i < buffer.size) {
            if (otherMap.buffer(i) != null) {
              buffer(i) = otherMap.buffer(i)
            }
          } else {
            buffer.append(otherMap.buffer(i))
          }
        }
        println("Merged: " + buffer + ", " + otherMap.buffer)
    }

  }

  def value: Array[String] = {
    // concateate the current buffer
    // do not allow uninitialzed entries
    val totalLength = buffer.map(_.length).sum
    println("Value: " + buffer + ", " + totalLength)
    val result = ArrayBuffer[String]()
    result.sizeHint(totalLength)
    buffer.foreach(chunk => result ++= chunk)
    result.toArray
  }
}

/**
  * Feature source from a standard variable in columns representation
  * NOTE: This class may be removed in the future but for now is here to simplify
  * ingestion of traditional CSV files for analysis.
  */
case class CsvStdFeatureSource[V](data: RDD[String],
    defaultType: VariableType = ContinuousVariable, csvFormat: CSVFormat = DefaultCSVFormatSpec)
    extends FeatureSource {

  val variableNames: List[String] = new CSVParser(csvFormat).parseLine(data.first).get.tail
  val br_variableNames: Broadcast[List[String]] = data.context.broadcast(variableNames)

  lazy val transposedData: RDD[(String, Array[String])] = {
    // expects data in coma separated format of
    // <SampleId>  , VarName1, VarName2,....
    // SampleName1, v_1_1,   v_1_2, ...
    // SampleName2, v_2_1,   v_2_2, ...

    // The first step is to transpose data in each partition and produce an RDD
    // of (VariableName, (PartitionIndex, [values for samples]])

    val transposedPartitions: RDD[(String, (Int, Array[String]))] =
      data.mapPartitionsWithIndex(
          {
            case (partIndex, it) =>
              val csvParser = new CSVParser(csvFormat)
              val variableNames = br_variableNames.value
              val variableValues = Array.fill(variableNames.size)(ArrayBuffer[String]())
              val sampleNames = ArrayBuffer[String]()
              it.drop(if (partIndex == 0) 1
                  else 0) // skip header line (the first line of the first partition)
                .map(csvParser.parseLine(_).get) // parse the line
                .foreach({
                  case sampleId :: sampleValues =>
                    sampleNames.append(sampleId)
                    sampleValues.zipWithIndex.foreach({
                      case (v, i) => // transpose: assign the values of a sample to
                        // corresponding variables
                        variableValues(i).append(v)
                    })
                })
              variableNames.zip(variableValues.map(c => (partIndex, c.toArray))).toIterator
          }, true)

    // The second step is to combine all the values for each variable originating
    // from differenrt partitions (in the order of partitions)
    transposedPartitions
      .sortBy(_._2._1) // sort by partition Index
      .map({ case (variableName, (partIndex, variableValues)) => (variableName, variableValues) })
      .groupByKey() // group by variableName
      .mapValues(variableValues =>
          variableValues
            .foldLeft(ArrayBuffer[String]())(_ ++= _)
            .toArray) // combine values from this variable coming
      // from different partitions (samples)
      .sortBy(_._1) // sort by variable name
  }

  // TODO: [Peformance] It would be better (especiall for larger files)
  // to collect sample names in the transposition calculation above
  // for smaller files (which is how we intend to use initially this should not
  // be an issue though

  lazy val sampleNames: List[String] = {
    data
      .mapPartitions({ it =>
        val csvParser = new CSVParser(csvFormat)
        it.map(csvParser.parseLine(_).get.head)
      })
      .collect()
      .toList
      .drop(1)
  }

  def features: RDD[Feature] = featuresAs[Vector]

  def featuresAs[V](implicit cr: DataBuilder[V]): RDD[Feature] = {
    transposedData.map({
      case (varId, values) =>
        StdFeature.from[V](varId, defaultType, values.toList)
    })
  }
}
