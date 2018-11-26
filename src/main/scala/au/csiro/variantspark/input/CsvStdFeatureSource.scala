package au.csiro.variantspark.input

import com.github.tototoshi.csv.{CSVFormat, CSVParser, DefaultCSVFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Accumulable
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.{Map=>MutableMap}


class MapAccumulator extends AccumulatorV2[(Int, Array[String]), Array[String]] { 
  
  val buffer = ArrayBuffer[Array[String]]()
    
  def add(v: (Int, Array[String])): Unit = {
    println("Add: " + v._1 + "," + v._2.toList)
    
    while(buffer.size <= v._1) {
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
        for(i <- 0 until otherMap.buffer.size) {
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
    buffer.foreach(chunk => result++=chunk)
    result.toArray
  }  
}


/**
 * Frature source from a standard variable in columns representation
 */
case class CsvStdFeatureSource[V](data:RDD[String], csvFormat:CSVFormat = DefaultCSVFormatSpec) extends FeatureSource {
  
  //val acc_sampleName = new MapAccumulator()
  //data.context.register(acc_sampleName, "SampleAcc")
  val variableNames = new CSVParser(csvFormat).parseLine(data.first).get.tail
  val br_variableNames = data.context.broadcast(variableNames)
  
  lazy val transposedData = {
    data.mapPartitionsWithIndex({ case (i,it) =>
    val csvParser = new CSVParser(csvFormat)
    val variableNames = br_variableNames.value
    val columns = Array.fill(variableNames.size)(ArrayBuffer[String]())
    val sampleNames = ArrayBuffer[String]()
    it.drop(if (i==0) 1 else 0).map(csvParser.parseLine(_).get).foreach({l => 
      val sampleId = l.head
      // we will accumulate sample names later
      l.tail.zipWithIndex.foreach({case (v, i) => 
          columns(i).append(v)
      })
      sampleNames.append(sampleId)
    })
    //acc_sampleName.add((i,sampleNames.toArray))
    variableNames.zip(columns.map(c => (i,c.toArray))).toIterator      
     // essentially need to transpose the data .. should I Just use Spar Function
     // otherwise produce (varID, (partId, data)) 
     // groupBy (varID)
     // reduce data by combining all the paritions ordered by partID        
          }, true)
      .sortBy(_._2._1) // sort by partition Index
      .groupByKey()
      .map({ case (varId, it) => 
        val values = ArrayBuffer[String]()
        it.foreach({case (pi, block) => values++=block })
        (varId, values.toArray)
      })
      .sortBy(_._1)
  }
  
  
  lazy val sampleNames:List[String] = {
    data.mapPartitions({it => 
      val csvParser = new CSVParser(csvFormat)
      it.map(csvParser.parseLine(_).get.head)
    }).collect().toList.drop(1)
  }

  def featuresAs[V](implicit cr:CanRepresent[V]):RDD[Feature[V]] = {
    transposedData.map({case (varId,values) =>
      cr.from(varId, values.toList)
    })
  }
}
