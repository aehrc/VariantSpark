package au.csiro.variantspark.output

import au.csiro.variantspark.input.FeatureSource
import java.io.FileWriter
import au.csiro.pbdava.ssparkle.common.utils.CSVUtils
import java.io.File

/**
 * This only works for smallish datasets (used local files)
 */
case class CSVFeatureSink(val fileName:String) extends FeatureSink {
  
  def save(fs:FeatureSource)  {
    CSVUtils.withFile(new File(fileName)) { csvWriter =>
      csvWriter.writeRow("" :: fs.sampleNames)
      csvWriter.writeAll(fs.features().collect().toSeq.map(f => f.label :: f.values.toList))
    }
  }
  
}