package au.csiro.variantspark.cli.args

import org.kohsuke.args4j.Option
import au.csiro.variantspark.algo.To100ImportanceNormalizer
import au.csiro.variantspark.algo.RawVarImportanceNormalizer

trait ImportanceOutputArgs {
 
    // output options
//  @Option(name="-of", required=false, usage="Path to output file (def = stdout)", aliases=Array("--output-file") )
//  val outputFile:String = null
//
  
  @Option(name="-oc", required=false, usage="The number of output paritions. Use `0` for spark default (def=0)",
      aliases=Array("--output-partitions"))
  val nOuputParitions:Int = 0

  
  @Option(name="-ovn", required=false, usage="Type of normalization to apply to variable importance [raw|to100] (def=to100)",
      aliases=Array("--output-normalization"))
  val outputNormalization:String = "to100"
  
  
  def importanceNormalizer = outputNormalization match {
    case "to100" => To100ImportanceNormalizer
    case "raw"  => RawVarImportanceNormalizer
    case _ => throw new IllegalArgumentException(s"Unrecognized normalization type: `${outputNormalization}`. Valid options are `to100`, `raw`")
  }
  
  def limitVariables(importances:Seq[(Long,Double)], limit:Int):Seq[(Long,Double)] =  {
    if (limit > 0) importances.sortBy(-_._2).take(limit) else importances
  }
}