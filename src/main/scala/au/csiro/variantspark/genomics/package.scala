package au.csiro.variantspark

package object genomics  {
  type ContigID = String
  type IndividualID = String
  type GenotypePool = scala.collection.Map[IndividualID, GenotypeSpec]
  type IndexedVariant = Int
  type BasesVariant = String
  type DNABase = String
}