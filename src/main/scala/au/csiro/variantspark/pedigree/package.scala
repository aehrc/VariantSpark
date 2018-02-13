package au.csiro.variantspark

package object pedigree {
  type ContigID = String
  type IndividualID = String
  type IndexedGenotypeSpec = GenotypeSpec[Int]
  type GenotypePool = scala.collection.Map[IndividualID, IndexedGenotypeSpec]
}