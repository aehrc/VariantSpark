package au.csiro.variantspark.genomics.reprod
import org.junit.Assert._
import org.junit.Test
import au.csiro.variantspark.genomics._

class MutationTest {
  
  val testCoord = GenomicCoord("2", 10L)
  
  @Test
  def testMakeMutations() {
    assertEquals(Set(),Mutation.makeAll(testCoord, "T", Set("A","C","G")).toSet)
    assertEquals(Set(Mutation(testCoord,"T","G")),
        Mutation.makeAll(testCoord, "T", Set("A","C")).toSet)
    assertEquals(Set(Mutation(testCoord,"T","G"), Mutation(testCoord,"T","C")),
        Mutation.makeAll(testCoord, "T", Set("A")).toSet)
  }

}