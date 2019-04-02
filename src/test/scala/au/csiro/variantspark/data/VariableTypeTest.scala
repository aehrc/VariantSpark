package au.csiro.variantspark.data


import org.junit.Assert._
import org.junit.Test


class VariableTypeTest {

  @Test
  def testParsesTypesCorrectlyFromString() {
    assertEquals(NominalVariable, VariableType.fromString("NOMINAL"))
    assertEquals(DiscreteVariable, VariableType.fromString("DISCRETE"))
    assertEquals(ContinuousVariable, VariableType.fromString("CONTINUOUS"))
    assertEquals(OrdinalVariable, VariableType.fromString("ORDINAL"))
    assertEquals(BoundedOrdinalVariable(3), VariableType.fromString("ORDINAL(3)"))
    assertEquals(BoundedNominalVariable(3), VariableType.fromString("NOMINAL(3)"))
  }
}