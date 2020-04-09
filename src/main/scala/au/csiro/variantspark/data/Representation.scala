package au.csiro.variantspark.data

trait RepresentationFactory {
  def builderFor(variableType: VariableType): DataBuilder[_]
  def createRepresentation(variableType: VariableType, stringData: List[String]): Data =
    builderFor(variableType).from(stringData)
}

case object DefRepresentationFactory extends RepresentationFactory {
  def builderFor(variableType: VariableType): DataBuilder[_] = variableType match {
    case bounded: Bounded if bounded.nLevels < 128 => ByteArrayDataBuilder
    case _: CategoricalType => IntArrayDataBuilder
    case _: NumericalType => VectorDataBuilder
    case _ => throw new IllegalArgumentException(s"Do not know how to represent ${variableType}")
  }
}
