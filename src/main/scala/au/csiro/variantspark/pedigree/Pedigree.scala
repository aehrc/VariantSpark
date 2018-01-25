package au.csiro.variantspark.pedigree

class Individual() {
  def id:String = ???
}

case class Founder(override val id:String) extends Individual()
case class Offspring(override val id:String, val father: Individual, val mother: Individual) extends Individual()

case class FamilyTriple(val child:String, val father:String, val mother:String)



