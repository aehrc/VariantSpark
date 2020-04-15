package au.csiro.variantspark.work

case class Model(hello: String, age: Int, xxx: Option[Int])

object JSONTest {

  def main(args: Array[String]) {

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.JsonDSL._
    implicit val formats: DefaultFormats.type = DefaultFormats

    val rawJson = """{"hello": "world", "age": 42}"""

    val obj = parse(rawJson).asInstanceOf[JObject]
    println(obj)
    println(obj \ "hello")

    val json = ("name" -> "joe") ~ ("age" -> 35)
    println(obj ~ ("name" -> "joe"))

    println(obj.extract[Model])

  }
}
