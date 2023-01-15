
import spray.json._
import DefaultJsonProtocol._
import java.io._

object Main extends App {

  val str = scala.io.Source.fromURL("https://raw.githubusercontent.com/mledoze/countries/master/countries.json").mkString

  implicit val nameFormat: RootJsonFormat[Name] = jsonFormat1(Name)
  implicit val countryFormat: RootJsonFormat[Country] = jsonFormat4(Country)
  implicit val resultFormat: RootJsonFormat[Result] = jsonFormat3(Result)

  val tmp = str.parseJson.convertTo[List[Country]]

  val res = tmp
    .filter(_.region == "Africa")
    .map(x => Result(x.name.official, x.capital.head, x.area))
    .sortBy(_.area)(Ordering[Int].reverse)
    .take(10)

  val file = new File(args.head)
  val bw = new BufferedWriter(new FileWriter(file))
  bw.write(res.toJson.prettyPrint)
  bw.close()

}
