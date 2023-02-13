import scala.io.{BufferedSource, Source}

object Main extends App {

  def source: BufferedSource = Source.fromResource("bestsellers_with_categories.csv")

  def parseHeaders(head: String): Map[String, Int] = {
    head.split(",").zipWithIndex.toMap
  }

  def prepareCSV(row: String, headers: Map[String, Int]): Book = {
    val data = row.split(""",(?![^"]*"(?:(?:[^"]*"){2})*[^"]*$)""");

    Book(
      data(headers("Name")),
      data(headers("Author")),
      data(headers("User Rating")),
      data(headers("Reviews")),
      data(headers("Price")),
      data(headers("Year")),
      data(headers("Genre"))
    )
  }

  val head :: rows = source.getLines.toList
  val headers: Map[String, Int] = parseHeaders(head)
  val booksInfo: Seq[Book] = rows.map(row => prepareCSV(row, headers))

  val topic = "books"

  Producer.send(booksInfo, topic)
  Consumer.get(topic)

}
