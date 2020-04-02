import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class wineryProduction(
                 country: Option[String],
                 id: Option[Int],
                 points: Option[Int],
                 price: Option[Int],
                 title: Option[String],
                 variety: Option[String],
                 winery: Option[String])

object jsonReader {
  val conf = new SparkConf().setAppName("JsonReader")
  conf.setMaster("local");
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val text = sc.textFile(args(0))

    text.foreach(x => {implicit val formats = DefaultFormats; println(parse(x).extract[wineryProduction].price)})

  }
}
