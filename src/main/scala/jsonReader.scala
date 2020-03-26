import org.apache.spark.{SparkConf, SparkContext}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class wineryProduction(
                 country: String,
                 id: Int,
                 points: Int,
                 title: String,
                 variety: String,
                 winery: String)

object jsonReader {
  val conf = new SparkConf().setAppName("JsonReader")
  conf.setMaster("local");
  val sc = new SparkContext(conf)

  def main(args: Array[String]): Unit = {
    val text = sc.textFile(args(0))

    text.foreach(x => {implicit val formats = DefaultFormats; println(parse(x).extractOpt[wineryProduction])})

  }
}
