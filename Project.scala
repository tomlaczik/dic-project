package project

import scala.util.parsing.json.{JSON, JSONObject}
import scalaj.http.{Http}

object Project {
  def main(args: Array[String]) {
    val dates = List(
      "2017",
      "2018",
      "2019",
      "2020"
    )

    for (date <- dates) {
      println(JSONObject(Map("Reported Date" -> date)).toString())
      val response = Http("https://data.sa.gov.au/data/api/3/action/datastore_search")
        .param("resource_id", "590083cd-be2f-4a6c-871e-0ec4c717717b")
        .param("limit", "1")
        .param("q", JSONObject(Map("Reported Date" -> date)).toString())
        .asString

      val data = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
      val result = data.get("result").get.asInstanceOf[Map[String, Any]]
      val records = result.get("records").get.asInstanceOf[List[Map[String, String]]]

      println(records)

      println(result.get("total").getOrElse(0))
    }
  }
}
