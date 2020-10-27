import java.io._

import scala.collection.immutable.TreeMap
import scala.util.parsing.json.{JSON, JSONObject}
import scalaj.http.Http

import vegas._
import vegas.render.WindowRenderer._
import vegas.spec.Spec._

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.driver.core.Cluster

object Part2 {
  def main(args: Array[String]) {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
          .appName("final-project")
          .getOrCreate()
    val sentimentData = spark.read.cassandraFormat("headline_sentiments", "project").load()
    val sentiments = sentimentData.groupBy("date").avg("sentiment")
    sentimentData.show()

    // Each resource contains data between January 7 of the given year and December 6 of the next year
    // E.g. 2018: 2018-01-07 => 2019-12-06
    // Source: https://data.gov.au/dataset/ds-sa-860126f7-eeb5-4fbc-be44-069aa0467d11/details
    val resourceIds = TreeMap(
      /*"2019" -> "590083cd-be2f-4a6c-871e-0ec4c717717b",
      "2017" -> "bf604730-9ec8-44dd-88a3-f024b387e0e4",
      "2016" -> "9eb63b08-c300-49c5-b6ef-ecb0945008b5",
      "2015" -> "19beeceb-a870-4424-b533-43c774bcb03e",
      "2014" -> "49ff5a8b-6e68-4f53-861a-085055999fcf",
      "2013" -> "e4a20c4d-7462-4ce2-95c9-b288f5634f62",
      "2012" -> "2f55aea8-3d10-44a9-8e12-e8bd247340d1",
      "2011" -> "147393d8-cf1b-4fdb-9697-4b67051e36a7",
      "2010" -> "9d8a2ad3-ffe5-42a4-861a-b14c60a28623",*/
      "2018" -> "809b11dd-9944-4406-a0c4-af6a5b332177"
    )

    for ((year, resourceId) <- resourceIds) {
      println("Resource year: " + year)
      println("Resource ID: " + resourceId)

      val startDate = year + "-01-01";
      val endDate = year + "-12-31";
      val query = "SELECT \"Reported Date\", SUM(\"Offence count\") FROM \"" + resourceId + "\" WHERE \"Reported Date\" BETWEEN '" + startDate + "' AND '" + endDate + "' GROUP BY \"Reported Date\" ORDER BY \"Reported Date\""

      println("SQL: " + query)

      val response = Http("https://data.sa.gov.au/data/api/3/action/datastore_search_sql")
        .param("sql", query)
        .asString

      val data = JSON.parseFull(response.body).get.asInstanceOf[Map[String, Any]]
      val result = data.get("result").get.asInstanceOf[Map[String, Any]]
      val records = result.get("records").get.asInstanceOf[List[Map[String, String]]]

      println("First record: " + records(0))
      println("Last record: " + records(records.size - 1))
      println("Total records: " + records.size)
      println("")

      val recordsWithSentiment = records.map(record => {
        val date = record("Reported Date").slice(0, 10)
        val sentiment = sentiments.filter(sentiments("date") === date.replace("-", ""))
        if(sentiment.count > 0) {
          Map(
            "date" -> date,
            "incidents" -> record("sum"),
            "sentiment" -> sentiment.head.getDouble(1) * 100
          )
        }
        else {
          null
        }
      }).filter(_ != null)

      println(recordsWithSentiment)


      val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
      val session = cluster.connect()
      session.execute("CREATE KEYSPACE IF NOT EXISTS project WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
      session.execute("CREATE TABLE IF NOT EXISTS project.result (date text PRIMARY KEY, incidents int, sentiment double);")

      val rdd = spark.sparkContext.parallelize(recordsWithSentiment.map(record => (record("date"), record("incidents"), record("sentiment"))))
      rdd.saveToCassandra("project","result",SomeColumns("date","incidents","sentiment"))
      session.close()


      val plot = Vegas
        .layered(year)
        .withData(recordsWithSentiment)
        .withLayers(
          Layer()
            .mark(Line)
            .encodeX("Date", Temp)
            .encodeY("Number of Crimes", Quant),
          Layer()
            .mark(Line)
            .encodeX("Date", Temp)
            .encodeY("Headline Sentiment", Quant)
            .configMark(color="red")
        )

      val file = new File("output/" + year + ".html")
      val bw = new BufferedWriter(new FileWriter(file))
      bw.write(plot.html.pageHTML())
      bw.close()
    }
  }
}
