
import edu.stanford.nlp.simple._
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap
 

import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.convert.wrapAll._
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.immutable.List
import scala.collection.mutable.ListBuffer  

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/*
//imports for writing to cassandra
import org.apache.spark.streaming.kafka._
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


//only needed when using directstream from KafkaUtils
class NullDecoder(props: VerifiableProperties = null) extends Decoder[Null] {
  def fromBytes(bytes: Array[Byte]): Null = null
}
*/

object Main extends App {

	//setup for sentiment-analysis
	val props = new Properties()
  	props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  	val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

	val headlines = new ListBuffer[Array[String]]()

	val sentiments = new ListBuffer[Array[String]]()

	// read csv into headlines
	val bufferedSource = Source.fromFile("abcnews-date-text.csv")
	for (line <- bufferedSource.getLines.take(11).drop(1)) {
   
    	val Array(date, headline) = line.split(",").map(_.trim) //each line gets written into an array

    	//filter out everything before 2010
    	//if(s"$date".toInt>20100000){
    		headlines.append(Array(s"$date", s"$headline"))
    	//}
	}
	bufferedSource.close 

	var test: Annotation = _
	var sentences: java.util.List[CoreMap] = _
	val toRemove = "()".toSet

	//transform second array, add sentiment analysis and write everything into sentiments
	headlines.foreach {((i) => 
		pipeline.process(i(1)).get(classOf[CoreAnnotations.SentencesAnnotation])
	    	.map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
	    	.map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
	    	.foreach(x => sentiments.append(Array(i(0), x.toString.filterNot(toRemove).split(",")(0),x.toString.filterNot(toRemove).split(",")(1))))
	)} 

	//sentiments is now an Array of String-Arrays, each of which contains date(0), headline(1) and sentiment(2)
	//sentiments.foreach {((i) => i.foreach(println))} 
	//println("sentiments(0)(1)")

////////////////////////////////////Using Spark
	val spark:SparkSession = SparkSession.builder().master("local[1]")
          .appName("final-project")
          .getOrCreate()
    val rdd:RDD[Array[String]] = spark.sparkContext.parallelize(sentiments)
    /*
      val rddCollect:Array[Array[String]] = rdd.collect()
      println("Number of Partitions: "+rdd.getNumPartitions)
      println("Action: First element: "+rdd.first())
      println("Action: RDD converted to Array[String] : ")
      rddCollect.foreach(println)
*/
      

//Writing to Kassandra

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS project WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
    session.execute("CREATE TABLE IF NOT EXISTS project.headline_sentiments (count int PRIMARY KEY, date text, headline text, sentiment int);")


    //rdd.saveToCassandra(project,headline_sentiments,SomeColumns("count","date","headline","sentiment))
    
    //or
    val schema =  StructType(Array(StructField("date",StringType,true),StructField("headline",StringType,true),StructField("sentiment",IntegerType,true)))
    val rddDF = sqlContext.applySchema(rdd, schema)
    rddDF.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "words_copy", "keyspace" -> "test")).save()

///////////////////////////////////


/*

/////////////////////////Using Kafkautils

    val pairs = KafkaUtils.createDirectStream[
	    Null,
	    String,
	    NullDecoder,
	    StringDecoder
    ](ssc, kafkaConf, Set("headline_sentiments"))


    def mappingFunc(key: Null, value: Option[String], state: State[Int]): (Int, String, String, Int) = {
    	
		val count = state.getOption.getOrElse(1)

		val field = sentiments(count)

		val date = field(0)

		val headline = field(1)

		val sentiment = field(2)

    	state.update(count+1)

    	return (count, date, headline, sentiment)
    }

    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("project", "headline_sentiments", SomeColumns("count", "date", "headline", "sentiment"))

    ssc.start()
    ssc.awaitTermination()

    session.close()
  }
///////////////////////////////////

*/

}
