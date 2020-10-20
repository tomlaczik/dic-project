
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


object Main extends App {
	
	println("Date, Headline") //20030219


	//setup for sentinent-analysis
	val props = new Properties()
  	props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  	val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

	//val headlines: HashMap[Int, String] = new HashMap()

	val headlines = new ListBuffer[Array[String]]()

	val sentinents = new ListBuffer[Array[String]]()

	//failed tests to create a Map with 2 values
	//val test = new HashMap[Int, mutable.Set[List[String]]] with MultiMap[Int, List[String]]
	//test.addBinding(10, List("a", "4"))
	//val sentMap = new Map[String, List[String]]
	//val mm = new mutable.HashMap[Int, mutable.Set[String]] with mutable.MultiMap[Int, String]
	//mm.addBinding(20201020, Set("abc", "3"))

	
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

	//headlines.foreach {((i) => i.foreach(println))} 
	headlines.foreach {((i) => 
		//test = pipeline.process(i(1))
		//println(test, i(0), i(1))
		pipeline.process(i(1)).get(classOf[CoreAnnotations.SentencesAnnotation])
	    	.map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
	    	.map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
	    	.foreach(x => sentinents.append(Array(i(0), x.toString)))
	)} 

	sentinents.foreach {((i) => i.foreach(println))} 

/*
		val annotation: Annotation = pipeline.process(i(1))
		val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])

		sentences
	    .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
	    .map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
	    .foreach(x => sentinents.append(Array(i(0), i(1), x)))
*/

/*

	// predict sentiment for each headline and save to sentinents

	for ((date, headline) <- headlines) {
  		
  		val annotation: Annotation = pipeline.process(headline)
		val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])

		sentences
	    .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
	    .map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
	    .foreach(x => sentinents.put(date.toInt, x))

	}

	for ((date, sentinent) <- sentinents) {
		println(date, sentinent)
	}




	//ToDo: print collection


	
	
	val sent = new Sentence("Lucy is in the sky with diamonds.")
	val nerTags = sent.nerTags()

	val props = new Properties()
  	props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
  	val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)

	val str = "Lucy is in the sky with diamonds."
	val annotation: Annotation = pipeline.process(str)
	val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])


	sentences
	    .map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
	    .map { case (sentence, tree) => (sentence.toString,RNNCoreAnnotations.getPredictedClass(tree)) }
	    .foreach(x => println(x))

	
*/
}
