name := "dic_project"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.6.0" classifier "models",
  "org.apache.spark" %% "spark-core" % "2.2.1",
  "org.apache.spark" %% "spark-sql" % "2.2.1",
  "org.apache.spark" % "spark-streaming_2.11" % "2.2.1",
  "org.scalaj" % "scalaj-http_2.11" % "2.3.0",
  "org.vegas-viz" %% "vegas" % "0.3.11",
)

