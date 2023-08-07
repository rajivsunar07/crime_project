scalaVersion := "2.12.15"

libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.15"
val sparkVersion = "3.3.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "io.delta" %% "delta-core" % "2.1.0"
)