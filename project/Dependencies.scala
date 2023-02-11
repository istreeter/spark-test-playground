import sbt._

object Dependencies {
  lazy val sparkCore = "org.apache.spark" %% "spark-core" % "3.3.1"
  lazy val sparkSql  = "org.apache.spark" %% "spark-sql" % "3.3.1"
  lazy val sparkHive = "org.apache.spark" %% "spark-hive" % "3.3.1"

  lazy val delta = "io.delta" %% "delta-core" % "2.2.0"
}
