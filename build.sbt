import scala.sys.process._

name := "DISC"
version := "0.1"
scalaVersion := "2.11.12"

/*Dependency*/
resolvers += "Spark Packages Repo" at "http://repos.spark-packages.org"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.3" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.3" //% "provided"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.3" //% "provided"
libraryDependencies += "com.joptimizer" % "joptimizer" % "5.0.0"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.3" % "test"
libraryDependencies += "net.sf.trove4j" % "trove4j" % "3.0.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.4"
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.3.0"
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.0-RC2"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.2"
libraryDependencies += "it.unimi.dsi" % "fastutil" % "8.1.0"

//libraryDependencies += "org.antlr" % "antlr4-runtime" % "4.7.2"
//libraryDependencies += "it.unimi.dsi" % "webgraph" % "3.6.1"
//dependencyOverrides += "com.google.guava" % "guava" % "15.0"

//lazy val root = (project in file("."))
//  .configs(IntegrationTest)
//  .settings(Defaults.itSettings)

watchSources += baseDirectory.value / "script/"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x                             => MergeStrategy.first
}

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp.filter { f =>
    f.data.getName == "systemml-1.1.0.jar" ||
    f.data.getName == "SCPSolver.jar" ||
    f.data.getName == "LPSOLVESolverPack.jar" ||
    f.data.getName == "GLPKSolverPack.jar"
  }
}

test in assembly := {}

/*Custom tasks*/
lazy val upload = taskKey[Unit]("Upload the files")
upload := {
  "./script/upload.sh" !
}

lazy val assembleThenUpload = taskKey[Unit]("Upload the jar after assembly")
assembleThenUpload := {
  assembly.value
  "./script/upload.sh" !
}
