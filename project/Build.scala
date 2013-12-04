import sbt._
import Keys._
import scala._


object BuildSettings {

  import Dependencies._

  lazy val genIdeaFastSetting = addCommandAlias("gen-idea-fast", "gen-idea no-classifiers")

  lazy val sharedSettings = Defaults.defaultSettings ++ Seq(
    name := "HadoopMiniCluster",
    version := "0.1",
    organization := "com.dbtsai",
    scalaVersion := "2.9.2",
    exportJars := false,
    crossPaths := false,
    javacOptions ++= Seq("-source", "1.6", "-target", "1.6"),
    testOptions += Tests.Argument(TestFrameworks.JUnit, "-q", "-v"),
    testListeners <<= (target, streams).map((t, s) => Seq(new eu.henkelmann.sbt.JUnitXmlTestsListener(t.getAbsolutePath))),
    resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    libraryDependencies ++= sharedLibraryDependencies
  ) ++ genIdeaFastSetting
}

object ExampleBuild extends Build {

  import BuildSettings._
  import Dependencies._

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = sharedSettings ++ Seq(
      name := "root",
      libraryDependencies ++= hadoopDependencies
    )
  )
}

object Dependencies {
  val HADOOP_VERSION = "2.0.0-mr1-cdh4.2.0"

  lazy val sharedLibraryDependencies = Seq(
    "com.novocode" % "junit-interface" % "0.10-M4" % "test",
    "org.mockito" % "mockito-all" % "1.9.5" % "test",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test" intransitive(),
    "commons-lang" % "commons-lang" % "2.6",
    "com.google.guava" % "guava" % "15.0",
    "com.google.code.gson" % "gson" % "2.2.4",
    "log4j" % "log4j" % "1.2.17"
  )

  def matchHadoopDependencies(x: String): Seq[ModuleID] = x match {
    case "2.0.0-mr1-cdh4.2.0" =>
      Seq(
        "com.google.protobuf" % "protobuf-java" % "2.4.0a" ,//intransitive(),
        "commons-io" % "commons-io" % "2.4",
        "commons-daemon" % "commons-daemon" % "1.0.15",// intransitive(),
        "commons-logging" % "commons-logging" % "1.1.3",//intransitive(),
        "org.antlr" % "antlr" % "3.4" ,//intransitive(),
        "org.apache.avro" % "avro" % "1.7.4",// intransitive(),
        "org.apache.avro" % "avro-mapred" % "1.7.4" classifier "hadoop2",// intransitive(),
        "org.apache.hadoop" % "hadoop-auth" % "2.0.0-cdh4.3.0",// intransitive(),
        "org.apache.hadoop" % "hadoop-client" % "2.0.0-mr1-cdh4.3.0",// intransitive(),
        "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.3.0" classifier "" classifier "tests",// intransitive(),
        // "org.apache.hadoop" % "hadoop-common" % "2.0.0-cdh4.3.0" % "test" classifier "tests" intransitive(),
        "org.apache.hadoop" % "hadoop-core" % "2.0.0-mr1-cdh4.3.0" ,//intransitive(),
        "org.apache.hadoop" % "hadoop-hdfs" % "2.0.0-cdh4.3.0" classifier "" classifier "tests" ,//intransitive(),
        // "org.apache.hadoop" % "hadoop-hdfs" % "2.0.0-cdh4.3.0" % "test" classifier "tests" intransitive(),
        "org.apache.hadoop" % "hadoop-minicluster" % "2.0.0-mr1-cdh4.3.0" % "test" ,//intransitive(),
        "org.apache.hadoop" % "hadoop-streaming" % "2.0.0-mr1-cdh4.3.0" ,//intransitive(),
        "org.apache.hadoop" % "hadoop-test" % "2.0.0-mr1-cdh4.3.0" % "test",// intransitive(),
        "org.apache.hadoop" % "hadoop-tools" % "2.0.0-mr1-cdh4.3.0" ,//intransitive(),
        "org.apache.mrunit" % "mrunit" % "1.0.0" % "test" classifier "hadoop2",
        "org.apache.pig" % "pig" % "0.11.0-cdh4.3.0" % "compile" //intransitive()
      )
    case x => sys.error("Unsupported Hadoop version " + x)
  }

  lazy val hadoopDependencies = matchHadoopDependencies(HADOOP_VERSION)
}
