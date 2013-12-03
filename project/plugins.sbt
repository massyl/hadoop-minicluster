addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.4.0")

resolvers += Classpaths.typesafeResolver

resolvers += "Scala Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/"

addSbtPlugin("eu.henkelmann" % "junit_xml_listener" % "0.4-SNAPSHOT")