import AssemblyKeys._ // put this at the top of the file

name := "crunch-gen"

version := "0.1.0"

scalaVersion := "2.9.2"

resolvers ++= Seq(
  "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
  "Cloudera Hadoop Releases" at "https://repository.cloudera.com/content/repositories/releases/",
  "Cloudera Third-Party Releases" at "https://repository.cloudera.com/content/repositories/third-party/"
)

libraryDependencies += "dk.brics.automaton" % "automaton" % "1.11-8"

libraryDependencies ++= Seq(
  "com.cloudera.crunch" % "crunch" % "0.2.4" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  ),
  "org.apache.hadoop" % "hadoop-core" % "0.20.2-cdh3u4" % "provided" excludeAll(
    ExclusionRule(organization = "com.sun.jdmk"),
    ExclusionRule(organization = "com.sun.jmx"),
    ExclusionRule(organization = "javax.jms")
  )
)

libraryDependencies += "de.javakaffee" % "kryo-serializers" % "0.9"

parallelExecution in Test := false

assemblySettings

