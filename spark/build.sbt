import AssemblyKeys._ // put this at the top of the file

name := "spark-gen"

scalacOptions += "-optimise"

scalaVersion := "2.9.1"

libraryDependencies += "org.spark-project" %% "spark-core" % "0.5.0" % "provided"

libraryDependencies += "dk.brics.automaton" % "automaton" % "1.11-8"

assemblySettings

mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case x if x.startsWith("javax/servlet") => MergeStrategy.first
    case x if x.startsWith("org/xmlpull") => MergeStrategy.first
    case x if x.startsWith("org/apache/jasper") => MergeStrategy.first
    case x if x.startsWith("org/objectweb/asm") => MergeStrategy.first
     case x if x.endsWith(".html") => MergeStrategy.concat
    case x => old(x)
  }
}

assembleArtifact in packageScala := false
