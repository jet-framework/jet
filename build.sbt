name := "Distributed"

version := "0.1"

organization := "EPFL"

scalaVersion := virtScala

scalacOptions += "-Yvirtualize" 

//libraryDependencies += LMS_Key

libraryDependencies += "org.scala-lang" % "scala-library" % virtScala

libraryDependencies += "org.scala-lang" % "scala-compiler" % virtScala
