scalaVersion := virtScala

scalacOptions += "-Yvirtualize" 

scalacOptions += "-deprecation" 

scalacOptions += "-unchecked" 

libraryDependencies += "org.scalatest" % "scalatest_2.10.0-virtualized-SNAPSHOT" % "1.6.1-SNAPSHOT" % "test"

resolvers += "Dropbox" at "http://dl.dropbox.com/u/12870350/scala-virtualized"

seq(defaultScalariformSettings: _*)
