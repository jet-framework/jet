import sbt._
import Keys._
import com.typesafe.sbtscalariform.ScalariformPlugin
import scalariform.formatter.preferences._

object HelloBuild extends Build {

   ScalariformPlugin.ScalariformKeys.preferences := FormattingPreferences().setPreference(SpaceBeforeColon, true)

   val lmsproj = RootProject(uri("./virtualization-lms-core/"))

   val virtScala = "2.10.0-M1-virtualized"

   lazy val default = Project(id = "default",
                            base = file("."),
                            settings = Project.defaultSettings ++ Seq(helloTask, helloTask2)) aggregate(dsl)

   lazy val dsl = Project(id = "dsl",
                            base = file("dsl"),
                            settings = Project.defaultSettings)
			.dependsOn(lmsproj)

   lazy val formatSourceSettings = seq(ScalariformPlugin.scalariformSettings: _*)

   lazy val spark = Project(id = "spark",
                            base = file("spark"),
                            settings = Project.defaultSettings ++ formatSourceSettings) 
				//.dependsOn(ProjectRef(uri("git://github.com/mesos/spark.git#master"),"core"))

   lazy val crunch = Project(id = "crunch",
                            base = file("crunch"),
                            settings = Project.defaultSettings ++ formatSourceSettings) 

   lazy val scoobi = Project(id = "scoobi",
                            base = file("scoobi"),
                            settings = Project.defaultSettings ++ formatSourceSettings) 

   lazy val gens = Project(id = "gens",
                            base = file("."),
                            settings = Project.defaultSettings) aggregate(spark, crunch, scoobi)


   val dotGen = TaskKey[Unit]("dotgen", "Runs the dot generation")
   val helloTask = dotGen := {
	 "bash updateDot.sh" .run
   }

   val cleanGen = TaskKey[Unit]("cleangen", "Cleans all the generated classes in the projects")
   val helloTask2 = cleanGen := {
	 "bash clean.sh" .run
   }

}
