import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait ExampleApp extends DListProgram with ApplicationOps with SparkDListOps with StringAndNumberOps {
  // start
  def isSuspicious(x: Rep[Int]) = String.valueOf(x).matches("[45]\\d{2}")

  def example(x: Rep[Unit]) = {
    val log = DList("logentries").map(LogEvent.parse(_, "\t"))
    val logFiltered = log.filter(e => isSuspicious(e.respCode))
    val logTupled = logFiltered.map(e => (e.userId, e))
    val users = DList("users").map(User.parse(_, ";"))
    val joined = logTupled.join(users.map(u => (u.id, u)))
    //    joined.map { 
    //      case (uid, (event, user)) =>
    //        "User " + user.name + " issued a suspicious request on " + event.date
    //    }
    //    .save("analysis.txt")
    joined.map { in =>
      "User " + in._2._2.name +
        " issued a suspicious request on " + in._2._1.date
    }.save("analysis.txt")
    unit(())
  }
  // end

  def describe(t: Rep[(Int, Person)]) =
    t._2.name + " is the oldest person in " + t._1

  def example2(x: Rep[Unit]) = {
    val persons = DList("persons").map(Person.parse(_, "\t"))
    persons
      .map(p => (p.zipcode, p))
      .groupByKey()
      .reduce((p1, p2) => if (p1.age > p2.age) p1 else p2)
      .map(describe)
      .save("oldestPersons")
    unit(())
  }
  // end
  def exampleLMS(x: Rep[Unit]) = {
    val persons = DList("persons")
    persons
      .map(Person.parse(_, "\t"))
      .filter(_.age > 100)
      .map(_.name)
      .save("oldestPersons")
    unit(())
  }

}

class ExampleAppGenerator extends CodeGeneratorTestSuite {

  val appname = "ExampleApp"
  
  // format: OFF
  /**
   * Variants:
   *  		CM	FR	LF	IN
   * v0:	-	-	-	-
   * v1:	x	-	-	-
   * v2:	x	x	-	-
   * v3:	x	x	x	x
   */
  // format: ON
  def test {
    tryCompile {
      println("-- begin")
      var fusionEnabled = false
      val dsl = new ExampleApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp
      val codegenSpark = new SparkFlatMapGen { val IR: dsl.type = dsl }
//      val codegenScoobi = new ScoobiGen { val IR: dsl.type = dsl
//        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = fusionEnabled
//      }
      val codegenCrunch = new CrunchGen { val IR: dsl.type = dsl }
//      val list = List(codegenSpark, codegenScoobi, codegenCrunch)
      val list = List(codegenSpark, codegenCrunch)
      def writeVersion(version: String) {
//        if (version != "v3") return
        val func = dsl.example2 _
        for (gen <- list) {
          val versionDesc = version+ (gen match {
            case x: Versioned => x.version
            case _ => ""
          })
          var pw = setUpPrintWriter
          gen.emitProgram(func, appname, pw, versionDesc)
          writeToProject(pw, gen.getProjectName, appname, versionDesc, codegenSpark.lastGraph)
        }
      }
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
        codegen.inlineInLoopFusion = false
        codegen.loopFusion = false
        codegen.inlineClosures = true
      }
      fusionEnabled = false
      dsl.disablePatterns = true
      writeVersion("v0")
      
      dsl.disablePatterns = false
      writeVersion("v1")
      
      list.foreach { codegen =>
//        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
      }
      fusionEnabled = true
      writeVersion("v2")
      
      list.foreach { codegen =>
        codegen.inlineInLoopFusion = true
        codegen.loopFusion = true
      }
      writeVersion("v3")

      println("-- end")
    }
  }

}
