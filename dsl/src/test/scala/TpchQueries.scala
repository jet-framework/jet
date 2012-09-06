import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps, LiftNumeric }
import scala.util.Random

trait TpchQueriesApp extends DListProgram with ApplicationOps {

  def loadTest(x: Rep[Unit]) = {
    val read = DList(getArgs(0))
    val parsed = read.map(x => LineItem.parse(x, "\\|"))
    parsed
      .filter(_.l_linestatus == 'F')
      .map(x => (x.l_receiptdate, x.l_comment))
      .save(getArgs(1))

    unit(())
  }

  //  def query3nephele(x: Rep[Unit]) = {
  //    val limit = getArgs(1).toInt
  //    val date = getArgs(2).toDate
  //    val lineitems = DList(getArgs(0) + "/lineitem.tbl")
  //      .map(x => LineItem.parse(x, "\\|"))
  //    val orders = DList(getArgs(0) + "/orders.tbl")
  //      .map(x => Order.parse(x, "\\|"))
  //    val filteredOrders = orders
  //      .filter(x => x.o_custkey < limit)
  //      .filter(x => date < x.o_orderdate)
  //    val lineItemTuples = lineitems.map(x => (x.l_orderkey, x))
  //    val orderTuples = filteredOrders.map(x => (x.o_orderkey, x))
  //    val joined = lineItemTuples.join(orderTuples)
  //    val tupled = joined.map { x => val y: Rep[(Int, Int)] = (x._2._1.l_orderkey, x._2._2.o_shippriority); (y, x._2._1.l_extendedprice) }
  //    val grouped = tupled.groupByKey
  //    grouped.reduce((x, y) => x + y)
  //      .save(getArgs(3))
  //    unit(())
  //  }

  // val x91 = new ch.epfl.distributed.datastruct.Interval(1996, 1, 1);

  def query12(x: Rep[Unit]) = {
    // read arguments
    val inputFolder = getArgs(0)
    val outputFolder = getArgs(1)
    val date = getArgs(2).toDate
    val shipModes = getArgs(3)

    // read and parse tables
    val lineitems = DList(getArgs(0) + "/lineitem/")
      .map(x => LineItem.parse(x, "\\|"))
    val orders = DList(getArgs(0) + "/orders/")
      .map(x => Order.parse(x, "\\|"))

    // filter the line items
    val filteredLineitems = lineitems
      .filter(x => x.l_shipmode.matches(shipModes))
      .filter(x => date <= x.l_receiptdate)
      .filter(x => x.l_shipdate < x.l_commitdate)
      .filter(x => x.l_commitdate < x.l_receiptdate)
      .filter(x => x.l_receiptdate < date + (1, 0, 0))
    // perform the join
    val orderTuples = orders.map(x => (x.o_orderkey, x))
    val lineItemTuples = filteredLineitems.map(x => (x.l_orderkey, x))
    val joined = lineItemTuples.join(orderTuples, getArgs(4).toInt)
    // prepare for aggregation
    val joinedTupled = joined.map {
      x =>
        val prio = x._2._2.o_orderpriority;
        val isHigh = prio.matches("1-URGENT") || prio.matches("2-HIGH")
        //val isHigh = prio.startsWith("1") || prio.startsWith("2");
        val count = if (isHigh) 1 else 0
        val part2: Rep[(Int, Int)] = (count, 1 - count)
        (x._2._1.l_shipmode, part2)
    }

    // aggregate and save
    val reduced = joinedTupled.groupByKey(1).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    reduced.map {
      x =>
        "shipmode " + x._1 + ": high " + x._2._1 + ", low " + x._2._2
    }.save(getArgs(1))

    unit(())
  }

  def query12Mapper(x: Rep[Unit]) = {
    // read arguments
    val inputFolder = getArgs(0)
    val outputFolder = getArgs(1)
    val date = getArgs(2).toDate
    val shipMode1 = getArgs(3)
    val shipMode2 = getArgs(4)

    // read and parse tables
    val lineitems = DList(getArgs(0) + "/lineitem*")
      .map(x => LineItem.parse(x, "\\|"))

    // filter the line items
    val filteredLineitems = lineitems
      .filter(x => x.l_shipmode == shipMode1 || x.l_shipmode == shipMode2)
      .filter(x => date <= x.l_receiptdate)
      .filter(x => x.l_shipdate < x.l_commitdate)
      .filter(x => x.l_commitdate < x.l_receiptdate)
      .filter(x => x.l_receiptdate < date + (1, 0, 0))
    val lineItemTuples = filteredLineitems.map(x => (x.l_orderkey, x.l_shipmode))
    lineItemTuples.save(outputFolder)
  }

  /*
  def tupleProblem(x: Rep[Unit]) = {
    val lineitems = DList(getArgs(0) + "/lineitem*")
      .map(x => LineItem.parse(x, "\\|"))
    val tupled = lineitems.map(x => ((x.l_linenumber, x.l_orderkey), x.l_comment))
    tupled
      .save(getArgs(3))
  }
  */

}

class TpchQueriesAppGenerator extends CodeGeneratorTestSuite {

  //  val appname = "TpchQueriesMapper"
  val appname = "TpchQueries"

  // format: OFF
  /**
   * Variants:
   *  		FR	LF 	IN
   * v0:	-	-	-
   * v1:	x	-	-
   * v2:	-	x	-
   * v3:	-	x	x
   * v4:	x 	x	-
   * v5:	x	x 	x
   * new:
   * All versions have Regex patterns enabled,
   * just the fast splitter is disabled.
   * 		FS 	FR	LF+IN
   * v0:	- 	- 	-
   * v1:	x 	- 	-
   * v2:	- 	x 	-
   * v3:	- 	- 	x
   * v4:	x 	x 	x
   * Scoobi and crunch with writables.
   */
  // format: ON
  def testBoth {
    tryCompile {
      println("-- begin")
      var applyFusion = true
      val dsl = new TpchQueriesApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp {
        override val verbosity = 1
      }
      val codegenSpark = new SparkGen {
        val IR: dsl.type = dsl
      }
      val codegenScoobi = new ScoobiGen { val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = applyFusion
      }
//      codegenScoobi.useWritables = true
//      val codegenKryoScoobi = new KryoScoobiGen with Versioned {
//        val IR: dsl.type = dsl
//		  val version = "k"      	
//      }
      val codegenCrunch = new CrunchGen {
        val IR: dsl.type = dsl
      }
//      val codegenKryoCrunch = new KryoCrunchGen {
//        val IR: dsl.type = dsl
//      }
      val list = List(codegenSpark,codegenCrunch)
      //val list = List(codegenCrunch)
      def writeVersion(version: String) {
        //if (version != "v4") return
        val func = dsl.query12 _
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
        codegen.loopFusion = false
        codegen.inlineClosures = false
        codegen.inlineInLoopFusion = false
      }
      applyFusion = false
      dsl.useFastRegex = false
      dsl.useFastSplitter = false
      writeVersion("v0")
      
      dsl.useFastSplitter = true
      dsl.useFastRegex = true
      writeVersion("v1")
      
      dsl.useFastRegex = false
      dsl.useFastSplitter = false
      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
      }
      writeVersion("v2")
      
      list.foreach { codegen =>
        codegen.loopFusion = true
        codegen.inlineClosures = false
        codegen.inlineInLoopFusion = true
      }
      applyFusion = true
      dsl.useFastSplitter = true
      dsl.useFastRegex = true
      writeVersion("v4")

      dsl.useFastSplitter = false
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
      }
      writeVersion("v3")
      
      println("-- end")
    }

  }

}
