import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random
import scala.collection.mutable
import java.io.File
import scala.collection.immutable
trait ComplexBase extends Base {

  class Complex

  def Complex(re: Rep[Double], im: Rep[Double]): Rep[Complex]
  def infix_re(c: Rep[Complex]): Rep[Double]
  def infix_im(c: Rep[Complex]): Rep[Double]
}

trait ComplexStructExp extends ComplexBase with StructExp {

  def Complex(re: Rep[Double], im: Rep[Double]) = struct[Complex](ClassTag[Complex]("Complex"), immutable.ListMap("re" -> re, "im" -> im))
  def infix_re(c: Rep[Complex]): Rep[Double] = field[Double](c, "re")
  def infix_im(c: Rep[Complex]): Rep[Double] = field[Double](c, "im")

}

trait DListsProg extends DListProgram with ComplexBase {

  def flatMapFusionTest(x: Rep[Unit]) = {
    val read = DList(getArgs(0))
    read
      .map(x => x)
      //      .flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
      .filter(x => x.length > 1)
      .save(getArgs(1))
    unit(())
  }

  def scrunch(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0))
    words1.map(_ + "").filter(_.length > 1)
      .flatMap(_.split("asdf").toSeq)
      .map(x => (x, unit(1))).groupByKey.reduce(_ + _)
      .save(getArgs(1))
  }

  def partitioner(x: Rep[Unit]) = {
    val words1 = DList("in").map(x => (x, unit(1)))
    words1
      .groupByKey(partitioner = { (x, y) => x.length % y })
      .reduce(_ + _)
      .save("out")
    words1.partitionBy { (x, y) => x.length % y }.save("out2")
  }

  def materialize(x: Rep[Unit]) = {
    val words1 = DList("in")
    val complexs = words1.map(x => Complex(x.toDouble, x.toDouble + 5.0))
    val it = complexs.materialize()
    println(it.head)
    val sampled = complexs.takeSample(0.0001).materialize
    println(sampled.head)
    val sampledSeed = complexs.takeSample(0.0001, 42).materialize
    println(sampledSeed.head)
  }

  def simple(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0))
    val complex = words1.map(x => Complex(x.toDouble, x.toDouble + 5.0))
    val c2 = (
      if (getArgs(2).toInt > 5)
        complex.map(x => (x.im))
      else
        complex.map(x => (x.re)))
    val c3 = (
      if (getArgs(3).toInt > 55)
        c2.map(x => x + 4)
      else
        c2.map(x => x - 4.0)
    )
    c2.save(getArgs(2))
    //    complex.save(getArgs(1))
    //    val tupled = words1
    //    tupled.map(x => (x, unit(1))).groupByKey.save(getArgs(1))
    //    tupled.map(x => (x, unit(1))).groupByKey.save(getArgs(2))

    //    words1
    //    //.filter(_.matches("\\d+"))
    //    .map(_.toInt)

    //)(0)
    unit(())
  }

  def testJoin(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0) + "1")
    val words2 = DList(getArgs(0) + "2")
    val words1Tupled = words1.map(x => (x, Complex(x.toDouble, 3.0)))
    val words2Tupled = words2.map(x => (x, Complex(2.5, x.toDouble)))
    val joined = words1Tupled.join(words2Tupled)
    joined.map(x => x._1 + " " + x._2._1.re + " " + x._2._2.im)
    //    joined
    //      .save(getArgs(1))
    words1Tupled.cogroup(words2Tupled).save("grouped")
    //    joined
    unit(())
  }

  def testWhile(x: Rep[Unit]) = {
    val nums = DList(getArgs(0)).map(_.toInt)
    var x = 0
    var quad = nums
    while (x < 5) {
      quad = quad.map(x => x + unit(2))
      x = x + 1
    }
    quad.save(getArgs(1))
    unit(())
  }

  /*
  def simple2(x: Rep[Unit]) = {
    val words1 = DList(getArgs(0))
    words1.map { x => (x, unit(1)) }
      .filter(!_._1.matches("asdf"))
      .map(_._2)
      .save(getArgs(1))
    //)(0)
    unit(())
  }
  */
}

trait DListsProgSortCrunch extends DListProgram with ComplexBase with CrunchDListOps {
  def testSort(x: Rep[Unit]) = {
    val x = DList("in")
    x.sort(true).save("out")
  }

}

trait DListsProgSortSpark extends DListProgram with ComplexBase with SparkDListOps {
  def testSort(x: Rep[Unit]) = {
    val x = DList("in")
    x.map(x => (x, x))
      .sortByKey(true).save("out")
  }
}

class TestBasic extends CodeGeneratorTestSuite {

  def testCrunch {
    tryCompile {
      println("-- begin")

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp with ApplicationOpsExp with SparkDListOpsExp

      val codegen = new CrunchGen {
        val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = true
      }
      val pw = setUpPrintWriter
      codegen.emitSource(dsl.testJoin, "TestBasic", pw)

      writeToProject(pw, "crunch", "TestBasic")
      release(pw)
      println("-- end")
    }
  }

  /*
  def testPrinter {
    try {
      println("-- begin")

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp

      val codegen = new BaseCodeGenerator with ScalaGenDList { val IR: dsl.type = dsl }
      val pw = setUpPrintWriter
      codegen.emitProgram(dsl.simple, "test", pw)
      println(getContent(pw))
//      writeToProject(pw, "spark", "SparkGenerated")
      release(pw)
      println("-- end")
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
    }

  }*/

  def testSpark {
    tryCompile {
      println("-- begin")

      import scala.virtualization.lms.common.{ StructFatExpOptCommon }
      val dsl = new DListsProg with DListProgramExp with ComplexStructExp with ApplicationOpsExp with SparkDListOpsExp with StructFatExpOptCommon
      val codegen = new SparkGen {
        val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = false
      }
      codegen.loopFusion = false
      val pw = setUpPrintWriter
      codegen.emitSource(dsl.testJoin, "flatMapFusionTest", pw)

      writeToProject(pw, "spark", "SparkGenerated")
      release(pw)
      println("-- end")
    }
  }

  def testScoobi {
    tryCompile {
      println("-- begin")
      val pw = setUpPrintWriter

      val dsl = new DListsProg with DListProgramExp with ComplexStructExp

      val codegen = new ScoobiGen { val IR: dsl.type = dsl }
      codegen.emitSource(dsl.testJoin, "g", pw)
      writeToProject(pw, "scoobi", "ScoobiGenerated")
      //      println(getContent(pw))
      release(pw)
      println("-- end")
    }
  }

  /*
  def testCrunchSort {
    tryCompile {
      println("-- begin")

      val dsl = new DListsProgSortCrunch with DListProgramExp with ComplexStructExp with ApplicationOpsExp with CrunchDListOpsExp

      val codegen = new CrunchEGen {
        val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = true
      }
      val pw = setUpPrintWriter
      codegen.emitSource(dsl.testSort, "TestSort", pw)

      writeToProject(pw, "crunch", "TestSort")
      release(pw)
      println("-- end")
    }
  }
  def testSparkSort {
    tryCompile {
      println("-- begin")

      val dsl = new DListsProgSortSpark with DListProgramExp with ComplexStructExp with ApplicationOpsExp with SparkDListOpsExp

      val codegen = new SparkGen {
        val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = false
      }
      val pw = setUpPrintWriter
      codegen.emitSource(dsl.testSort, "TestSort", pw)

      writeToProject(pw, "spark", "TestSort")
      release(pw)
      println("-- end")
    }
  }
*/

}

