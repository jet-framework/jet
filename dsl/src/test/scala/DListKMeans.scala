import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, BaseExp, StructExp, PrimitiveOps }
import scala.virtualization.lms.util.OverloadHack
import scala.util.Random
import scala.virtualization.lms.internal.ScalaCodegen
import scala.virtualization.lms.internal.CodeMotion
import scala.virtualization.lms.common.ScalaGenIterableOps
import scala.reflect.SourceContext

trait VectorBase extends Base with OverloadHack {
  trait Vector

  def Vector(elems: Rep[Array[Double]]): Rep[Vector]

  implicit def vecToVecOps(v: Rep[Vector]) = new vecOps(v)

  class vecOps(v: Rep[Vector]) {
    def *(o: Rep[Vector]) = vec_simpleOp(v, o, "*")
    def +(o: Rep[Vector]) = vec_simpleOp(v, o, "+")
    def -(o: Rep[Vector]) = vec_simpleOp(v, o, "-")
    def /(o: Rep[Vector])(implicit o1: Overloaded1) = vec_simpleOp(v, o, "/")
    def /(o: Rep[Double])(implicit o2: Overloaded2) = vec_pointWiseOp(v, o, "/")
    def squaredDist(o: Rep[Vector]) = vec_squaredDist(v, o)
    def print() = vec_print(v)
  }

  def vec_simpleOp(v: Rep[Vector], o: Rep[Vector], op: String): Rep[Vector]
  def vec_pointWiseOp(v: Rep[Vector], o: Rep[Double], op: String): Rep[Vector]
  def vec_squaredDist(v: Rep[Vector], o: Rep[Vector]): Rep[Double]
  def vec_print(v: Rep[Vector]): Rep[String]
}

trait VectorBaseExp extends VectorBase with BaseExp {
  case class NewVector(elems: Exp[Array[Double]]) extends Def[Vector]
  case class VectorSimpleOp(v1: Exp[Vector], v2: Exp[Vector], op: String) extends Def[Vector]
  case class VectorPointWiseOp(v: Exp[Vector], d: Exp[Double], op: String) extends Def[Vector]
  case class VectorSquaredDist(v1: Exp[Vector], v2: Exp[Vector]) extends Def[Double]
  case class VectorPrint(v1: Exp[Vector]) extends Def[String]

  def Vector(elems: Exp[Array[Double]]) = NewVector(elems)

  def vec_simpleOp(v: Exp[Vector], o: Exp[Vector], op: String) = VectorSimpleOp(v, o, op)
  def vec_pointWiseOp(v: Exp[Vector], o: Exp[Double], op: String) = VectorPointWiseOp(v, o, op)
  def vec_squaredDist(v: Exp[Vector], o: Exp[Vector]) = VectorSquaredDist(v, o)
  def vec_print(v: Exp[Vector]) = VectorPrint(v)

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = (e match {
    case NewVector(elems) => NewVector(f(elems))
    case VectorSimpleOp(v1, v2, op) => VectorSimpleOp(f(v1), f(v2), op)
    case VectorPointWiseOp(v1, v2, op) => VectorPointWiseOp(f(v1), f(v2), op)
    case VectorSquaredDist(v1, v2) => VectorSquaredDist(f(v1), f(v2))
    case VectorPrint(elems) => VectorPrint(f(elems))
    case _ => super.mirrorDef(e, f)
  }).asInstanceOf[Def[A]]

}

trait ScalaVectorCodeGen extends ScalaCodegen {
  val IR: VectorBaseExp
  import IR._
  def writeLoop(initOutVar: String, loopBody: String, desc: String, var1: String, var2: String = "") = {
    val out = """ {
          // %s
    		%s
    	    var out = %s
    	    var i = 0
    	    while (i < $var1.size) {
    			%s
    			i += 1
    		}
    	    out
    		}""".format(desc,
      if (var2 != "") """if ($var1.size != $var2.size) 
    	        						throw new IllegalArgumentException("Should have same length")"""
      else "",
      initOutVar, loopBody)
    out.replaceAll("\\$var1", var1).replaceAll("\\$var2", var2)
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case nv @ NewVector(elems) => emitValDef(sym, quote(elems))
    case VectorSimpleOp(v1, v2, op) => {
      val q1 = quote(v1)
      val q2 = quote(v2)
      emitValDef(sym, writeLoop("new Array[Double]($var1.size)", "out(i) = $var1(i) " + op + " $var2(i)", "Vector Simple Op", q1, q2))
    }
    case VectorPointWiseOp(v, d, op) => {
      val qv = quote(v)
      val qd = quote(d)
      emitValDef(sym, writeLoop("new Array[Double]($var1.size)", "out(i) = $var1(i) " + op + " " + qd, "Vector Point wise op", qv))
    }
    case VectorSquaredDist(v1, v2) => {
      val q1 = quote(v1)
      val q2 = quote(v2)
      emitValDef(sym, writeLoop("0.0", "val dist = $var1(i) - $var2(i); out += dist * dist", "Vector Squared Dist", q1, q2))
    }

    case VectorPrint(v1) => {
      emitValDef(sym, """%s.mkString("(", ",", ")")""".format(quote(v1)))
    }

    case _ => super.emitNode(sym, rhs)
  }
  override def remap[A](m: Manifest[A]): String = {
    if (m <:< manifest[Vector]) {
      "Array[Double]"
    } else {
      super.remap(m)
    }
  }
}

trait KMeansApp extends DListProgram with ApplicationOps with SparkDListOps with VectorBase {
  def parseVector(line: Rep[String]): Rep[Vector] = {
    Vector(line.split(" ").map(_.toDouble))
  }

  def closestPoint(p: Rep[Vector], centers: Rep[Array[Vector]]): Rep[Int] = {
    var index = 0
    var bestIndex = 0
    var closest = Double.PositiveInfinity

    var i = 0
    while (i < centers.length) {
      val vCurr = centers(i)
      val tempDist = p.squaredDist(vCurr)
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      } else { unit(()) }
      i = i + 1
    }

    return bestIndex
  }

  def kmeans(x: Rep[Unit]) = {
    val lines = DList(getArgs(0))
    val data = lines.map(parseVector).cache()
    val K = getArgs(1).toInt
    val convergeDist = getArgs(2).toDouble
    report("Taking sample")
    // TODO spark code uses take Sample, not available in scoobi
    var centers = data.takeSample(false, K, 42).toArray
    var tempDist = unit(1.0)
    var i = 0
    report("Starting big while")

    while (i < 5) {
      val closest = data.map { p =>
        val tup: Rep[(Vector, Int)] = (p, unit(1))
        (closestPoint(p, centers), tup)
      }

      var pointStats = closest.groupByKey.reduce {
        (p1, p2) =>
          (p1._1 + p2._1, p1._2 + p2._2)
      }

      val newPoints = pointStats.map { pair => (pair._1, pair._2._1 / pair._2._2) }.materialize().toArray

      tempDist = 0.0
      for (pair <- newPoints) {
        tempDist += centers(pair._1).squaredDist(pair._2)
      }

      centers = newPoints.toArray.sortBy(_._1).map(_._2)

      report("Iteration " + i + " done, distance " + tempDist)

      i = i + 1
      unit(())
    }

    report("5 iterations done")
    //    for (center <- centers) {
    //      println(center.print())
    //    }

    unit(())
  }

}

class KMeansAppGenerator extends CodeGeneratorTestSuite {

  val appname = "KMeansApp"

  def testSpark {
    tryCompile {
      println("-- begin")

      val dsl = new KMeansApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp with VectorBaseExp

      val codegen = new SparkGen with ScalaVectorCodeGen with ScalaGenIterableOps {
        val IR: dsl.type = dsl

      }
      codegen.loopFusion = false
      codegen.reduceByKey = true
      var pw = setUpPrintWriter
      codegen.emitSource(dsl.kmeans, appname, pw)

      writeToProject(pw, "spark", appname)
      release(pw)

      println("-- end")
    }
  }

}

/*
  def squaredDistForMin(o: Rep[Vector], max: Rep[Double]) = vec_squaredDistForMin(v, o, max)
  def vec_squaredDistForMin(v: Rep[Vector], o: Rep[Vector], max: Rep[Double]): Rep[Double]
  case class VectorSquaredDistForMin(v1: Exp[Vector], v2: Exp[Vector], max: Exp[Double]) extends Def[Double]
  def vec_squaredDistForMin(v: Exp[Vector], o: Exp[Vector], max: Rep[Double]) = VectorSquaredDistForMin(v, o, max)
  case VectorSquaredDistForMin(v1, v2, max) => VectorSquaredDistForMin(f(v1), f(v2), f(max))
case VectorSquaredDistForMin(v1, v2, max) => {
      val q1 = quote(v1)
      val q2 = quote(v2)
      emitValDef(sym, """
    	    {
    		  // Vector Squared Dist
    	    if (%s.size != %s.size)
    			throw new IllegalArgumentException("Should have same length")
    	    var out = 0.0
    	    var i = 0
    	    while (i < %s.size && out <= %s) {
    			val dist = %s(i) - %s(i)
    	  		out += dist * dist
    			i += 1
    		}
    	    out
    		}
    	    """.format(q1, q2, q1, quote(max), q1, q2))
    }
*/
