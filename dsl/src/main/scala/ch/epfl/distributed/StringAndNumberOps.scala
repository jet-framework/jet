package ch.epfl.distributed

import scala.virtualization.lms.common.PrimitiveOps
import scala.virtualization.lms.common.StringOps
import scala.virtualization.lms.common.StringOpsExp
import scala.virtualization.lms.common.PrimitiveOpsExp
import scala.reflect.SourceContext
import scala.virtualization.lms.internal.ScalaCodegen
import java.io.PrintWriter
import scala.virtualization.lms.util.OverloadHack

trait StringAndNumberOps extends PrimitiveOps with StringOps with OverloadHack {
  def infix_toLong(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Long](s1)
  def infix_toDouble(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Double](s1)
  def infix_toInt(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Int](s1)
  def infix_toByte(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Byte](s1)
  def infix_toFloat(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Float](s1)
  def infix_toBoolean(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Boolean](s1)
  def infix_toShort(s1: Rep[String])(implicit ctx: SourceContext) = string_toNumber[Short](s1)
  def infix_substring(s: Rep[String], start: Rep[Int])(implicit ctx: SourceContext) = string_substring(s, start)
  def infix_toChar(s1: Rep[String])(implicit ctx: SourceContext) = string_toChar(s1)

  //  def infix_%( l : Rep[Long], mod : Rep[Long])(implicit o: Overloaded2, ctx: SourceContext) = long_modulo(l, mod)
  //  

  def string_toNumber[A <: AnyVal: Manifest](s: Rep[String])(implicit ctx: SourceContext): Rep[A]
  def string_toChar(s: Rep[String])(implicit ctx: SourceContext): Rep[Char]
  def string_substring(s: Rep[String], start: Rep[Int])(implicit ctx: SourceContext): Rep[String]

  def report(x: Rep[String]) = report_time(x: Rep[String])
  def report_time(x: Rep[String]): Rep[Unit]

  //  def long_modulo( l : Rep[Long], mod : Rep[Long])(implicit ctx: SourceContext) : Rep[Long]
  //  implicit def repStringToStringOps(s: Rep[String]) = new stringOpsCls(s)
  //  class stringOpsCls(s: Rep[String]) {
  //    def +(other: Rep[Any]) = string_plus(s, other)
  //  }

}

trait StringAndNumberOpsExp extends StringAndNumberOps with PrimitiveOpsExp with StringOpsExp {

  case class StringToNumber[A <: AnyVal: Manifest](s: Exp[String]) extends Def[A] {
    val m = manifest[A]
    val typeName = m.toString.reverse.takeWhile(_ != '.').reverse
  }
  case class StringSubstring(s: Exp[String], start: Exp[Int]) extends Def[String]

  case class StringToChar(s: Exp[String]) extends Def[Char]

  case class ReportTime(x: Exp[String]) extends Def[Unit]

  //  case class LongModulo(l : Exp[Long], mod : Exp[Long]) extends Def[Long]
  //  
  override def string_toNumber[A <: AnyVal: Manifest](s: Rep[String])(implicit ctx: SourceContext) = StringToNumber[A](s)
  //  override def long_modulo( l : Exp[Long], mod : Exp[Long])(implicit ctx: SourceContext) = LongModulo(l, mod)

  override def report_time(x: Exp[String]) = reflectEffect(ReportTime(x: Exp[String]))

  // toInt etc do not trim the input, they just reject it
  val hardParser = Array.apply("Int", "Short", "Byte")

  override def string_valueof(d: Exp[Any]) = d match {
    case Def(stn @ StringToNumber(s)) if (hardParser.contains(stn.typeName)) => s
    case Def(StringToNumber(s)) => string_trim(s)
    case _ => super.string_valueof(d)
  }

  def string_toChar(s: Rep[String])(implicit ctx: SourceContext) = StringToChar(s)
  def string_substring(s: Exp[String], start: Exp[Int])(implicit ctx: SourceContext) = StringSubstring(s, start)
  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = (e match {
    case n @ StringToNumber(s) => StringToNumber(f(s))(n.m)
    case StringSubstring(s, i) => StringSubstring(f(s), f(i))
    case n @ StringToChar(s) => StringToChar(f(s))
    case ReportTime(x) => ReportTime(f(x))
    case _ => super.mirrorDef(e, f)
  }).asInstanceOf[Def[A]]

  override def mirror[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = (e match {
    case StringValueOf(a) => string_valueof(f(a))
    case _ => super.mirror(e, f)
  }).asInstanceOf[Exp[A]]

}
package datastruct {
  class RegexFrontend
}

trait StringPatternOpsExp extends StringOps with StringOpsExp {

  var disablePatterns = false
  var useFastRegex = true
  var useFastSplitter = true
  import datastruct.RegexFrontend
  case class StringPattern(regex: Exp[String], useFrontend: Boolean = useFastRegex, useFastSplitter: Boolean = useFastSplitter) extends Def[RegexFrontend]
  case class StringSplitPattern(s: Exp[String], pattern: Exp[RegexFrontend], limit: Exp[Int]) extends Def[Array[String]]
  case class StringMatchesPattern(string: Exp[String], pattern: Exp[RegexFrontend]) extends Def[Boolean]
  case class StringReplaceAllPattern(string: Exp[String], pattern: Exp[RegexFrontend], repl: Exp[String]) extends Def[String]

  override def string_split(s: Rep[String], separators: Rep[String], limit: Rep[Int]) =
    if (disablePatterns)
      super.string_split(s, separators, limit)
    else
      StringSplitPattern(s, StringPattern(separators), limit)
  override def string_matches(s: Exp[String], regex: Exp[String]) =
    if (disablePatterns)
      super.string_matches(s, regex)
    else
      StringMatchesPattern(s, StringPattern(regex))
  override def string_replaceall(s: Exp[String], regex: Exp[String], repl: Exp[String])(implicit ctx: SourceContext) =
    if (disablePatterns)
      super.string_replaceall(s, regex, repl)
    else
      StringReplaceAllPattern(s, StringPattern(regex), repl)

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit ctx: SourceContext): Def[A] = (e match {
    case StringPattern(regex, useFrontend, fs) => StringPattern(f(regex), useFrontend, fs)
    case StringSplitPattern(s, pat, l) => StringSplitPattern(f(s), f(pat), f(l))
    case StringReplaceAllPattern(s, pat, l) => StringReplaceAllPattern(f(s), f(pat), f(l))
    case StringMatchesPattern(s, pat) => StringMatchesPattern(f(s), f(pat))
    case _ => super.mirrorDef(e, f)
  }).asInstanceOf[Def[A]]

}

trait StringAndNumberOpsCodeGen extends ScalaCodegen {

  val IR: StringAndNumberOpsExp
  import IR._
  //  override def emitNode(sym: Sym[Any], rhs: Def[Any])(implicit stream: PrintWriter): Unit = rhs match {
  //    case StringToDouble(s) => emitValDef(sym, "java.lang.Double.valueOf(%s)".format(quote(s)))
  //    case LongModulo(l, mod) => emitValDef(sym, "%s %% %s".format(quote(l), quote(mod)))
  //    case _ => super.emitNode(sym, rhs)
  //  }

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case StringToChar(s) => emitValDef(sym, "%s.charAt(0)".format(quote(s)))
    case n @ StringToNumber(s) => emitValDef(sym, "%s.to%s".format(quote(s), n.typeName))
    case StringSubstring(s, start) => emitValDef(sym, "%s.substring(%s)".format(quote(s), quote(start)))
    case ReportTime(x) => stream.println("""println(System.currentTimeMillis+" "+%s)""".format(quote(x)))
    case _ => super.emitNode(sym, rhs)
  }

}

trait StringPatternOpsCodeGen extends ScalaCodegen {
  val IR: StringPatternOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case StringSplitPattern(s, pattern, limit) => emitValDef(sym, "%s.split(%s, %s)".format(quote(pattern), quote(s), quote(limit)))
    case StringPattern(s, reg, fs) => emitValDef(sym, "new ch.epfl.distributed.datastruct.RegexFrontend(%s, %s, %s)".format(quote(s), reg, fs))
    case StringMatchesPattern(s, pattern) => emitValDef(sym, "%s.matches(%s)".format(quote(pattern), quote(s)))
    case StringReplaceAllPattern(s, pattern, repl) => emitValDef(sym, "%s.replaceAll(%s, %s)".format(quote(pattern), quote(s), quote(repl)))
    case _ => super.emitNode(sym, rhs)
  }

}
