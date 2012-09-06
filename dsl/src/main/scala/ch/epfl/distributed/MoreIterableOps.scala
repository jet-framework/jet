package ch.epfl.distributed

import scala.virtualization.lms.common.{ ArrayOps, IterableOps, ArrayOpsExp, IterableOpsExp }
import scala.virtualization.lms.internal.ScalaCodegen
import java.io.PrintWriter
import scala.reflect.SourceContext

trait MoreIterableOps extends ArrayOps with IterableOps {
  implicit def arrayToMoreArrayOps[T: Manifest](a: Array[T]) = new MoreArrayOpsCls(unit(a))
  implicit def repArrayToMoreArrayOps[T: Manifest](a: Rep[Array[T]]) = new MoreArrayOpsCls(a)

  implicit def arrayToIterable[T: Manifest](a: Rep[Array[T]]) = a.asInstanceOf[Rep[Iterable[T]]]

  class MoreArrayOpsCls[T: Manifest](a: Rep[Array[T]]) {

  }

  implicit def arrayToMoreIterableOps[T: Manifest](a: Rep[Array[T]]) = new MoreIterableOpsCls(a)
  implicit def repIterableToMoreIterableOps[T: Manifest](a: Rep[Iterable[T]]) = new MoreIterableOpsCls(a)
  implicit def iterableToMoreIterableOps[T: Manifest](a: Iterable[T]) = new MoreIterableOpsCls(unit(a))

  class MoreIterableOpsCls[T: Manifest](a: Rep[Iterable[T]]) {
    def toArray = iterable_toArray(a)
    def last = iterable_last(a)
    def head = iterable_head(a)
  }

  def iterable_last[T: Manifest](a: Rep[Iterable[T]]): Rep[T]
  def iterable_head[T: Manifest](a: Rep[Iterable[T]]): Rep[T]
  def iterable_toArray[T: Manifest](a: Rep[Iterable[T]]): Rep[Array[T]]

}

trait MoreIterableOpsExp extends MoreIterableOps with ArrayOpsExp with IterableOpsExp {

  case class SingleResultIterableOp[T: Manifest](it: Exp[Iterable[T]], op: String) extends Def[T]
  case class IterableToArrayOp[T: Manifest](it: Exp[Iterable[T]]) extends Def[Array[T]]

  override def iterable_last[T: Manifest](a: Exp[Iterable[T]]) = SingleResultIterableOp(a, "last")
  override def iterable_head[T: Manifest](a: Exp[Iterable[T]]) = SingleResultIterableOp(a, "head")
  override def iterable_toArray[T: Manifest](a: Rep[Iterable[T]]) = IterableToArrayOp(a)

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = (e match {
    case SingleResultIterableOp(a, op) => SingleResultIterableOp(f(a), op)
    case IterableToArrayOp(a) => IterableToArrayOp(f(a))
    case _ => super.mirrorDef(e, f)
  }).asInstanceOf[Def[A]]

}

trait MoreIterableOpsCodeGen extends ScalaCodegen {

  val IR: MoreIterableOpsExp
  import IR._

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case SingleResultIterableOp(it, s) => emitValDef(sym, "%s.%s".format(quote(it), s))
    case IterableToArrayOp(it) => emitValDef(sym, "%s.toArray".format(quote(it)))
    case _ => super.emitNode(sym, rhs)
  }
}
