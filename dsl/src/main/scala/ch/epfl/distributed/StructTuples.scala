package ch.epfl.distributed
import scala.virtualization.lms.common.{ TupleOps, TupleOpsExp, BaseExp, StructExp }
import scala.reflect.SourceContext

trait StructTupleOpsExp extends TupleOpsExp with StructExp {

  class TupleStructTag[A: Manifest, B: Manifest]() extends ClassTag[(A, B)]("tuple2s") {
    val mA = manifest[A]
    val mB = manifest[B]
  }

  implicit override def make_tuple2[A: Manifest, B: Manifest](t: (Exp[A], Exp[B]))(implicit pos: SourceContext): Exp[(A, B)] =
    SimpleStruct[Tuple2[A, B]](new TupleStructTag[A, B](), Map("_1" -> t._1, "_2" -> t._2))

  override def tuple2_get1[A: Manifest](t: Exp[(A, _)])(implicit pos: SourceContext) = t match {
    //    case Def(ETuple2(a,b)) => a
    case _ => field[A](t, "_1")
  }
  override def tuple2_get2[B: Manifest](t: Exp[(_, B)])(implicit pos: SourceContext) = t match {
    //    case Def(ETuple2(a,b)) => b
    case _ => field[B](t, "_2")
  }

}