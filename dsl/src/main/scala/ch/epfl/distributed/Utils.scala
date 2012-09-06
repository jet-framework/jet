package ch.epfl.distributed

trait Matchers extends AbstractScalaGenDList {
  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{ Stm, TP, SubstTransformer, Field }
  import IR.ClosureNode

  object SomeDef {
    def unapply(x: Any) = x match {
      //case TTPDef(x) => Some(x)
      case TP(_, x) => Some(x)
      case x: Def[_] => Some(x)
      case Def(x) => Some(x)
      //	          case x => Some(x)
      case x => None //{ println("did not match " + x); None }
    }
  }

  /*
  object TTPDef {
    def unapply(ttp: Stm) = ttp match {
      //case TTP(_, ThinDef(x)) => Some(x)
      case _ => None
    }
  }
  */

  object FieldAccess {
    def unapply(ttp: Stm) = ttp match {
      case SomeDef(f @ Field(obj, field, typ)) => Some(f)
      case _ => None
    }
  }

  object ClosureNode {
    def unapply(any: Any): Option[ClosureNode[_, _]] = any match {
      //      case SomeDef(cn: ClosureNode[_, _]) => Some(cn)
      case cn: ClosureNode[_, _] => Some(cn)
      case _ => None
    }
  }

  object SomeAccess {
    def unapply(ttp: Any) = ttp match {
      case SomeDef(IR.Tuple2Access1(d)) => Some((d, "._1"))
      case SomeDef(IR.Tuple2Access2(d)) => Some((d, "._2"))
      case SomeDef(IR.Field(struct, name, _)) => Some((struct, "." + name))
      case _ => None
    }
  }

  def printDef(x: Any) = {
    x match {
      case SomeDef(x) => "some def " + x
      case Block(x) => "Block " + x
      case _ => "not a def" + x
    }
  }

  object SimpleType {
    def unapply(x: Manifest[_]): Option[Manifest[_]] = x match {
      case _ if x.erasure.isPrimitive => Some(x)
      case _ if x.erasure.getName == "java.lang.String" => Some(x)
      case _ => None
    }
  }

  def isSimpleType(x: Manifest[_]) = SimpleType.unapply(x).isDefined

}