package ch.epfl.distributed

import scala.virtualization.lms.common._
import scala.virtualization.lms.internal._
import scala.virtualization.lms.util.OverloadHack
import java.io.PrintWriter
import java.io.FileOutputStream
import java.io.PrintWriter
import java.io.File
import java.io.FileWriter
import java.io.StringWriter
import scala.reflect.SourceContext
import scala.collection.mutable
import scala.collection.immutable
import java.util.regex.Pattern

trait DList[+A]

trait DListOps extends Base with Variables {
  def getArgs = get_args()

  type PartitionerUser[K] = (Rep[K], Rep[Int]) => Rep[Int]

  object DList {
    def apply(file: Rep[String]) = dlist_new[String](file)
  }

  implicit def repDListToDListOps[A: Manifest](dlist: Rep[DList[A]]) = new dlistOpsCls(dlist)
  implicit def varDListToDListOps[A: Manifest](dlist: Var[DList[A]]) = new dlistOpsCls(readVar(dlist))
  class dlistOpsCls[A: Manifest](dlist: Rep[DList[A]]) {
    def flatMap[B: Manifest](f: Rep[A] => Rep[Iterable[B]]) = dlist_flatMap(dlist, f)
    def map[B: Manifest](f: Rep[A] => Rep[B]) = dlist_map(dlist, f)
    def filter(f: Rep[A] => Rep[Boolean]) = dlist_filter(dlist, f)
    def save(path: Rep[String]) = dlist_save(dlist, path)
    def ++(dlist2: Rep[DList[A]]) = dlist_++(dlist, dlist2)
    def materialize() = dlist_materialize(dlist)
    def takeSample(fraction: Rep[Double], seed: Rep[Int]) = dlist_takeSample(dlist, fraction, Some(seed))
    def takeSample(fraction: Rep[Double]) = dlist_takeSample(dlist, fraction, None)
  }

  implicit def repDListToDListIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, Iterable[V])]]) = new dlistIterableTupleOpsCls(x)
  implicit def varDListToDListIterableTupleOpsCls[K: Manifest, V: Manifest](x: Var[DList[(K, Iterable[V])]]) = new dlistIterableTupleOpsCls(readVar(x))
  class dlistIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, Iterable[V])]]) {
    def reduce(f: (Rep[V], Rep[V]) => Rep[V]) = dlist_reduce[K, V](x, f)
  }

  implicit def repDListToDListTupleOps[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) = new dlistTupleOpsCls(x)
  implicit def varDListToDListTupleOps[K: Manifest, V: Manifest](x: Var[DList[(K, V)]]) = new dlistTupleOpsCls(readVar(x))
  class dlistTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) {
    def groupByKey(splits: Rep[Int] = unit(-2), partitioner: PartitionerUser[K] = null) = dlist_groupByKey[K, V](x, splits, partitioner)
    def partitionBy(partitioner: PartitionerUser[K]) = dlist_partitionBy[K, V](x, partitioner)
    def groupByKey = dlist_groupByKey[K, V](x, unit(-2), null)
    def join[V2: Manifest](right: Rep[DList[(K, V2)]], splits: Rep[Int] = unit(-2)) = dlist_join(x, right, splits)
    def cogroup[V2: Manifest](right: Rep[DList[(K, V2)]]) = dlist_cogroup(x, right)
  }

  def get_args(): Rep[Array[String]]

  //operations
  def dlist_new[A: Manifest](file: Rep[String]): Rep[DList[String]]
  def dlist_map[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[B]): Rep[DList[B]]
  def dlist_flatMap[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Iterable[B]]): Rep[DList[B]]
  def dlist_filter[A: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Boolean]): Rep[DList[A]]
  def dlist_save[A: Manifest](dlist: Rep[DList[A]], path: Rep[String]): Rep[Unit]
  def dlist_++[A: Manifest](dlist1: Rep[DList[A]], dlist2: Rep[DList[A]]): Rep[DList[A]]
  def dlist_reduce[K: Manifest, V: Manifest](dlist: Rep[DList[(K, Iterable[V])]], f: (Rep[V], Rep[V]) => Rep[V]): Rep[DList[(K, V)]]
  def dlist_join[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[DList[(K, V1)]], right: Rep[DList[(K, V2)]], splits: Rep[Int]): Rep[DList[(K, (V1, V2))]]
  def dlist_cogroup[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[DList[(K, V1)]], right: Rep[DList[(K, V2)]]): Rep[DList[(K, (Iterable[V1], Iterable[V2]))]]
  def dlist_groupByKey[K: Manifest, V: Manifest](dlist: Rep[DList[(K, V)]], splits: Rep[Int], partitioner: PartitionerUser[K]): Rep[DList[(K, Iterable[V])]]
  def dlist_partitionBy[K: Manifest, V: Manifest](dlist: Rep[DList[(K, V)]], partitioner: PartitionerUser[K]): Rep[DList[(K, V)]]
  def dlist_materialize[A: Manifest](dlist: Rep[DList[A]]): Rep[Iterable[A]]
  def dlist_takeSample[A: Manifest](dlist: Rep[DList[A]], fraction: Rep[Double], seed: Option[Rep[Int]]): Rep[DList[A]]
}

object FakeSourceContext {
  def apply() = SourceContext("unknown", Nil)
}

case class FieldRead(val path: String) {
  val getPath = path.split("\\.").toList
}

trait DListOpsExp extends DListOpsExpBase with DListBaseExp with FunctionsExp {
  def toAtom2[T: Manifest](d: Def[T])(implicit ctx: SourceContext): Exp[T] = super.toAtom(d)

  type Partitioner[K] = Exp[(K, Int) => Int]

  case class ShapeDep[T](s: Rep[T], fusible: Boolean) extends Def[Int]
  case class Dummy() extends Def[Unit]
  case class IteratorCollect[T](gen: Rep[Gen[T]], block: Block[Gen[T]]) extends Def[DList[T]]
  case class IteratorValue[A: Manifest, Coll[_]](l: Rep[Coll[A]], v: Rep[Int]) extends Def[A] {
    val mA = manifest[A]
  }
  case class ForeachElem[T](y: Block[Gen[T]]) extends Def[Gen[T]]

  override def syms(e: Any): List[Sym[Any]] = e match {
    case IteratorCollect(g, y) => syms(y)
    case _ => super.syms(e)
  }

  override def symsFreq(e: Any) = e match {
    case IteratorCollect(g, y) => freqNormal(y)
    case _ => super.symsFreq(e)
  }

  override def boundSyms(e: Any): List[Sym[Any]] = e match {
    case IteratorCollect(g, y) => effectSyms(y)
    case _ => super.boundSyms(e)
  }

  trait DListNode {
    val directFieldReads = mutable.HashSet[FieldRead]()
    val successorFieldReads = mutable.HashSet[FieldRead]()
    val metaInfos = mutable.Map[String, Any]()
  }

  trait ClosureNode[A, B] extends DListNode {
    val in: Exp[DList[_]]
    def closure: Exp[A => B]
    def getClosureTypes: (Manifest[A], Manifest[B])
  }

  trait Closure2Node[A, B, C] extends DListNode {
    val in: Exp[DList[_]]
    def closure: Exp[(A, B) => C]
    def getClosureTypes: ((Manifest[A], Manifest[B]), Manifest[C])
  }

  trait ComputationNode extends DListNode {
    def getTypes: (Manifest[_], Manifest[_])
    def getElementTypes: (Manifest[_], Manifest[_]) = (getTypes._1.typeArguments(0), getTypes._2.typeArguments(0))
  }

  trait EndNode[A] extends ComputationNodeTyped[A, Nothing] {
    override def getTypes = (getInType, manifest[Nothing])
    def getInType: Manifest[A]
    override def getElementTypes: (Manifest[_], Manifest[_]) = (getTypes._1.typeArguments(0), manifest[Nothing])
  }

  trait ComputationNodeTyped[A, B] extends ComputationNode {
    override def getTypes: (Manifest[A], Manifest[B])
  }

  trait PreservingTypeComputation[A] extends ComputationNodeTyped[A, A] {
    def getType: Manifest[A]
    def getTypes = (getType, getType)
  }

  case class NewDList[A: Manifest](file: Exp[String]) extends Def[DList[String]]
      with ComputationNodeTyped[Nothing, DList[A]] {
    val mA = manifest[A]
    def getTypes = (manifest[Nothing], manifest[DList[A]])
  }

  def makeDListManifest[B: Manifest] = manifest[DList[B]]

  case class DListMap[A: Manifest, B: Manifest](in: Exp[DList[A]], closure: Exp[A => B])
      extends Def[DList[B]] with ComputationNodeTyped[DList[A], DList[B]] with ClosureNode[A, B] {
    val mA = manifest[A]
    val mB = manifest[B]
    def getClosureTypes = (mA, mB)
    def getTypes = (makeDListManifest[A], makeDListManifest[B])
  }

  case class DListFilter[A: Manifest](in: Exp[DList[A]], closure: Exp[A => Boolean])
      extends Def[DList[A]] with PreservingTypeComputation[DList[A]] with ClosureNode[A, Boolean] {
    val mA = manifest[A]
    def getClosureTypes = (mA, Manifest.Boolean)
    def getType = makeDListManifest[A]
  }

  case class DListFlatMap[A: Manifest, B: Manifest](in: Exp[DList[A]], closure: Exp[A => Iterable[B]])
      extends Def[DList[B]] with ComputationNodeTyped[DList[A], DList[B]] with ClosureNode[A, Iterable[B]] {
    val mA = manifest[A]
    val mB = manifest[B]
    def getTypes = (manifest[DList[A]], manifest[DList[B]])
    def getClosureTypes = (manifest[A], manifest[Iterable[B]])
  }

  case class DListFlatten[A: Manifest](dlists: List[Exp[DList[A]]]) extends Def[DList[A]]
      with PreservingTypeComputation[DList[A]] {
    val mA = manifest[A]
    def getType = manifest[DList[A]]
  }

  case class DListGroupByKey[K: Manifest, V: Manifest](dlist: Exp[DList[(K, V)]], splits: Exp[Int], partitioner: Option[Partitioner[K]]) extends Def[DList[(K, Iterable[V])]]
      with ComputationNodeTyped[DList[(K, V)], DList[(K, Iterable[V])]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    val mOutType = manifest[(K, Iterable[V])]
    val mInType = manifest[(K, V)]
    def getTypes = (manifest[DList[(K, V)]], manifest[DList[(K, Iterable[V])]])
  }

  case class DListReduce[K: Manifest, V: Manifest](in: Exp[DList[(K, Iterable[V])]], closure: Exp[(V, V) => V])
      extends Def[DList[(K, V)]] with Closure2Node[V, V, V]
      with ComputationNodeTyped[DList[(K, Iterable[V])], DList[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getTypes = (manifest[DList[(K, Iterable[V])]], manifest[DList[(K, V)]])
  }

  case class DListJoin[K: Manifest, V1: Manifest, V2: Manifest](left: Exp[DList[(K, V1)]], right: Exp[DList[(K, V2)]], splits: Exp[Int])
      extends Def[DList[(K, (V1, V2))]] with DListNode {
    def mK = manifest[K]
    def mV1 = manifest[V1]
    def mV2 = manifest[V2]
    def mIn1 = manifest[(K, V1)]
  }

  case class DListCogroup[K: Manifest, V1: Manifest, V2: Manifest](left: Exp[DList[(K, V1)]], right: Exp[DList[(K, V2)]])
      extends Def[DList[(K, (Iterable[V1], Iterable[V2]))]] with DListNode {
    def mK = manifest[K]
    def mV1 = manifest[V1]
    def mV2 = manifest[V2]
    def mIn1 = manifest[(K, V1)]
  }

  case class DListSave[A: Manifest](dlist: Exp[DList[A]], path: Exp[String]) extends Def[Unit]
      with EndNode[DList[A]] {
    val mA = manifest[A]
    def getInType = manifest[DList[A]]
  }

  case class DListMaterialize[A: Manifest](dlist: Exp[DList[A]]) extends Def[Iterable[A]]
      with EndNode[DList[A]] {
    val mA = manifest[A]
    def getInType = manifest[DList[A]]
  }

  case class DListTakeSample[A: Manifest](dlist: Exp[DList[A]], fraction: Exp[Double], seed: Option[Exp[Int]]) extends Def[DList[A]]
      with PreservingTypeComputation[DList[A]] {
    val mA = manifest[A]
    def getType = makeDListManifest[A]
  }

  case class GetArgs() extends Def[Array[String]]

  override def get_args() = GetArgs()
  override def dlist_new[A: Manifest](file: Exp[String]) = NewDList[A](file)
  override def dlist_map[A: Manifest, B: Manifest](dlist: Exp[DList[A]], f: Exp[A] => Exp[B]) = DListMap[A, B](dlist, doLambda(f))
  override def dlist_flatMap[A: Manifest, B: Manifest](dlist: Rep[DList[A]], f: Rep[A] => Rep[Iterable[B]]) = DListFlatMap(dlist, doLambda(f))
  override def dlist_filter[A: Manifest](dlist: Rep[DList[A]], f: Exp[A] => Exp[Boolean]) = DListFilter(dlist, doLambda(f))
  override def dlist_save[A: Manifest](dlist: Exp[DList[A]], file: Exp[String]) = {
    val save = new DListSave[A](dlist, file)
    reflectEffect(save)
  }
  override def dlist_++[A: Manifest](dlist1: Rep[DList[A]], dlist2: Rep[DList[A]]) = DListFlatten(immutable.List(dlist1, dlist2))
  override def dlist_reduce[K: Manifest, V: Manifest](dlist: Exp[DList[(K, Iterable[V])]], f: (Exp[V], Exp[V]) => Exp[V]) = DListReduce(dlist, doLambda2(f))
  override def dlist_join[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[DList[(K, V1)]], right: Rep[DList[(K, V2)]], splits: Exp[Int]): Rep[DList[(K, (V1, V2))]] = DListJoin(left, right, splits)
  override def dlist_cogroup[K: Manifest, V1: Manifest, V2: Manifest](left: Rep[DList[(K, V1)]], right: Rep[DList[(K, V2)]]) = DListCogroup(left, right)
  override def dlist_groupByKey[K: Manifest, V: Manifest](dlist: Exp[DList[(K, V)]], splits: Exp[Int], partitioner: PartitionerUser[K]) =
    DListGroupByKey(dlist, splits, if (partitioner == null) None else Some(doLambda2(partitioner)))
  override def dlist_partitionBy[K: Manifest, V: Manifest](dlist: Rep[DList[(K, V)]], partitioner: PartitionerUser[K]) =
    DListFlatMap(DListGroupByKey(dlist, unit(-2), Some(doLambda2(partitioner))),
      doLambda({ in: Exp[(K, Iterable[V])] =>
        in._2.toArray.map(x => (in._1, x)).toSeq
      }))
  override def dlist_materialize[A: Manifest](dlist: Rep[DList[A]]) = DListMaterialize(dlist)
  override def dlist_takeSample[A: Manifest](dlist: Exp[DList[A]], fraction: Exp[Double], seed: Option[Exp[Int]]) = DListTakeSample(dlist, fraction, seed)

  def copyMetaInfo(from: Any, to: Any) = {
    def copyMetaInfoHere[A <: DListNode](from: DListNode, to: A) = { to.metaInfos ++= from.metaInfos; to }
    (from, to) match {
      case (x: DListNode, y: DListNode) => copyMetaInfoHere(x, y)
      case (x: DListNode, Def(y: DListNode)) => copyMetaInfoHere(x, y)
      case _ =>
    }
  }

  override def mirrorFatDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = (e match {
    case IteratorCollect(g, y) => IteratorCollect(f(g), if (f.hasContext) reifyEffectsHere(f.reflectBlock(y)) else f(y))
    case ForeachElem(y) => ForeachElem(if (f.hasContext) reifyEffectsHere(f.reflectBlock(y)) else f(y))
    case _ => super.mirrorFatDef(e, f)
  }).asInstanceOf[Def[A]]

  override def mirror[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Exp[A] = (e match {
    case d @ IteratorValue(in, i) =>
      toAtom(IteratorValue(f(in), f(i))(d.mA))(mtype(manifest[A]), implicitly[SourceContext])
    case ForeachElem(y) =>
      toAtom(ForeachElem(if (f.hasContext) reifyEffectsHere(f.reflectBlock(y)) else f(y)))(mtype(manifest[A]), implicitly[SourceContext])
    case SimpleLoop(s, i, IteratorCollect(g, y)) if f.hasContext =>
      // Here we have a need for specific order of mirroring due to generator g which is duplicated in IteratorCollect
      val ns = f(s)
      val ni = f(i).asInstanceOf[Sym[Int]]
      val nb = reifyEffectsHere(f.reflectBlock(y))
      val ng = f(g)
      toAtom(SimpleLoop(ns, ni, IteratorCollect(ng, nb)))(mtype(manifest[A]), implicitly[SourceContext])

    case _ => super.mirror(e, f)
  }).asInstanceOf[Exp[A]]

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = {
    var out = e match {
      case GetArgs() => GetArgs()
      case d @ NewDList(path) => NewDList(f(path))(d.mA)
      case d @ DListMap(dlist, func) => DListMap(f(dlist), f(func))(d.mA, d.mB)
      case d @ DListFilter(dlist, func) => DListFilter(f(dlist), f(func))(d.mA)
      case d @ DListFlatMap(dlist, func) => DListFlatMap(f(dlist), f(func))(d.mA, d.mB)
      case d @ DListSave(dlist, path) => DListSave(f(dlist), f(path))(d.mA)
      case d @ DListJoin(left, right, splits) => DListJoin(f(left), f(right), f(splits))(d.mK, d.mV1, d.mV2)
      case d @ DListCogroup(left, right) => DListCogroup(f(left), f(right))(d.mK, d.mV1, d.mV2)
      case d @ DListReduce(dlist, func) => DListReduce(f(dlist), f(func))(d.mKey, d.mValue)
      case d @ DListFlatten(dlists) => DListFlatten(f(dlists))(d.mA)
      case d @ DListGroupByKey(dlist, splits, part) => DListGroupByKey(f(dlist), f(splits), part.map(x => f(x)))(d.mKey, d.mValue)
      case d @ DListMaterialize(dlist) => DListMaterialize(f(dlist))(d.mA)
      case d @ DListTakeSample(dlist, fraction, seedOption) => DListTakeSample(f(dlist), f(fraction), seedOption.map(x => f(x)))(d.mA)
      case d @ IteratorCollect(g, y) => IteratorCollect(f(g), if (f.hasContext) reifyEffectsHere(f.reflectBlock(y)) else f(y))
      case d @ ForeachElem(y) => ForeachElem(if (f.hasContext) reifyEffectsHere(f.reflectBlock(y)) else f(y))
      case d @ IteratorValue(in, i) => IteratorValue(f(in), f(i))(d.mA)
      case d @ ShapeDep(in, fus) => ShapeDep(f(in), fus)
      case _ => super.mirrorDef(e, f)
    }
    copyMetaInfo(e, out)
    out.asInstanceOf[Def[A]]
  }

}

trait DListImplOps extends DListOps with FunctionsExp {

}

/*
 * Because of some circular inheritance. It has something to do code analysis and the DList.
 */
trait AbstractScalaGenDList extends ScalaGenBase with DListBaseCodeGenPkg {
  val IR: DListOpsExp
  import IR.{ TP, Stm, SimpleStruct, Def, Sym, Exp, Block, StructTag, ClassTag }
  import IR.{ findDefinition, syms, infix_rhs }
  class BlockVisitor(block: Block[_]) {
/*
 * Syms of a Block.
 */
    def visitAll(inputSym: Exp[Any]): List[Stm] = {
      def getInputs(x: Exp[Any]) = x match {
        case x: Sym[_] =>
          findDefinition(x) match {
            case Some(x) => syms(infix_rhs(x))
            case None => Nil
          }
        case _ => Nil
      }
      val toVisit = mutable.Set[Sym[_]]()
      toVisit += inputSym.asInstanceOf[Sym[_]]
      val out = mutable.Buffer[Stm]()
      while (!toVisit.isEmpty) {
        val nextSym = toVisit.head
        toVisit.remove(nextSym)
        out ++= findDefinition(nextSym)
        toVisit ++= getInputs(nextSym)
      }
      out.toList.distinct.reverse
    }

    lazy val statements = visitAll(block.res)
    lazy val defs = statements.flatMap(_.defs)
  }

  trait PartInfo[A] {
    def m: Manifest[A]
    def niceName: String
  }
  // Fields inside a type.
  case class FieldInfo[A: Manifest](val name: String, val niceType: String, position: Int) extends PartInfo[A] {
    val m = manifest[A]
    def niceName = niceType
    //      lazy val containingType = typeInfos2.map(_._2).filter(_.fields.size > position).find(_.fields(position) == this).get
    def getType = typeHandler.typeInfos2(niceType)
  }
  
  // Type of a Struct. Why is this not in LMS?
  case class TypeInfo[A: Manifest](val name: String, val fields: List[FieldInfo[_]]) extends PartInfo[A] {
    val m = manifest[A]
    def getField(field: String) = fields.find(_.name == field)
    def niceName = name
  }
  
  // This is needed for field analysis. You also need this for object creations.
  def isStruct(m: Manifest[_]) = typeHandler.remappings.contains(m.asInstanceOf[Manifest[Any]])

  class TypeHandler(block: Block[_]) extends BlockVisitor(block) {
    val objectCreations = statements.flatMap {
      case TP(_, s @ SimpleStruct(tag, elems)) => Some(s)
      //case TTP(_, ThinDef(s @ SimpleStruct(tag, elems))) => Some(s)
      case _ => None
    }

    def getNameForTag(t: StructTag[_]) = t match {
      case ClassTag(n) => n
      case _ => throw new RuntimeException("Add name for this tag type")
    }

    val remappings = objectCreations.map {
      s =>
        (s.m, getNameForTag(s.tag))
    }.toMap
    def cleanUpType(m: Manifest[_]) = {
      var out = m.toString
      remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))
      out
    }
    // Phi's do not have the correct type.
    //    def getType(s: Exp[_]) = s match {
    //      case Def(Phi(_, _, _, _, x)) => x.Type
    //      case x => x.Type
    //    }
    val typeInfos = objectCreations.map {
      s =>
        (s.tag, s.elems) //.mapValues(x => cleanUpType(getType(x))))
    }.toMap
    val typeInfos2 = objectCreations.map {
      s =>
        var i = -1
        val name = getNameForTag(s.tag)
        val fields = s.elems.map { x =>
          i += 1;
          val typ = x._2.tp
          new FieldInfo(x._1, cleanUpType(typ), i)(typ)
        }.toList
        (name, new TypeInfo(name, fields)(s.m))
    }.toMap

    def getTypeAt(path: String, mIn: Manifest[_]): PartInfo[_] = {
      //      println()
      //      println("#### "+path+" in "+mIn)
      val pathParts = path.split("\\.").drop(1).toList
      val m = mIn.asInstanceOf[Manifest[Any]]
      var typeNow: Any = m
      var restPath = pathParts
      val step1 = mIn match {
        // if m is a tuple:
        case x: Manifest[(_, _)] if (x.toString.startsWith("scala.Tuple2")) =>
          pathParts match {
            case Nil =>
              val f1 = new FieldInfo("_1", cleanUpType(x.typeArguments(0)), 0)(x.typeArguments(0))
              val f2 = new FieldInfo("_2", cleanUpType(x.typeArguments(1)), 1)(x.typeArguments(1))
              new TypeInfo("tuple2s", f1 :: f2 :: Nil)(x)
            case "_1" :: _ => {
              restPath = restPath.drop(1)
              new FieldInfo("_1", cleanUpType(x.typeArguments(0)), 0)(x.typeArguments(0))
            }
            case "_2" :: _ => {
              restPath = restPath.drop(1)
              new FieldInfo("_2", cleanUpType(x.typeArguments(1)), 1)(x.typeArguments(1))
            }
          }
        // if m is a normal type: just look up the type for this manifest
        case x => typeInfos2(cleanUpType(x))
      }

      //      println("Step 1"+step1+", rest is "+restPath)
      def getRest(restPath: List[String], x: PartInfo[_]): PartInfo[_] = {
        //        println("Looking up rest of the path "+restPath+" for "+x)
        restPath match {
          case Nil => x
          case field :: _ =>
            val typeInfo = x match {
              case f: FieldInfo[_] => typeInfos2(f.niceType)
              case f: TypeInfo[_] => f
            }
            getRest(restPath.drop(1), typeInfo.getField(field).get)
        }
      }

      val out = getRest(restPath, step1)
      //      println("----- Returning "+out)
      //      println()
      out

    }

  }

  var typeHandler: TypeHandler = null

  def collectionName: String

  override def remap[A](m: Manifest[A]): String = {
    val remappings = typeHandler.remappings.filter(!_._2.startsWith("tuple2s"))
    var out = super.remap[A](m)
    if (out.startsWith("ch.epfl.distributed.DList")) {
      out = out.replaceAll("ch.epfl.distributed.DList", collectionName)
    }
    remappings.foreach(x => out = out.replaceAll(Pattern.quote(x._1.toString), x._2))

    // hack for problem with constant tuples in nested tuples
    val expname = "Expressions$Exp["
    while (out.contains(expname)) {
      println("##*$*%**%%** => Remaphack in progress for " + out)
      val start = out.indexOf(expname) + expname.length
      val end = out.indexOf("]", start)
      val len = end - start
      val actualType = out.substring(start, end)
      val searchStart = out.substring(0, start)
      val removeLength = expname.length +
        searchStart.reverse.drop(expname.length)
        .takeWhile { x => x != '[' && x != ' ' }.length
      val (before, after) = out.splitAt(start)
      out = before.reverse.drop(removeLength).reverse + actualType + after.drop(actualType.length + 1)
    }
    out
  }

  /** The default parallelism to use, aka number of reducers */
  var parallelism = 40
}

trait ScalaGenDList extends AbstractScalaGenDList with Matchers with DListTransformations with DListFieldAnalysis {
  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    IteratorValue,
    ShapeDep,
    Dummy,
    NewDList,
    DListSave,
    DListMap,
    DListFilter,
    DListFlatMap,
    DListFlatten,
    DListGroupByKey,
    DListReduce,
    ComputationNode,
    DListNode,
    DListJoin,
    DListCogroup,
    DListMaterialize,
    DListTakeSample,
    GetArgs
  }
  import IR.{ SimpleStruct }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  def getProjectName: String

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = rhs match {
    case sd @ IteratorValue(r, i) => emitValDef(sym, "it.next // loop var " + quote(i))
    case sd @ ShapeDep(dep, _) => stream.println("// " + quote(dep))
    case sd @ Dummy() => stream.println("// dummy ")
    case nv @ NewDList(filename) => emitValDef(sym, "New dlist created from %s with type %s".format(filename, nv.mA))
    case vs @ DListSave(dlist, filename) => stream.println("Saving dlist %s (of type %s) to %s".format(dlist, remap(vs.mA), filename))
    case d @ DListMaterialize(dlist) => stream.println("Materializing dlist %s (of type %s)".format(dlist, remap(d.mA)))
    case d @ DListTakeSample(dlist, fraction, seedOption) => stream.println("Taking sample from dlist %s (of type %s), with fraction %s and seed %s".format(dlist, remap(d.mA), fraction, seedOption))
    case vm @ DListMap(dlist, func) => emitValDef(sym, "mapping dlist %s with function %s, type %s => %s".format(dlist, quote(func), vm.mA, vm.mB))
    case vf @ DListFilter(dlist, function) => emitValDef(sym, "filtering dlist %s with function %s".format(dlist, function))
    case vm @ DListFlatMap(dlist, function) => emitValDef(sym, "flat mapping dlist %s with function %s".format(dlist, function))
    case vm @ DListFlatten(v1) => emitValDef(sym, "flattening dlists %s".format(v1))
    case gbk @ DListGroupByKey(dlist, splits, part) => emitValDef(sym, "grouping dlist by key (" + quote(splits) + " splits), with partitioner " + part)
    case gbk @ DListJoin(left, right, _) => emitValDef(sym, "Joining %s with %s".format(left, right))
    case d @ DListCogroup(left, right) => emitValDef(sym, "cogroup %s with %s".format(left, right))
    case red @ DListReduce(dlist, f) => emitValDef(sym, "reducing dlist")
    case GetArgs() => emitValDef(sym, "getting the arguments")
    case IR.Lambda(_, _, _) if inlineClosures =>
    case IR.Lambda2(_, _, _, _) if inlineClosures =>
    case _ => super.emitNode(sym, rhs)
  }

  def makePackageName(pack: String) = if (pack == "") "" else "." + pack

  override def emitSource[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] =
    emitProgram(f, className, stream, "")

  def emitProgram[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter, pack: String)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {

    val x = fresh[A]
    val y = reifyBlock(f(x))

    typeHandler = new TypeHandler(y)

    val sA = remap(mA)
    val sB = remap(mB)

    withStream(stream) {
      stream.println("/*****************************************\n" +
        "  Emitting Generated Code                  \n" +
        "*******************************************/")

      // TODO: separate concerns, should not hard code "pxX" name scheme for static data here
      //      stream.println("class "+className+" extends (("+sA+")=>("+sB+")) {")
      //      stream.println("def apply("+quote(x)+":"+sA+"): "+sB+" = {")

      emitBlock(y)
      stream.println(quote(getBlockResult(y)))

      stream.println("/*****************************************\n" +
        "  End of Generated Code                  \n" +
        "*******************************************/")
    }

    Nil
  }

  def writeClosure(closure: Exp[_]) = {
    val sw = new StringWriter()
    val pw = new PrintWriter(sw)
    def remapHere(x: Manifest[_]) = if (typesInInlinedClosures) ": " + remap(x) else ""
    withStream(pw) {
      closure match {
        case Def(Lambda(fun, x, y)) => {
          pw.println("{ %s %s => ".format(quote(x), remapHere(x.tp)))
          emitBlock(y)
          pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.tp)))
          pw.print("}")
        }
        case Def(Lambda2(fun, x1, x2, y)) => {
          pw.println("{ (%s %s, %s %s) => ".format(quote(x1), remapHere(x1.tp), quote(x2), remapHere(x2.tp)))
          emitBlock(y)
          pw.println("%s %s".format(quote(getBlockResult(y)), remapHere(y.tp)))
          pw.print("}")
        }
      }
    }
    pw.flush
    sw.toString
  }

  var inlineClosures = false

  def typesInInlinedClosures = true

  def handleClosure(closure: Exp[_]) = {
    if (inlineClosures) {
      writeClosure(closure)
    } else {
      quote(closure)
    }
  }

  var narrowExistingMaps = true
  var insertNarrowingMaps = true
  var mapMerge = true
  var loopFusion = true
  var inlineInLoopFusion = true

  def getParams(): List[(String, Any)] = {
    List(
      ("field reduction", narrowExistingMaps && insertNarrowingMaps),
      ("loop fusion", loopFusion),
      ("inline in loop fusion", inlineInLoopFusion),
      ("regex patterns pre compiled", !IR.disablePatterns))
  }

  def getOptimizations() = getParams().map(x => "// " + x._1 + ": " + x._2).mkString("\n")

  def markMapsToNarrow(b: Block[_]) {
    val analyzer = newFieldAnalyzer(b)
    analyzer.nodes.foreach {
      case d @ DListMap(x, f) if (!d.metaInfos.contains("narrowed"))
        && !SimpleType.unapply(d.getClosureTypes._2).isDefined
        && analyzer.hasObjectCreationInClosure(d) =>
        d.metaInfos("toNarrow") = true
      case _ =>
    }
  }

  def doNarrowExistingMaps[B: Manifest](b: Block[B]) = {
    if (narrowExistingMaps) {
      markMapsToNarrow(b)
      narrowNarrowers(b, "toNarrow")
    } else {
      b
    }
  }

  def insertNarrowersAndNarrow[B: Manifest](b: Block[B], runner: TransformationRunner) = {
    narrowNarrowers(insertNarrowers(b, runner), "narrower")
  }

  def insertNarrowers[B: Manifest](y: Block[B], runner: TransformationRunner) = {
    if (insertNarrowingMaps)
      runner.run(y)
    else
      y
  }

  def narrowNarrowers[A: Manifest](b: Block[A], tag: String) = {
    var curBlock = b
    //    emitBlock(curBlock)
    var goOn = insertNarrowingMaps
    while (goOn) {
      val fieldAnalyzer = newFieldAnalyzer(curBlock)

      val candidates = fieldAnalyzer.ordered.flatMap {
        case d @ DListMap(x, lam) if (d.metaInfos.contains(tag) &&
          !d.metaInfos.contains("narrowed") &&
          SimpleType.unapply(d.mB).isDefined) => {
          d.metaInfos("narrowed") = true
          None
        }
        case d @ DListMap(x, lam) if (d.metaInfos.contains(tag) &&
          !d.metaInfos.contains("narrowed")) =>
          Some(d)
        case _ => None
      }
      if (candidates.isEmpty) {
        goOn = false
      } else {
        fieldAnalyzer.makeFieldAnalysis
        val toTransform = candidates.head
        println("Found candidate for narrowing: " + toTransform)
        toTransform.metaInfos("narrowed") = true
        val narrowTrans = new NarrowMapsTransformation(toTransform, typeHandler)
        curBlock = narrowTrans.run(curBlock)
        narrowTrans
      }
    }
    //    emitBlock(curBlock)
    curBlock
  }

  var lastGraph: String = ""

  def prepareGraphData(block: Block[_], comments: Boolean = true) {
    lastGraph = try {
      val analyzer = newFieldAnalyzer(block)
      analyzer.makeFieldAnalysis
      analyzer.addComments = comments
      analyzer.exportToGraph
    } catch {
      case e => e.getMessage + "\n" + e.getStackTraceString
    }
  }

  def writeGraphToFile(block: Block[_], name: String, comments: Boolean = true) {
    val out = new FileOutputStream(name)
    val analyzer = newAnalyzer(block)
    out.write(analyzer.exportToGraph.getBytes)
    out.close
  }

}

trait ScalaFatLoopsFusionOpt extends DListBaseCodeGenPkg with ScalaGenIfThenElseFat with LoopFusionOpt {
  val IR: DListOpsExp with IfThenElseFatExp

  import IR.{
    collectYields,
    fresh,
    toAtom2,
    SimpleLoop,
    reifyEffects,
    ShapeDep,
    mtype,
    IteratorCollect,
    Block,
    Dummy,
    IteratorValue,
    yields,
    skip,
    doApply,
    ifThenElse,
    reflectMutableSym,
    reflectMutable,
    Reflect,
    Exp,
    Def,
    Reify,
    Gen,
    Yield,
    IfThenElse,
    Skip,
    reflectEffect,
    ForeachElem,
    summarizeEffects
  }

  override def unapplySimpleIndex(e: Def[Any]) = e match {
    case IteratorValue(a, i) => Some((a, i))
    case _ => super.unapplySimpleIndex(e)
  }
  override def unapplySimpleDomain(e: Def[Int]): Option[Exp[Any]] = e match {
    case ShapeDep(a, true) => Some(a)
    case _ => super.unapplySimpleDomain(e)
  }

  override def unapplySimpleCollect(e: Def[Any]) = e match {
    case IteratorCollect(Def(Reflect(Yield(_, a), _, _)), _) => Some(a.head)
    case _ => super.unapplySimpleCollect(e)
  }

  // take d's context (everything between loop body and yield) and duplicate it into r
  override def plugInHelper[A, T: Manifest, U: Manifest](oldGen: Exp[Gen[A]], context: Exp[Gen[T]], plug: Exp[Gen[U]]): Exp[Gen[U]] = context match {
    case `oldGen` => plug

    case Def(Reify(y, s, e)) =>
      getBlockResultFull(reifyEffects(plugInHelper(oldGen, y, plug)))

    case Def(Reflect(IfThenElse(c, Block(a), /*Block(Def(Skip(x)))), _, _))*/ Block(Def(Reify(Def(Reflect(Skip(x), _, b)), _, _)))), u, es)) =>
      // this is wrong but we need to check if it works at all
      ifThenElse(c, reifyEffects(plugInHelper(oldGen, a, plug)), reifyEffects(skip[U](reflectMutableSym(fresh[Int]), x)))

    case Def(Reflect(SimpleLoop(sh, x, ForeachElem(Block(y))), _, _)) =>
      val body = reifyEffects(plugInHelper(oldGen, y, plug))
      reflectEffect(SimpleLoop(sh, x, ForeachElem(body)), summarizeEffects(body))

    case Def(x) =>
      sys.error("Missed me => " + x + " should find " + Def.unapply(oldGen).getOrElse("None"))
  }

  override def applyPlugIntoContext(d: Def[Any], r: Def[Any]) = (d, r) match {
    case (IteratorCollect(g, Block(a)), IteratorCollect(g2, Block(b))) =>
      IteratorCollect(g2, Block(plugInHelper(g, a, b)))

    case _ => super.applyPlugIntoContext(d, r)
  }

  override def shapeEquality(s1: Exp[Int], s2: Exp[Int]) = (s1, s2) match {
    case (Def(ShapeDep(_, _)), _) => false
    case (_, Def(ShapeDep(_, _))) => false
    case _ => super.shapeEquality(s1, s2)
  }

}

