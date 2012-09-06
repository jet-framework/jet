package ch.epfl.distributed

import scala.virtualization.lms.common.ScalaGenBase
import scala.virtualization.lms.common.BooleanOps
import scala.collection.mutable
import scala.virtualization.lms.common.WorklistTransformer
import scala.virtualization.lms.common.ForwardTransformer
import scala.virtualization.lms.internal.Utils
import scala.reflect.SourceContext

trait DListTransformations extends ScalaGenBase with AbstractScalaGenDList with Matchers with DListAnalysis with Utils {

  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const }
  import IR.{
    NewDList,
    DListSave,
    DListMap,
    DListFilter,
    DListFlatMap,
    DListFlatten,
    DListGroupByKey,
    DListReduce,
    ComputationNode,
    DListNode
  }
  import IR.{ TTP, TP, SubstTransformer }
  import IR.{ findDefinition }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda }
  import IR.{ Struct }

  abstract class TransformationRunner {
    val wt = new WorklistTransformer() {
      val IR: DListTransformations.this.IR.type = DListTransformations.this.IR

      var symSubst: scala.collection.immutable.Map[Sym[Any], () => Sym[Any]] = Map.empty
      var previousSubst = new mutable.HashMap[Exp[Any], Exp[Any]]

      def registerFunction[A](x: Exp[A])(y: () => Exp[A]): Unit = if (nextSubst.contains(x.asInstanceOf[Sym[A]]))
        printdbg("discarding, already have a replacement for " + x)
      else {
        printdbg("register replacement for " + x)
        nextSubst = nextSubst + (x.asInstanceOf[Sym[A]] -> y)
      }

      override def apply[A](x: Exp[A]): Exp[A] = x match {
        case s: Sym[A] if symSubst.contains(s) =>
          symSubst(s)().asInstanceOf[Sym[A]]
        case _ =>
          super.apply(x)
      }

      override def runOnce[A: Manifest](s: Block[A]): Block[A] = {
        val res = super.runOnce(s)
        symSubst = Map.empty
        res
      }

    }

    def run[T: Manifest](y: Block[T]): Block[T] = {
      registerTransformations(newAnalyzer(y))
      if (wt.nextSubst.isEmpty) {
        y
      } else {
        println("Running")
        wt.run(y)
      }
    }
    def registerTransformations(analyzer: Analyzer)

    def getConsumers(analyzer: Analyzer, x: Sym[_]) = {
      analyzer.statements.filter(y => IR.syms(y).contains(x))
    }
  }

  class NarrowerInsertionTransformation extends TransformationRunner {
    import wt.IR._

    def isNarrower(x: Exp[Any]) = x match {
      case Def(d @ DListMap(_, _)) if d.metaInfos.contains("narrower")
        || d.metaInfos.contains("toNarrow") =>
        true
      case _ => false
    }
    def makeNarrower[T: Manifest](in: Exp[DList[T]]) = {
      if (isNarrower(in))
        in
      else {
        val narrower = dlist_map(wt(in), { x: Rep[T] => x })
        findDefinition(narrower.asInstanceOf[Sym[_]]).get.defs
          .head.asInstanceOf[DListNode].metaInfos("narrower") = true
        narrower
      }
    }
    def registerTransformations(analyzer: Analyzer) {
      analyzer.narrowBeforeCandidates.foreach {
        case gbk @ DListGroupByKey(x, splits, part) =>
          val stm = findDefinition(gbk).get
          class GroupByKeyTransformer[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]], splits: Exp[Int], part: Option[Partitioner[K]]) {
            val mapNew = makeNarrower(in)
            val gbkNew = toAtom2(new DListGroupByKey(mapNew, wt(splits), part.map(x => wt(x)))(manifest[K], manifest[V]))(mtype(stm.syms.head.tp), FakeSourceContext())
            //val gbkNew = dlist_groupByKey(mapNew, part.getOrElse(null))
            wt.register(stm.syms.head)(gbkNew)
          }
          new GroupByKeyTransformer(gbk.dlist, splits, part)(gbk.mKey, gbk.mValue)

        case j @ DListJoin(l, r, splits) =>
          val stm = findDefinition(j).get
          class DListJoinTransformer[K: Manifest, V1: Manifest, V2: Manifest](left: Exp[DList[(K, V1)]], right: Exp[DList[(K, V2)]], splits: Exp[Int]) {
            val mapNewLeft = makeNarrower(left)
            val mapNewRight = makeNarrower(right)

            val joinNew = dlist_join(mapNewLeft, mapNewRight, splits)
            wt.register(stm.syms.head)(joinNew)
          }
          new DListJoinTransformer(l, r, splits)(j.mK, j.mV1, j.mV2)
        case _ =>
      }
    }

  }

  class NarrowMapsTransformation(target: IR.Lambda[_, _], fieldReads: List[FieldRead], typeHandler: TypeHandler) extends TransformationRunner {
    def this(target: DListNode with ClosureNode[_, _], typeHandler: TypeHandler) = this(
      target.closure match {
        case Def(l @ IR.Lambda(_, _, _)) => l
      }, target.successorFieldReads.toList, typeHandler)

    def registerTransformations(analyzer: Analyzer) {
      val targetLambda = analyzer.statements.filter(_.defs.contains(target)).head.syms.head

      val targetSym = target.y.res

      val fields = fieldReads.map(_.path)

      case class Node(val path: String, val children: mutable.Map[String, Node] = mutable.HashMap()) {
        def resolve(pathToChild: String): Option[Node] = {
          val newS = pathToChild.drop(5)
          val arg = if (newS.size >= 1) newS.drop(1) else ""
          resolveInternal(arg)
        }
        private def resolveInternal(pathToChild: String): Option[Node] = {
          if (!pathToChild.isEmpty) {
            val parts = (pathToChild.split("\\.", 2).toList ++ List("")).take(2)
            if (children.contains(parts.head)) {
              children(parts.head).resolveInternal(parts.last)
            } else {
              None
            }
          } else {
            Some(this)
          }
        }
      }
      val out = new Node("input")

      for (x <- fields) {
        var curNode = out
        for (y <- x.split("\\.").drop(1)) {
          if (!curNode.children.contains(y)) {
            val newNode = Node(y)
            curNode.children(y) = newNode
          }
          curNode = curNode.children(y)
        }
      }

      def build[C](path: String, readFromSym: Exp[C]): Exp[C] = {
        val node = out.resolve(path).get
        val typeInfo = typeHandler.getTypeAt(path, target.y.res.tp)
        printdbg("Typeinfo for path " + path + " is " + typeInfo)
        typeInfo match {
          case ti @ TypeInfo(name, fields) => {
            val elems = for ((childName, node) <- node.children)
              yield (childName, build(path + "." + childName, readFromSym));
            printdbg("Building new Struct with name " + name + " and elems " + elems + " for type " + ti)
            IR.toAtom2(IR.SimpleStruct(IR.ClassTag(name), elems.toMap)(ti.m))(ti.m, FakeSourceContext())
          }
          case fi @ FieldInfo(name, niceType, position) => {
            val newSym = IR.field(readFromSym, name)(fi.m, FakeSourceContext())
            //              val newSym = IR.toAtom2(IR.Field(readFromSym, name, fi.m))(fi.m, FakeSourceContext())
            if (node.children.isEmpty) {
              newSym
            } else {
              val elems = for ((childName, node) <- node.children)
                yield (childName, build(path + "." + childName, newSym));
              val typ = fi.getType
              printdbg("Building new Struct with name " + niceType + " and elems " + elems + " for field " + fi)
              IR.toAtom2(IR.SimpleStruct(IR.ClassTag(niceType), elems.toMap)(typ.m))(typ.m, FakeSourceContext())
            }

          }
        }
      }.asInstanceOf[Exp[C]]
      def h = wt.IR.mtype _
      class LambdaConstructor[A: Manifest, B: Manifest](target: Lambda[A, B]) {
        lazy val newResult = build("input", wt(targetSym))
        lazy val newLam = Lambda(target.f, target.x, wt.IR.Block(wt(newResult)))(wt.IR.mtype(target.mA), wt.IR.mtype(target.mB)).asInstanceOf[Lambda[A, B]]
        lazy val newLamAtom = IR.toAtom2(newLam)(target.m, FakeSourceContext())
      }
      val lc = new LambdaConstructor(target)(wt.IR.mtype(target.mA), wt.IR.mtype(target.mB))
      wt.register(targetLambda)(lc.newLamAtom)
    }
  }

  class MonadicToLoopsTransformation extends TransformationRunner {
    import wt.IR.{
      collectYields,
      fresh,
      toAtom2,
      SimpleLoop,
      reifyEffects,
      ShapeDep,
      mtype,
      IteratorCollect,
      ForeachElem,
      Block,
      IteratorValue,
      yields,
      skip,
      doApply,
      ifThenElse,
      reflectMutableSym,
      reflectMutable,
      reflectEffect,
      summarizeEffects,
      Summary,
      Reflect,
      Exp,
      Gen
    }

    def monadicOp(a: Any) = a match {
      case SomeDef(_: (DListFilter[_])) | SomeDef(_: DListMap[_, _]) | SomeDef(_: DListFlatMap[_, _]) => true
      case _ => true
    }

    def fusible(r: Exp[Any], analyzer: Analyzer) = r match {
      case SomeDef(x: DListNode) if analyzer.nodeSuccessors(x).size <= 1 => true
      case _ => false
    }

    def registerTransformations(analyzer: Analyzer) {
      analyzer.nodes.foreach {
        case m @ DListFilter(r, lm @ Def(Lambda(f, in, bl))) if monadicOp(r) || getConsumers(analyzer, findDefinition(m).get.syms.head).forall(monadicOp) =>
          val stm = findDefinition(m).get
          wt.register(stm.syms.head) {
            val i = fresh[Int]
            val d = reflectMutableSym(fresh[Int])
            val value = toAtom2(IteratorValue(wt(r), i))(mtype(r.tp), FakeSourceContext())
            val (g, y) = collectYields {
              reifyEffects {
                // Yield the iterator value in the block
                ifThenElse(doApply(wt(lm), value), reifyEffects { yields(d, List(i), value)(mtype(r.tp.typeArguments(0))) }, reifyEffects { skip(d, List(i)) })
              }
            }
            // create a loop with the body that inlines the filtering function
            val loop = SimpleLoop(toAtom2(ShapeDep(wt(r), fusible(r, analyzer))), i, IteratorCollect(g, y))

            // make an stm out of the loop
            toAtom2(loop)(mtype(stm.syms.head.tp), FakeSourceContext())
          }
          System.out.println("Registering " + stm + " to a filter loop")
        case m @ DListMap(r, lm @ Def(lmdef @ Lambda(f, v, bl))) if monadicOp(r) || getConsumers(analyzer, findDefinition(m).get.syms.head).forall(monadicOp) =>
          println(findDefinition(m).get.syms.head)
          val stm = findDefinition(m).get

          val eval = () => {
            val i = fresh[Int]
            val d = reflectMutableSym(fresh[Int])

            val yld = doApply(wt(lm), toAtom2(IteratorValue(wt(r), i))(mtype(r.tp), FakeSourceContext()))
            val (g, y) = collectYields {
              reifyEffects {
                yields(d, List(i), yld)(lmdef.mB)
              }
            }

            println("Generator type= " + stripGen(g.tp))
            // create a loop with body that inlines the function
            val loop = SimpleLoop(toAtom2(ShapeDep(wt(r), fusible(r, analyzer))), i, IteratorCollect(g, y))

            // make an stm out of the loop
            toAtom2(loop)(mtype(stm.syms.head.tp), FakeSourceContext())
          }
          System.out.println("Registering " + stm + " to a map loop")
          wt.registerFunction(stm.syms.head)(eval)
        case m @ DListFlatMap(r, lm @ Def(lmdef @ Lambda(f, in, bl))) if monadicOp(r) || getConsumers(analyzer, findDefinition(m).get.syms.head).forall(monadicOp) =>
          val stm = findDefinition(m).get

          val eval = () => {
            val i = fresh[Int]
            val d = reflectMutableSym(fresh[Int])

            val (g, y) = collectYields {
              reifyEffects {
                val coll = doApply(wt(lm), toAtom2(IteratorValue(wt(r), i))(mtype(r.tp), FakeSourceContext()))
                val shape2 = toAtom2(ShapeDep(coll, true))
                val j = fresh[Int]

                // Somehow the mirroring order here is wrong
                val innerBody = reifyEffects { yields(d, List(j, i), toAtom2(IteratorValue(coll, j))(mtype(coll.tp.typeArguments(0)), FakeSourceContext()))(lmdef.mB.typeArguments.head.asInstanceOf[Manifest[Any]]) }
                reflectEffect(SimpleLoop(shape2, j, ForeachElem(innerBody).asInstanceOf[Def[Gen[Any]]]), summarizeEffects(innerBody))
              }
            }

            println("Generator type= " + stripGen(g.tp))
            // create a loop with body that inlines the function
            val loop = SimpleLoop(toAtom2(ShapeDep(wt(r), fusible(r, analyzer))), i, IteratorCollect(g, y))

            // make an stm out of the loop
            toAtom2(loop)(mtype(stm.syms.head.tp), FakeSourceContext())
          }
          System.out.println("Registering " + stm + " to a flatmap loop")
          wt.registerFunction(stm.syms.head)(eval)
        case _ =>
      }
    }
  }

  /**
   * Should inline occurences of Apply(Lambda(f, v, b), value) to produce just the block b with input simbol rewired to value.
   */
  class InlineTransformation extends TransformationRunner {
    import wt.IR.{
      toAtom2,
      Block,
      IteratorValue,
      Apply,
      Exp
    }

    override def run[T: Manifest](y: IR.Block[T]): IR.Block[T] = {
      // due to correctness issues this transformer runs one inlining at a time
      registerTransformations(newAnalyzer(y))
      if (wt.nextSubst.isEmpty)
        y
      else {
        run(runOne(y))
      }

    }

    private def runOne[T: Manifest](y: IR.Block[T]) = {
      val blocksInlined = wt.run(y)
      wt.runOnce(blocksInlined)
    }

    def registerTransformations(analyzer: Analyzer) {
      // selects the first candidate to inline
      analyzer.orderedStatements.collectFirst {
        case s @ SomeDef(m @ IR.Apply(lm @ Def(Lambda(f, in, bl @ Block(inBl))), vl @ Def(value @ IteratorValue(a, b)))) =>
          // the whole apply with the body of the lambda (parameters are now hanging)
          wt.registerFunction(s.syms.head) { () =>
            wt.reflectBlock(bl)
          }

          // substitute the dangling parameter symbols with the value in the next run
          wt.symSubst += in -> (() => (toAtom2(IteratorValue(wt(a), wt(b)))(in.tp, FakeSourceContext())).asInstanceOf[Sym[Int]])
      }
    }
  }

  /*  

  class MergeFlattenTransformation extends Transformation {
    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(DListFlatten(list)) => {
        list.find { case Def(DListFlatten(list2)) => true case _ => false }.isDefined
      }
      case _ => false
      
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = {
      inExp match {
        case Def(lower @ DListFlatten(list)) =>
          val flat2 = list.find { case Def(DListFlatten(list2)) => true case _ => false }.get
          flat2 match {
            case d @ Def(upper @ DListFlatten(list2)) =>
              val out = new DListFlatten(list.filterNot(_ == d) ++ list2)
              var newDefs = List(out)
              val ttps = newDefs.map(IR.findOrCreateDefinition(_)).map(fatten)
              return (ttps, List((inExp, IR.findOrCreateDefinition(out).sym), (d, IR.findOrCreateDefinition(out).sym)))
          }
      }
      throw new RuntimeException("Bug in merge flatten")
    }

  }

  class SinkFlattenTransformation extends SimpleTransformation {
    def doTransformationPure(inExp: Exp[_]) = inExp match {
      case Def(vm @ DListMap(Def(vf @ DListFlatten(list)), func)) => {
        val mappers = list.map { x =>
          val mapper = new DListMap(x, func)
          val newDef = IR.toAtom2(mapper)
          newDef
        }
        new DListFlatten(mappers)
      }
      case _ => null
    }

  }

  class FieldOnStructReadTransformation extends Transformation {

    def appliesToNode(inExp: Exp[_], t: Transformer): Boolean = inExp match {
      case Def(t @ IR.Field(Def(IR.SimpleStruct(_, elems)), name, typ)) => true
      case _ => false
    }

    override def applyToNode(inExp: Exp[_], transformer: Transformer): (List[TTP], List[(Exp[_], Exp[_])]) = inExp match {
      case d @ Def(t @ IR.Field(Def(IR.SimpleStruct(_, elems)), name, typ)) => {
        val outDef = elems.get(name).get
        (Nil, List((d, outDef)))
      }
      case _ => throw new RuntimeException("should not be called if appliesToNode returns false")
    }
  }

*/

}
