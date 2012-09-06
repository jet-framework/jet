package ch.epfl.distributed

import scala.virtualization.lms.common.{ ScalaGenBase, LoopsExp, LoopsFatExp, BaseGenLoops, ScalaGenLoops, ScalaGenLoopsFat }
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter

trait SparkProgram extends DListOpsExp with DListImplOps with SparkDListOpsExp

trait SparkDListOps extends DListOps {
  implicit def repDListToSparkDListOps[A: Manifest](dlist: Rep[DList[A]]) = new dlistSparkOpsCls(dlist)
  implicit def varDListToSparkDListOps[A: Manifest](dlist: Var[DList[A]]) = new dlistSparkOpsCls(readVar(dlist))
  class dlistSparkOpsCls[A: Manifest](dlist: Rep[DList[A]]) {
    def cache() = dlist_cache(dlist)
    def takeSample(withReplacement: Rep[Boolean], num: Rep[Int], seed: Rep[Int]) = dlist_takeSample(dlist, withReplacement, num, seed)
  }

  //  implicit def repDListToDListIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]]) = new dlistIterableTupleOpsCls(x)
  //  implicit def varDListToDListIterableTupleOpsCls[K: Manifest, V: Manifest](x: Var[DList[(K, V)]]) = new dlistIterableTupleOpsCls(readVar(x))
  //  class dlistIterableTupleOpsCls[K: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) {
  //    def reduceByKey(f: (Rep[V], Rep[V]) => Rep[V]) = dlist_reduceByKey[K, V](x, f)
  //  }
  implicit def repDListToSparkTupleOpsCls[K <% Ordered[K]: Manifest, V: Manifest](x: Rep[DList[(K, V)]]) = new dlistSparkTupleOpsCls(x)
  implicit def varDListToSparkTupleOpsCls[K <% Ordered[K]: Manifest, V: Manifest](x: Var[DList[(K, V)]]) = new dlistSparkTupleOpsCls(readVar(x))
  class dlistSparkTupleOpsCls[K: Manifest, V: Manifest](dlist: Rep[DList[(K, V)]]) {
    def sortByKey(ascending: Rep[Boolean]) = dlist_sortByKey(dlist, ascending)
  }

  def dlist_cache[A: Manifest](dlist: Rep[DList[A]]): Rep[DList[A]]
  def dlist_takeSample[A: Manifest](dlist: Rep[DList[A]], withReplacement: Rep[Boolean], num: Rep[Int], seed: Rep[Int]): Rep[Iterable[A]]

  def dlist_sortByKey[K: Manifest, V: Manifest](dlist: Rep[DList[(K, V)]], ascending: Rep[Boolean]): Rep[DList[(K, V)]]
}

trait SparkDListOpsExp extends DListOpsExp with SparkDListOps {
  case class DListReduceByKey[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]], closure: Exp[(V, V) => V], splits: Exp[Int], partitioner: Option[Partitioner[K]])
      extends Def[DList[(K, V)]] with Closure2Node[V, V, V]
      with PreservingTypeComputation[DList[(K, V)]] {
    val mKey = manifest[K]
    val mValue = manifest[V]
    def getClosureTypes = ((manifest[V], manifest[V]), manifest[V])
    def getType = manifest[DList[(K, V)]]
  }

  case class DListCache[A: Manifest](in: Exp[DList[A]]) extends Def[DList[A]] with PreservingTypeComputation[DList[A]] {
    val mA = manifest[A]
    def getType = manifest[DList[A]]
  }

  case class DListTakeSampleNum[A: Manifest](dlist: Exp[DList[A]], withReplacement: Exp[Boolean], num: Exp[Int], seed: Exp[Int]) extends Def[Iterable[A]] with EndNode[DList[A]] {
    val mA = manifest[A]
    def getInType = manifest[DList[A]]
  }

  case class DListSortByKey[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]], ascending: Exp[Boolean]) extends Def[DList[(K, V)]]
      with PreservingTypeComputation[DList[(K, V)]] {
    val mK = manifest[K]
    val mV = manifest[V]
    def getType = manifest[DList[(K, V)]]
  }

  override def dlist_cache[A: Manifest](in: Rep[DList[A]]) = DListCache[A](in)

  override def dlist_takeSample[A: Manifest](dlist: Exp[DList[A]], withReplacement: Exp[Boolean], num: Exp[Int], seed: Exp[Int]) = DListTakeSampleNum(dlist, withReplacement, num, seed)

  override def dlist_sortByKey[K: Manifest, V: Manifest](dlist: Exp[DList[(K, V)]], ascending: Exp[Boolean]) = DListSortByKey(dlist, ascending)

  override def mirrorDef[A: Manifest](e: Def[A], f: Transformer)(implicit pos: SourceContext): Def[A] = {
    val out = (e match {
      case v @ DListReduceByKey(dlist, func, splits, part) => DListReduceByKey(f(dlist), f(func), f(splits), part.map(x => f(x)))(v.mKey, v.mValue)
      case v @ DListCache(in) => DListCache(f(in))(v.mA)
      case v @ DListTakeSampleNum(in, r, n, s) => DListTakeSampleNum(f(in), f(r), f(n), f(s))(v.mA)
      case v @ DListSortByKey(in, asc) => DListSortByKey(f(in), f(asc))(v.mK, v.mV)
      case _ => super.mirrorDef(e, f)
    })
    copyMetaInfo(e, out)
    out.asInstanceOf[Def[A]]
  }

}

trait SparkTransformations extends DListTransformations {
  val IR: DListOpsExp with SparkDListOpsExp
  import IR.{ DListReduceByKey, DListReduce, DListGroupByKey, DListMap }
  import IR.{ Def, Exp }

  class ReduceByKeyTransformation extends TransformationRunner {
    import wt.IR._
    def registerTransformations(analyzer: Analyzer) {
      System.out.println("running ReduceByKeyTransformation")
      val reduces = analyzer.nodes.flatMap {
        case d @ DListReduce(r, f) => Some(d)
        case _ => None
      }

      System.out.println("Found reduces " + reduces)
      reduces.foreach {
        case d @ DListReduce(Def(DListGroupByKey(r, splits, part)), f) =>
          val stm = analyzer.findDef(d)
          wt.register(stm.syms.head) {
            toAtom2(new DListReduceByKey(wt(r), wt(f), wt(splits), part.map(wt.apply))(d.mKey, d.mValue))(mtype(stm.syms.head.tp), implicitly[SourceContext])
          }
      }
    }
  }

}

trait SparkDListFieldAnalysis extends DListFieldAnalysis {
  val IR: SparkDListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
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
    DListNode,
    DListReduceByKey,
    DListSortByKey,
    DListCache,
    DListMaterialize,
    DListTakeSampleNum,
    DListTakeSample,
    GetArgs
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, Closure2Node, freqHot, freqNormal, Lambda, Lambda2 }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  class SparkNarrowerInsertionTransformation extends NarrowerInsertionTransformation {
    override def registerTransformations(analyzer: Analyzer) {
      super.registerTransformations(analyzer)
      // TODO should use narrowBefore instead of narrowBeforeCandidates.
      // but maybe the whole candidates thing is suboptimal anyway
      analyzer.narrowBeforeCandidates.foreach {
        case d @ DListReduceByKey(in, func, splits, part) =>
          val stm = findDefinition(d).get
          class ReduceByKeyTransformer[K: Manifest, V: Manifest](in: Exp[DList[(K, V)]]) {
            val mapNew = makeNarrower(in)
            val redNew = DListReduceByKey(mapNew, wt(func), wt(splits), part.map(wt.apply))
            wt.register(stm.syms.head)(IR.toAtom2(redNew)(IR.mtype(d.getTypes._2), implicitly[SourceContext]))
          }
          new ReduceByKeyTransformer(d.in)(d.mKey, d.mValue)

        case d @ DListCache(dlist) =>
          val stm = findDefinition(d).get
          val mapNew = makeNarrower(dlist)
          val cacheNew = IR.dlist_cache(mapNew)(d.mA)
          wt.register(stm.syms.head)(cacheNew)
        case _ =>
      }

    }
  }

  override def newFieldAnalyzer(block: Block[_], typeHandlerForUse: TypeHandler = typeHandler) = new SparkFieldAnalyzer(block, typeHandlerForUse)

  override def newAnalyzer(block: Block[_]) = new SparkAnalyzer(block)

  class SparkAnalyzer(block: Block[_]) extends Analyzer(block) {
    override def isNarrowBeforeCandidate(x: DListNode) = x match {
      case DListReduceByKey(_, _, _, _) => true
      case DListCache(_) => true
      case x => super.isNarrowBeforeCandidate(x)
    }
  }

  class SparkFieldAnalyzer(block: Block[_], typeHandler: TypeHandler) extends FieldAnalyzer(block, typeHandler) {

    override def computeFieldReads(node: DListNode): Set[FieldRead] = node match {
      case v @ DListReduceByKey(in, func, _, _) => {
        // analyze function
        // convert the analyzed accesses to accesses of input._2
        val part1 = (analyzeFunction(v) ++ Set(FieldRead("input")))
          .map(_.path.drop(5))
          .map(x => "input._2" + x)
          .map(FieldRead)
        // add the accesses from successors
        val part2 = v.successorFieldReads
        val part3 = visitAll("input._1", v.getTypes._1.typeArguments(0))
        (part1 ++ part2 ++ part3).toSet
      }

      case v @ DListCache(in) => node.successorFieldReads.toSet

      case d @ DListSortByKey(in, _) =>
        node.successorFieldReads.toSet ++
          (if (isStruct(d.mK))
            visitAll("input", d.mK).filter(_.path.startsWith("input._1"))
          else
            List(FieldRead("input._1")))

      case _ => super.computeFieldReads(node)
    }
  }

}

trait SparkGenDList extends ScalaGenBase with ScalaGenDList with DListTransformations
    with SparkTransformations with Matchers with SparkDListFieldAnalysis with CaseClassTypeFactory {

  val IR: SparkDListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    NewDList,
    DListSave,
    DListMap,
    DListFilter,
    DListFlatMap,
    DListFlatten,
    DListGroupByKey,
    DListSortByKey,
    DListJoin,
    DListCogroup,
    DListReduce,
    DListMaterialize,
    ComputationNode,
    DListNode,
    DListTakeSample,
    DListTakeSampleNum,
    GetArgs,
    IteratorValue
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ DListReduceByKey, DListCache }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }

  val getProjectName = "spark"

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case nv @ NewDList(filename) => emitValDef(sym, "sc.textFile(%s)".format(quote(filename)))
      case vs @ DListSave(dlist, filename) => emitValDef(sym, "%s.saveAsTextFile(%s)".format(quote(dlist), quote(filename)))
      case vm @ DListMap(dlist, function) => emitValDef(sym, "%s.map(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFilter(dlist, function) => emitValDef(sym, "%s.filter(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatMap(dlist, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatten(v1) => {
        var out = v1.map(quote(_)).mkString("(", ").union(", ")")
        emitValDef(sym, out)
      }
      case gbk @ DListGroupByKey(dlist, _, Some(part)) => emitValDef(sym, "%s.groupByKey(makePartitioner(%s, sc.defaultParallelism))".format(quote(dlist), handleClosure(part)))
      case v @ DListJoin(left, right, Const(-2)) => emitValDef(sym, "%s.join(%s)".format(quote(left), quote(right)))
      case v @ DListJoin(left, right, Const(-1)) => emitValDef(sym, "%s.join(%s, %s)".format(quote(left), quote(right), parallelism))
      case v @ DListJoin(left, right, splits) => emitValDef(sym, "%s.join(%s, %s)".format(quote(left), quote(right), quote(splits)))
      case v @ DListCogroup(left, right) => emitValDef(sym, "%s.cogroup(%s)".format(quote(left), quote(right)))
      case red @ DListReduce(dlist, f) => emitValDef(sym, "%s.map(x => (x._1,x._2.reduce(%s)))".format(quote(dlist), handleClosure(red.closure)))
      case red @ DListReduceByKey(dlist, f, _, Some(part)) => emitValDef(sym, "%s.reduceByKey(makePartitioner(%s, sc.defaultParallelism), %s)".format(quote(dlist), handleClosure(part), handleClosure(red.closure)))
      case red @ DListReduceByKey(dlist, f, splits, None) => emitValDef(sym, "%s.reduceByKey(%s%s)".format(quote(dlist), handleClosure(red.closure),
        splits match {
          case Const(-2) => ""
          case Const(-1) => " _, " + parallelism
          case _ => " _, " + quote(splits)
        }))
      case red @ DListSortByKey(dlist, asc) => emitValDef(sym, "%s.sortByKey(%s)".format(quote(dlist), quote(asc)))
      case v @ DListCache(dlist) => emitValDef(sym, "%s.cache()".format(quote(dlist)))
      case v @ DListMaterialize(dlist) => emitValDef(sym, "%s.collect()".format(quote(dlist)))
      //def sample(withReplacement: Boolean, fraction: Double, seed: Int)
      case v @ DListTakeSample(in, frac, seedOption) => {
        val seed = seedOption.map(quote).getOrElse("System.currentTimeMillis.toInt")
        emitValDef(sym, "%s.sample(false, %s, %s)".format(quote(in), quote(frac), seed))
      }
      case v @ DListTakeSampleNum(in, r, n, s) => emitValDef(sym, "%s.takeSample(%s, %s, %s)".format(quote(in), quote(r), quote(n), quote(s)))
      case GetArgs() => emitValDef(sym, "sparkInputArgs.drop(1); // First argument is for spark context")
      case sd @ IteratorValue(r, i) => emitValDef(sym, "it.next // loop var " + quote(i))
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  var reduceByKey = true

  def transformTree[B: Manifest](block: Block[B]) = {
    var y = block
    // merge groupByKey with reduce to reduceByKey
    if (reduceByKey) {
      val rbkt = new ReduceByKeyTransformation()
      y = rbkt.run(y)
    }
    println("Narrowing existing maps")
    // narrow the existing maps
    y = doNarrowExistingMaps(y)
    println("Inserting narrowers and narrowing")
    // inserting narrower maps and narrow them
    y = insertNarrowersAndNarrow(y, new SparkNarrowerInsertionTransformation())

    prepareGraphData(y, true)

    if (loopFusion) {
      // lower monadic ops to lms loops
      y = new MonadicToLoopsTransformation().run(y)

      // inline lambdas generated by narrowing
      if (inlineInLoopFusion)
        y = new InlineTransformation().run(y)
    }

    y
  }

  val collectionName = "RDD"

  override def getParams(): List[(String, Any)] = {
    super.getParams() ++ List(("reduce by key", reduceByKey))
  }

  override def emitProgram[A, B](f: Exp[A] => Exp[B], className: String, stream: PrintWriter, pack: String)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {

    val x = fresh[A]
    var y = reifyBlock(f(x))

    typeHandler = new TypeHandler(y)
    //    getSchedule(availableDefs)(y.res, true).foreach { println }
    //    getSchedule(availableDefs)(y.res, true).foreach { println }
    val packName = makePackageName(pack)
    stream.println("/*****************************************\n" +
      "  Emitting Spark Code                  \n" +
      "*******************************************/")
    stream.println("""
package dcdsl.generated%s;
import scala.math.random
import spark._
import SparkContext._
import com.esotericsoftware.kryo.Kryo
import ch.epfl.distributed.utils.Helpers.makePartitioner

object %s {
    %s
    def main(sparkInputArgs: Array[String]) {
        System.setProperty("spark.default.parallelism", "%s")
        System.setProperty("spark.local.dir", "/mnt/tmp")
        System.setProperty("spark.serializer", "spark.KryoSerializer")
        System.setProperty("spark.kryo.registrator", "dcdsl.generated%s.Registrator_%s")
        System.setProperty("spark.kryoserializer.buffer.mb", "20")
        System.setProperty("spark.cache.class", "spark.DiskSpillingCache")
        
    		val sc = new SparkContext(sparkInputArgs(0), "%s")
        """.format(packName, className, getOptimizations(), parallelism,
      packName, className, className))

    val oldAnalysis = newAnalyzer(y)

    y = transformTree(y)
    val newAnalysis = newAnalyzer(y)
    withStream(stream) {
      emitBlock(y)
    }
    println("old vs new syms " + oldAnalysis.statements.size + " " + newAnalysis.statements.size)

    //    innerScope.foreach(println)

    //    oldAnalysis.orderedStatements.foreach(println)
    //    newAnalysis.orderedStatements.foreach(println)

    stream.println("""
        System.exit(0)
        }""")
    stream.println("}")
    stream.println("// Types that are used in this program")
    val restTypes = types.filterKeys(x => !skipTypes.contains(x))
    stream.println(restTypes.values.toList.sorted.mkString("\n"))

    stream.println("""class Registrator_%s extends KryoRegistrator {
        def registerClasses(kryo: Kryo) {
        %s
    kryo.register(classOf[ch.epfl.distributed.datastruct.SimpleDate])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Date])
    kryo.register(classOf[ch.epfl.distributed.datastruct.DateTime])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Interval])
  }
}""".format(className, types.keys.toList.sorted.map("kryo.register(classOf[" + _ + "])").mkString("\n")))
    stream.println("/*****************************************\n" +
      "  End of Spark Code                  \n" +
      "*******************************************/")

    stream.flush

    types.clear()
    //    writeGraphToFile(y, "test.dot", true)
    reset
    Nil
  }

}

trait SparkLoopsGen extends ScalaGenLoops with DListBaseCodeGenPkg {
  val IR: LoopsExp with DListOpsExp
  import IR._
}

trait ScalaGenSparkFat extends ScalaGenLoopsFat {
  val IR: DListOpsExp with LoopsFatExp
  import IR._

  override def emitFatNode(sym: List[Sym[Any]], rhs: FatDef) = rhs match {
    case SimpleFatLoop(Def(ShapeDep(sd, _)), x, rhs) =>
      val ii = x

      for ((l, r) <- (sym zip rhs)) r match {
        // temporary workaround for lost types
        case IteratorCollect(g @ Def(Reflect(ys @ YieldSingle(_, _), _, _)), b @ Block(y)) =>
          val outType = stripGen(g.tp)
          stream.println("val " + quote(sym.head) + " = " + quote(sd) + """.mapPartitions(it => {
        new Iterator[""" + outType + """] {
          private[this] val buff = new ch.epfl.distributed.datastruct.FastArrayList[""" + outType + """](1 << 10)
  		  private[this] final var start = 0
          private[this] final var end = 0

          @inline
          private[this] final def load = {
            var i = 0
          	buff.clear()
            while (i == 0 && it.hasNext) {
          """)
        case ForeachElem(y) =>
          stream.println("{ val it = " + quote(sd) + ".iterator") // hack for the wrong interface
          stream.println("while(it.hasNext) { // flatMap")
      }

      val gens = for ((l, r) <- sym zip rhs if !r.isInstanceOf[ForeachElem[_]]) yield r match {
        case IteratorCollect(g, Block(y)) =>
          (g, (s: List[String]) => {
            stream.println("buff(i) = " + s.head + "// yield")
            stream.println("i = i + 1")
            stream.println("val " + quote(g) + " = ()")
          })
      }

      withGens(gens) {
        emitFatBlock(syms(rhs).map(Block(_)))
      }
      stream.println("}")

      // with iterators there is no horizontal fusion so we do not have to worry about the ugly prefix and suffix
      for ((l, r) <- (sym zip rhs)) r match {
        case IteratorCollect(g, Block(y)) =>
          stream.println("""
            start = 0
            end = buff.length
          }

          override def hasNext(): Boolean = {
            if (start == end) load
            
            val hasAnElement = start != end
            if (!hasAnElement) buff.destroy()
            hasAnElement
          }

          override def next = {
            if (start == end) load

            val res = buff(start)
            start += 1
            res
          }
        }
      })""")
        case ForeachElem(y) =>
          stream.println("}")
      }

    case _ => super.emitFatNode(sym, rhs)
  }
}

trait ScalaGenSparkFatFlatMap extends ScalaGenLoopsFat {
  val IR: DListOpsExp with LoopsFatExp
  import IR._

  override def emitFatNode(sym: List[Sym[Any]], rhs: FatDef) = rhs match {
    case SimpleFatLoop(Def(ShapeDep(sd, _)), x, rhs) =>
      val ii = x

      for ((l, r) <- (sym zip rhs)) r match {
        // temporary workaround for lost types
        case IteratorCollect(g @ Def(Reflect(ys @ YieldSingle(_, _), _, _)), b @ Block(y)) =>
          val outType = stripGen(g.tp)
          stream.println("val " + quote(sym.head) + " = " + quote(sd) + """.flatMap(input => {
              val out = scala.collection.mutable.Buffer[""" + outType + """]();
              {""")
        case ForeachElem(y) =>
          stream.println("{ val it = " + quote(sd) + ".iterator") // hack for the wrong interface
          stream.println("while(it.hasNext) { // flatMap")
      }

      val gens = for ((l, r) <- sym zip rhs if !r.isInstanceOf[ForeachElem[_]]) yield r match {
        case IteratorCollect(g, Block(y)) =>
          (g, (s: List[String]) => {
            stream.println("out += " + s.head + "// yield")
            stream.println("val " + quote(g) + " = ()")
          })
      }

      withGens(gens) {
        emitFatBlock(syms(rhs).map(Block(_)))
      }
      stream.println("}")

      // with iterators there is no horizontal fusion so we do not have to worry about the ugly prefix and suffix
      for ((l, r) <- (sym zip rhs)) r match {
        case IteratorCollect(g, Block(y)) =>
          stream.println("""out
      })""")
        case ForeachElem(y) =>
          stream.println("}")
      }

    case _ => super.emitFatNode(sym, rhs)
  }

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case sd @ IteratorValue(r, i) => emitValDef(sym, "input // loop var " + quote(i))
      case _ => super.emitNode(sym, rhs)
    }
  }

}

trait SparkGen extends ScalaFatLoopsFusionOpt with DListBaseCodeGenPkg with SparkGenDList with ScalaGenSparkFat {
  val IR: SparkDListOpsExp

}

trait SparkFlatMapGen extends ScalaFatLoopsFusionOpt with DListBaseCodeGenPkg with SparkGenDList with ScalaGenSparkFatFlatMap {
  val IR: SparkDListOpsExp

}

