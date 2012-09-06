package ch.epfl.distributed

import scala.virtualization.lms.common.{ ScalaGenBase, LoopsFatExp, ScalaGenLoopsFat }
import java.io.PrintWriter
import scala.reflect.SourceContext
import scala.virtualization.lms.util.GraphUtil
import java.io.FileOutputStream
import scala.collection.mutable
import java.util.regex.Pattern
import java.io.StringWriter
import scala.virtualization.lms.common.WorklistTransformer
import java.io.FileWriter

trait ScoobiProgram extends DListProgram

trait ScoobiGenDList extends ScalaGenBase
    with DListFieldAnalysis
    with DListTransformations with Matchers with FastWritableTypeFactory {
  val IR: DListOpsExp
  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block }
  import IR.{
    NewDList,
    DListSave,
    DListMap,
    DListFilter,
    DListFlatMap,
    DListFlatten,
    DListGroupByKey,
    DListJoin,
    DListCogroup,
    DListReduce,
    ComputationNode,
    DListNode,
    DListMaterialize,
    GetArgs,
    IteratorValue
  }
  import IR.{ TTP, TP, SubstTransformer, Field }
  import IR.{ ClosureNode, freqHot, freqNormal, Lambda, Lambda2, Closure2Node }
  import IR.{ findDefinition, fresh, reifyEffects, reifyEffectsHere, toAtom }
  var wireFormats = List[String]()

  var implicits = ""

  val getProjectName = "scoobi"

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case nv @ NewDList(filename) => emitValDef(sym, "TextInput.fromTextFile(%s)".format(quote(filename)))
      case vs @ DListSave(dlist, filename) => emitValDef(sym, "persist(TextOutput.toTextFile(%s,%s))".format(quote(dlist), quote(filename)))
      case vs @ DListMaterialize(dlist) => emitValDef(sym, "persist(%s.materialize)".format(quote(dlist)))
      case vm @ DListMap(dlist, function) => emitValDef(sym, "%s.map(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFilter(dlist, function) => emitValDef(sym, "%s.filter(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatMap(dlist, function) => emitValDef(sym, "%s.flatMap(%s)".format(quote(dlist), handleClosure(vm.closure)))
      case vm @ DListFlatten(v1) => {
        var out = v1.map(quote(_)).mkString("(", " ++ ", ")")
        emitValDef(sym, out)
      }
      case gbk @ DListGroupByKey(dlist, _, _) => emitValDef(sym, "%s.groupByKey".format(quote(dlist)))
      case v @ DListJoin(left, right, _) => emitValDef(sym, "%s.join(%s)".format(quote(left), quote(right)))
      case v @ DListCogroup(left, right) => emitValDef(sym, "%s.coGroup(%s)".format(quote(left), quote(right)))
      case red @ DListReduce(dlist, f) => emitValDef(sym, "%s.combine(%s)".format(quote(dlist), handleClosure(f)))
      case sd @ IteratorValue(r, i) => emitValDef(sym, "input // loop var " + quote(i))
      case GetArgs() => emitValDef(sym, "scoobiInputArgs")
      case _ => super.emitNode(sym, rhs)
    }
    //    println(sym+" "+rhs)
    out
  }

  override val typesInInlinedClosures = true

  def mkWireFormats(): String = {
    // TODO: This is a bit of a hack. Cleaner solution pending.
    if (!types.values.exists(_.trim.startsWith("trait"))) return ""
    val out = new StringBuilder
    out ++= """implicit val wireFormat_simpledate = mkCaseWireFormatGen(SimpleDate, SimpleDate.unapply _)
        implicit val wireFormat_datetime = mkCaseWireFormatGen(DateTime, DateTime.unapply _)
   		implicit val wireFormat_date = mkAbstractWireFormat[Date, SimpleDate, DateTime]
        """
    def findName(s: String) = s.takeWhile('_' != _)
    val groupedNames = types.keySet.groupBy(findName).map { x => (x._1, (x._2 - (x._1)).toList.sorted) }.toMap
    var index = -1
    val caseClassTypes = groupedNames.values.flatMap(x => x).toList.sorted
    // generate the case class wire formats
    out ++= caseClassTypes.map { typ =>
      index += 1
      " implicit val wireFormat_%s = mkCaseWireFormatGen(%s.apply _, %s.unapply _) "
        .format(index, typ, typ)
    }.mkString("\n")
    out += '\n'
    // generate the abstract wire formats (link between an interface and its implementations)
    out ++= groupedNames.map { in =>
      index += 1;
      " implicit val wireFormat_%s = mkAbstractWireFormat%s[%s, %s] "
        .format(index, if (in._2.size == 1) "1" else "", in._1, in._2.mkString(", "))
    }.mkString("\n")
    // generate groupings (if a type is used as a key, this is needed)
    out ++= "\n//groupings\n"
    out ++= caseClassTypes.map { typ =>
      index += 1
      " implicit val grouping_%s = makeGrouping[%s] "
        .format(index, typ)
    }.mkString("\n")
    out += '\n'
    out.toString
  }

  def transformTree[B: Manifest](block: Block[B]) = {
    var y = block
    // narrow existing maps
    y = doNarrowExistingMaps(y)
    // inserting narrower maps and narrow
    y = insertNarrowersAndNarrow(y, new NarrowerInsertionTransformation)

    prepareGraphData(y, true)

    y = new MonadicToLoopsTransformation().run(y)
    if (loopFusion) {
      // lower monadic ops to lms loops

      // inline lambdas generated by narrowing
      if (inlineInLoopFusion)
        y = new InlineTransformation().run(y)
    }
    y
  }

  val collectionName = "DList"

  override def emitProgram[A, B](f: Exp[A] => Exp[B], className: String, streamIn: PrintWriter, pack: String)(implicit mA: Manifest[A], mB: Manifest[B]): List[(Sym[Any], Any)] = {

    val capture = new StringWriter
    val stream = new PrintWriter(capture)

    stream.println("/*****************************************\n" +
      "  Emitting Scoobi Code                  \n" +
      "*******************************************/")
    stream.println("""
package dcdsl.generated%s;
import ch.epfl.distributed.utils.WireFormatsGen
import com.nicta.scoobi.Emitter
import com.nicta.scoobi.Scoobi._
import com.nicta.scoobi.WireFormat
import ch.epfl.distributed.datastruct._
import ch.epfl.distributed.utils._
import org.apache.hadoop.io.{Writable, WritableUtils}
import java.io.{DataInput, DataOutput}
object %s extends ScoobiApp {
   %s
   def mkAbstractWireFormat1[T, A <: T: Manifest: WireFormat](): WireFormat[T] = new WireFormat[T] {
    import java.io.{ DataOutput, DataInput }

    override def toWire(obj: T, out: DataOutput) {
      implicitly[WireFormat[A]].toWire(obj.asInstanceOf[A], out)
    }

    override def fromWire(in: DataInput): T =
      implicitly[WireFormat[A]].fromWire(in)
  }
        
  def makeGrouping[A] = new Grouping[A] {
    def groupCompare(x: A, y: A): Int = (x.hashCode - y.hashCode)
  }
        
  implicit def WritableFmt2[T <: Serializable with Writable: Manifest] = new WireFormat[T] {
    def toWire(x: T, out: DataOutput) { x.write(out) }
    def fromWire(in: DataInput): T = {
      val x: T = implicitly[Manifest[T]].erasure.newInstance.asInstanceOf[T]
      x.readFields(in)
      x
    }
  }
  ###Implicits###
  def run () {
        val scoobiInputArgs = args
        import WireFormat.{ mkAbstractWireFormat }
        import WireFormatsGen.{ mkCaseWireFormatGen }
    	implicit val grouping_date = makeGrouping[Date]
    	implicit val grouping_simpledate = makeGrouping[SimpleDate]
    	implicit val grouping_datetime = makeGrouping[DateTime]
    	
        ###wireFormats###
        """.format(makePackageName(pack), className, getOptimizations(), className))

    val x = fresh[A]
    var y = reifyBlock(f(x))
    typeHandler = new TypeHandler(y)

    y = transformTree(y)

    withStream(stream) {
      emitBlock(y)
    }
    stream.println("}")
    stream.println("}")
    stream.println("// Types that are used in this program")

    stream.println(restTypes.values.toList.sorted.mkString("\n"))

    stream.println("/*****************************************\n" +
      "  End of Scoobi Code                  \n" +
      "*******************************************/")

    stream.flush

    val out = capture.toString
    // the filter is to make a copy of the keyset (mutable map)
    val prevTypes = types.keySet.filter(_ => true)
    val newOut1 = out.replace("###wireFormats###", mkWireFormats)
    val newOut2 = newOut1.replace("###Implicits###", implicits)
    streamIn.print(newOut2)
    streamIn.print(types.filterKeys(x => !prevTypes.contains(x)).values.mkString("\n"))
    reset
    Nil
  }

  override def reset {
    implicits = ""
    super.reset
  }

}

trait KryoScoobiGenDList extends ScoobiGenDList {
  //  override def getSuperTraitsForTrait: String = "extends KryoFormat"

  override def mkWireFormats(): String = {
    types += "Registrator" -> """class Registrator extends KryoRegistrator {
        import com.esotericsoftware.kryo.Kryo
        def registerClasses(kryo: Kryo) {
        %s
    kryo.register(classOf[ch.epfl.distributed.datastruct.SimpleDate])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Date])
    kryo.register(classOf[ch.epfl.distributed.datastruct.DateTime])
    kryo.register(classOf[ch.epfl.distributed.datastruct.Interval])
  }
}""".format(types.keys.toList.sorted.map("kryo.register(classOf[" + _ + "])").mkString("\n"))
    types += "KryoFormat" -> "trait KryoFormat"
    types += "KryoInstance" -> """object KryoInstance {
  def apply() = {
    if (instances.get == null) {
      instances.set(kryoInstance())
    }
    instances.get
  }
  private val instances = new ThreadLocal[SerializerInstance]()
  private def kryoInstance() = {
    val ks = new KryoSerializer()
    val r = new Registrator()
    r.registerClasses(ks.kryo)
    ks.newInstance
  }
}"""
    implicits += """

  implicit def KryoWireFormat[A <: KryoFormat] = new WireFormat[A] {
    def toWire(x: A, out: DataOutput) {
      val bytes = KryoInstance().serialize(x)
      WritableUtils.writeVInt(out, bytes.length)
      out.write(bytes)
    }
    def fromWire(in: DataInput): A = {
      val len = WritableUtils.readVInt(in)
      val bytes = Array.ofDim[Byte](len)
      in.readFully(bytes)
      KryoInstance().deserialize(bytes)
    }
  }
"""
    ""
  }

}

trait ScalaGenScoobiFat extends ScalaGenLoopsFat {
  val IR: DListOpsExp with LoopsFatExp
  import IR._

  override def emitFatNode(sym: List[Sym[Any]], rhs: FatDef) = rhs match {
    case SimpleFatLoop(Def(ShapeDep(sd, _)), x, rhs) =>
      val ii = x
      var outType = "Nothing"
      for ((l, r) <- sym zip rhs) r match {
        case IteratorCollect(g, Block(y)) =>
          outType = stripGen(g.tp)
          val inType = remap(sd.tp.typeArguments(0))
          stream.println("val " + quote(sym.head) + " = " + quote(sd) + """.parallelDo(new DoFn[""" + inType + """,""" + outType + """, Unit] {
        def setup(): Unit = {}
        def process(input: """ + inType + """, emitter: Emitter[""" + outType + """]): Unit = {
          """)
        case ForeachElem(y) =>
          stream.println("{ val it = " + quote(sd) + ".iterator") // hack for the wrong interface
          stream.println("while(it.hasNext) { // flatMap")
          stream.println("val input = it.next()")
      }

      val gens = for ((l, r) <- (sym zip rhs) if !r.isInstanceOf[ForeachElem[_]]) yield r match {
        case IteratorCollect(g, Block(y)) =>
          (g, (s: List[String]) => {
            stream.println("emitter.emit(" + s.head + ")// yield")
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
            def cleanup(emitter: Emitter[""" + outType + """]): Unit = {}
          })
          """)
        case ForeachElem(y) =>
          stream.println("}")
      }
    case _ => super.emitFatNode(sym, rhs)
  }
}

trait ScoobiGen extends ScalaFatLoopsFusionOpt with DListBaseCodeGenPkg with ScoobiGenDList with ScalaGenScoobiFat

trait KryoScoobiGen extends ScalaFatLoopsFusionOpt with DListBaseCodeGenPkg with KryoScoobiGenDList with ScalaGenScoobiFat

