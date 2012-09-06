package ch.epfl.distributed

import scala.virtualization.lms.util.GraphUtil
import scala.collection.mutable.Buffer
import scala.collection.mutable
import java.util.regex.Pattern
import scala.util.Random

trait DListAnalysis extends AbstractScalaGenDList with Matchers {

  val IR: DListOpsExp

  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block, TP }
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
    EndNode,
    GetArgs
  }
  import IR.{ SubstTransformer, Field }
  import IR.{ ClosureNode, Closure2Node, freqNormal, Lambda, Lambda2 }
  import IR.{ fresh, reifyEffects, reifyEffectsHere, toAtom }

  def newAnalyzer(block: Block[_]) = new Analyzer(block)

  class Analyzer(block: Block[_]) extends BlockVisitor(block) {
    lazy val nodes = statements.flatMap {
      case SomeDef(x: DListNode) => Some(x)
      case SomeDef(Reflect(x: DListNode, _, _)) => Some(x)
      case _ => Nil
    }
    lazy val nodesInReflects: Iterable[(DListNode, Option[Def[_]])] = statements.flatMap {
      case SomeDef(x: DListNode) => List((x, Some(x)))
      case SomeDef(r @ Reflect(x: DListNode, _, _)) => List((x, Some(r)))
      case _ => Nil
    }
    def findDef(node: DListNode with Def[_]) = {
      val defToSearch = nodesInReflects.find(_._1 == node) match {
        case Some((_, Some(x))) => x
        case _ => node
      }
      IR.findDefinition(defToSearch).get
    }
    lazy val loops = statements.flatMap {
      case SomeDef(s @ IR.SimpleLoop(x, y, z)) => Some(s)
      case Def(x) => { println("Did not match def " + x); None }
      case x => { println("Did not match " + x); None }
    }
    lazy val lambdas = statements.flatMap {
      case SomeDef(l @ Lambda(f, x, y)) => Some(l)
      case _ => None
    }
    lazy val lambda2s = statements.flatMap {
      case SomeDef(l @ IR.Lambda2(f, x1, x2, y)) => Some(l)
      case _ => None
    }
    lazy val endnodes = nodes.filter { case v: EndNode[_] => true; case _ => false }.map(_.asInstanceOf[EndNode[_]])

    def getInputs(x: DListNode) = {
      val syms = IR.syms(x)
      syms.flatMap { x: Sym[_] => IR.findDefinition(x) }.flatMap { _.defs.flatMap { _ match { case x: DListNode => Some(x) case _ => None } } }
    }

    lazy val ordered = GraphUtil.stronglyConnectedComponents(endnodes, getInputs).flatten

    def nodeSuccessors(x: DListNode) = nodes.filter(getInputs(_).contains(x))

    lazy val orderedStatements = getSchedule(availableDefs)(block.res, true)

    lazy val narrowBeforeCandidates: Iterable[DListNode] = ordered.filter(isNarrowBeforeCandidate)

    def isNarrowBeforeCandidate(x: DListNode) = x match {
      case DListGroupByKey(x, _, _) => true
      case DListJoin(x, y, _) => true
      // TODO case DListCogroup(x, y) => true
      case x => false
    }

    lazy val narrowBefore: Iterable[DListNode] = narrowBeforeCandidates
      .filter { x =>
        getInputs(x).size != x.metaInfos.getOrElse("insertedNarrowers", 0)
      }
      .filter { x =>
        getInputs(x).map {
          case x: DListNode if x.metaInfos.contains("narrower")
            || x.metaInfos.contains("toNarrow") => true
          case _ => false
        }.reduce(_ && _)
      }
      .filter {
        case x: ComputationNode => !isSimpleType(x.getElementTypes._1)
        case DListJoin(l, r, _) => true
        case _ => throw new RuntimeException("Add narrow before candidate here or in subclass")
      }

    private def getInputSyms(x: Sym[_]) = {
      IR.findDefinition(x) match {
        case Some(x) => IR.syms(x.defs.head)
        case _ => Nil
      }
    }

    def getNodesForSymbol(x: Sym[_]) = {
      GraphUtil.stronglyConnectedComponents(List(x), getInputSyms).flatten.reverse
    }

    def getNodesInLambda(x: Any) = {
      val out = x match {
        case Def(Lambda(_, x, IR.Block(y: Sym[_]))) => (x :: Nil, getNodesForSymbol(y))
        case Def(Lambda2(_, x1, x2, IR.Block(y: Sym[_]))) => (x1 :: x2 :: Nil, getNodesForSymbol(y))
        case _ => (Nil, Nil)
      }
      def isDependantOnInput(s: Sym[_], input: Sym[_]): Boolean = {
        val syms = getInputSyms(s)
        if (s == input || syms.contains(input)) {
          true
        } else {
          val recursed = syms.map(isDependantOnInput(_, input))
          recursed.fold(false)(_ || _)
        }
      }
      out._2.filter { node => out._1.map(input => isDependantOnInput(node, input)).fold(false)(_ || _) }
    }

    def getNodesInClosure(x: DListNode) = x match {
      case x: ClosureNode[_, _] => getNodesInLambda(x.closure)
      case x: Closure2Node[_, _, _] => getNodesInLambda(x.closure)
      case _ => Nil
    }

    def getIdForNode(n: DListNode with Def[_]) = {
      //      nodes.indexOf(n)
      val op1 = IR.findDefinition(n.asInstanceOf[Def[_]])
      if (op1.isDefined) {
        op1.get.syms.head.id
      } else {
        statements.flatMap {
          case TP(Sym(s), Reflect(x, _, _)) if x == n => Some(s)
          //          case TTPDef(Reflect(s@Def(x),_,_)) if x==n => Some(s)
          case _ => None
        }.head
      }
    }

    var addComments = true

    def exportToGraph = {
      //      println("All loops:")
      //      loops.foreach(println)
      val buf = Buffer[String]()
      buf += """digraph g {
ordering = "in";"""
      for (node <- nodes) {
        var comment = ""
        if (addComments && !node.metaInfos.isEmpty) {
          comment += "\\n" + node.metaInfos
        }
        buf += """%s [label="%s(%s)%s"];"""
          .format(getIdForNode(node), node.toString.takeWhile(_ != '('), getIdForNode(node),
            comment)
      }
      for (node <- nodes; input1 <- getInputs(node)) {
        val readPaths = (node.directFieldReads ++ input1.successorFieldReads).map(_.path)
        val readPathsSorted = readPaths.toList.distinct.sorted
        buf += """%s -> %s [label="%s"]; """.format(getIdForNode(input1), getIdForNode(node),
          readPathsSorted.mkString(","))
      }
      buf += "}"
      buf.mkString("\n")
    }

    /**
     * Exports the raw IR to .dot format.
     */
    def exportToGraphRaw = {
      val buf = Buffer[String]()
      buf += "digraph g {"
      for (sym <- statements.flatMap(_.syms)) {
        val df = IR.findDefinition(sym)
        buf += """%s [label="%s"];"""
          .format(sym.id, df.get.toString)
      }
      for (sym <- statements.flatMap(_.syms); input1 <- getInputSyms(sym)) {
        buf += """%s -> %s [label="%s"]; """.format(sym.id, input1.id, sym.id)
      }
      buf += "}"
      buf.mkString("\n")
    }

  }
}

trait DListFieldAnalysis extends DListAnalysis with DListTransformations {
  val IR: DListOpsExp

  import IR.{ Sym, Def, Exp, Reify, Reflect, Const, Block, TP }
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
    DListTakeSample,
    DListNode,
    EndNode,
    GetArgs
  }

  def newFieldAnalyzer(block: Block[_], typeHandlerForUse: TypeHandler = typeHandler) = new FieldAnalyzer(block, typeHandlerForUse)

  class FieldAnalyzer(block: Block[_], typeHandler: TypeHandler) extends Analyzer(block) {

    def pathToInput(node: Any, input: Sym[_], prefix: String = ""): Option[FieldRead] = {
      node match {
        case SomeAccess(nextNode, pathSegment) => pathToInput(nextNode, input, pathSegment + prefix)
        case x: Sym[_] if input == x => return Some(FieldRead("input" + prefix))
        case _ => return None
      }
    }

    def analyzeFunction(v: DListNode) = {
      val nodes = getNodesInClosure(v).flatMap(IR.findDefinition(_)).flatMap(_.defs)
      val fields = nodes.filter { SomeAccess.unapply(_).isDefined }
      fields.flatMap(n => pathToInput(n, getNodesInClosure(v).head)).toSet
    }

    import typeHandler._
    def visitAll(path: String, m: Manifest[_]) = {
      val reads = new mutable.HashSet[String]()
      val typ = typeHandler.getTypeAt(path, m)

      def recurse(path: String, typ: PartInfo[_]) {
        reads += path
        typ match {
          case TypeInfo(name, fields) => fields.foreach(x => recurse(path + "." + x.name, x))
          case FieldInfo(name, typ, pos) if typeHandler.typeInfos2.contains(typ) =>
            typeHandler.typeInfos2(typ).fields.foreach(x => recurse(path + "." + x.name, x))
          case _ =>
        }
      }
      recurse(path, typ)
      reads.map(FieldRead).toSet
    }

    def computeFieldReads(node: DListNode): Set[FieldRead] = {
      val out: Set[FieldRead] = node match {
        case NewDList(_) => Set()

        case v @ DListFilter(in, func) => analyzeFunction(v) ++ node.successorFieldReads

        case v @ DListMap(in, func) if !v.metaInfos.contains("narrowed")
          && !SimpleType.unapply(v.getClosureTypes._2).isDefined
          && hasObjectCreationInClosure(v) => {

          // tag this map to recognize it after transformations
          val id = "id_" + Random.nextString(20)
          v.metaInfos(id) = true
          // create narrowing transformation for this map
          val narrowMapTransformation = new NarrowMapsTransformation(v, typeHandler)
          // run the transformation
          val newBlock = narrowMapTransformation.run(block)

          // TODO is it needed? transformer.doTransformation(new FieldOnStructReadTransformation, 500)
          // analyze field reads of the new function
          val a2 = newFieldAnalyzer(newBlock, typeHandler)
          val candidates = a2.nodes.flatMap {
            case vm @ DListMap(_, _) if (vm.metaInfos.contains(id)) => {
              vm.metaInfos.remove(id)
              Some(vm)
            }
            case _ => None
          }
          // remove the tag, not needed afterwards
          v.metaInfos.remove(id)
          a2.analyzeFunction(candidates.head)
        }

        case v @ DListMap(in, func) => analyzeFunction(v)

        case v @ DListJoin(Def(left: DListNode), Def(right: DListNode), _) =>
          def fieldRead(x: List[String]) = FieldRead("input." + x.mkString("."))
          val reads = v.successorFieldReads.map(_.getPath.drop(1)).map {
            case "_2" :: "_1" :: x => List(left) -> fieldRead("_2" :: x)
            case "_2" :: "_2" :: x => List(right) -> fieldRead("_2" :: x)
            case "_1" :: x => List(left, right) -> fieldRead("_1" :: x)
            case x => Nil -> null
          }
          reads.foreach { case (targets, read) => targets.foreach { _.successorFieldReads += read } }
          visitAll("input", v.mIn1).filter(_.path.startsWith("input._1"))

        case v @ DListCogroup(Def(left: DListNode), Def(right: DListNode)) =>
          // TODO, this is not implemented
          /*
          def fieldRead(x: List[String]) = FieldRead("input." + x.mkString("."))
          val reads = v.successorFieldReads.map(_.getPath.drop(1)).map {
            case "_2" :: "_1" :: x => List(left) -> fieldRead("_2" :: x)
            case "_2" :: "_2" :: x => List(right) -> fieldRead("_2" :: x)
            case "_1" :: x => List(left, right) -> fieldRead("_1" :: x)
            case x => Nil -> null
          }
          
          reads.foreach { case (targets, read) => targets.foreach { _.successorFieldReads += read } }
          */
          def makeReads(visit: Manifest[_], add: String) = visitAll("input", visit).map { fr =>
            val buf = fr.getPath.toBuffer
            buf.insert(1, add)
            FieldRead(buf.mkString("."))
          }
          def leftVisits = makeReads(v.mV1, "_2")
          def rightVisits = makeReads(v.mV2, "_2")
          left.successorFieldReads ++= leftVisits
          right.successorFieldReads ++= rightVisits
          visitAll("input", v.mIn1).filter(_.path.startsWith("input._1"))

        case v @ DListReduce(in, func) =>
          // analyze function
          // convert the analyzed accesses to accesses of input._2.iterable
          val part1 = (analyzeFunction(v) ++ Set(FieldRead("input")))
            .map(_.path.drop(5))
            .map(x => "input._2.iterable" + x)
          // add the iterable to the path for reads from successors
          val part2 = v.successorFieldReads.map { _.getPath }.map {
            case "input" :: "_2" :: x => "input" :: "_2" :: "iterable" :: x
            case x => x
          }.map(_.mkString("."))
          (part1 ++ part2).map(FieldRead)

        case v @ DListGroupByKey(in, _, _) =>
          // rewrite access to input._2.iterable.X to input._2.X
          // add access to _1
          ((v.successorFieldReads.map(_.getPath).map {
            case "input" :: "_2" :: "iterable" :: x => "input" :: "_2" :: x
            case x => x
          }.map(_.mkString(".")))).map(FieldRead).toSet ++ visitAll("input._1", v.mInType)

        case v @ DListFlatten(inputs) =>
          // just pass on the successor field reads
          v.successorFieldReads.toSet

        case v: EndNode[_] => {
          val elemType = v.getElementTypes._1
          if (!isStruct(elemType)) {
            Set(FieldRead("input"))
          } else {
            // traverse all subtypes, mark all fields as read
            visitAll("input", elemType)
          }
        }

        case v @ DListFlatMap(in, func) => analyzeFunction(v)

        case d: DListTakeSample[_] => d.successorFieldReads.toSet

        case x => throw new RuntimeException("Need to implement field analysis for " + x)
        //Set[FieldRead]()
      }
      out
    }

    def makeFieldAnalysis {
      nodes.foreach {
        node =>
          node.directFieldReads.clear
          node.successorFieldReads.clear
      }

      ordered.foreach {
        node =>
          val reads = computeFieldReads(node)
          node.directFieldReads ++= reads
          getInputs(node).foreach { _.successorFieldReads ++= reads }
        //println("Computed field reads for " + node + " got " + reads)
      }
    }

    def hasObjectCreationInClosure(v: DListNode) = {
      val nodes = getNodesInClosure(v)
      nodes.find { case Def(s: IR.SimpleStruct[_]) => true case _ => false }.isDefined
    }

  }

}
