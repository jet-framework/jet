package ch.epfl.distributed

import scala.virtualization.lms.common._
import scala.virtualization.lms.internal._

trait DListProgram extends DListOps
  with ScalaOpsPkg
  with LiftScala
  with MoreIterableOps with StringAndNumberOps with DateOps

trait DListOpsExpBase extends DListOps
  with ScalaOpsPkgExp
  with FatExpressions with BlockExp with Effects with EffectExp
  with IfThenElseFatExp with LoopsFatExp
  with MoreIterableOpsExp with DateOpsExp
  with StringAndNumberOpsExp with StringPatternOpsExp
  with StructTupleOpsExp
  with Expressions

trait DListBaseExp extends DListOps
  with StructExp with StructExpOpt
  with FunctionsExp

trait DListProgramExp extends DListOpsExp
  with ScalaOpsPkgExp
  with FatExpressions with LoopsFatExp with IfThenElseFatExp

trait DListBaseCodeGenPkg extends ScalaCodeGenPkg
  with ScalaGenIterableOps
  with SimplifyTransform with GenericFatCodegen with LoopFusionOpt
  with FatScheduling with BlockTraversal with ScalaGenIfThenElseFat
  //with LivenessOpt
  with StringAndNumberOpsCodeGen with StringPatternOpsCodeGen
  with MoreIterableOpsCodeGen with ScalaGenDateOps { val IR: DListOpsExp }

trait PrinterGenerator extends DListBaseCodeGenPkg
    with ScalaGenDList //	with ScalaGenArrayOps with ScalaGenPrimitiveOps with ScalaGenStringOps
    //	with ScalaGenFunctions with ScalaGenWhile with ScalaGenVariables
    { val IR: DListProgramExp }

// Scoobi traits: