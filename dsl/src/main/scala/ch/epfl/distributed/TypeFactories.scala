package ch.epfl.distributed

import scala.collection.mutable

trait TypeFactory extends ScalaGenDList {
  val IR: DListOpsExp
  import IR.{ Sym, Def }

  def makeTypeFor(name: String, fields: Iterable[String]): String

  val types = mutable.Map[String, String]()

  val skipTypes = mutable.Set[String]()

  def restTypes = types.filterKeys(x => !skipTypes.contains(x))

  override def emitNode(sym: Sym[Any], rhs: Def[Any]) = {
    val out = rhs match {
      case IR.Field(tuple, x, tp) => emitValDef(sym, "%s.%s".format(quote(tuple), x))
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "(%s, %s)".format(quote(elems("_1")), quote(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
        //emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.SimpleStruct(IR.ClassTag(name), fields) => {
        try {
          val typeInfo = typeHandler.typeInfos2(name)
          val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
          val typeName = makeTypeFor(name, fieldsList.map(_._1))
          emitValDef(sym, "new %s(%s)".format(typeName, fieldsList.map(_._2).map(quote).mkString(", ")))
        } catch {
          case e =>
            emitValDef(sym, "Exception " + e + " when accessing " + fields + " of " + name)
            e.printStackTrace
        }
      }

      case _ => super.emitNode(sym, rhs)
    }
  }

  override def reset {
    types.clear
    super.reset
  }

}

trait TypeFactoryUtils extends TypeFactory {
  def getFieldList(name: String) = typeHandler.typeInfos2(name).fields
  def getFilteredFieldList(name: String, fields: Iterable[String]) = {
    val set = fields.toSet
    getFieldList(name).filter(x => set.contains(x.name))
  }
  def getNameFor(fieldsHere: List[FieldInfo[_]]) = fieldsHere.map(_.position + "").mkString("_", "_", "")
}

trait CaseClassTypeFactory extends TypeFactory with TypeFactoryUtils {
  def getSuperTraitsForTrait: String = "extends Serializable"
  def makeTypeFor(name: String, fields: Iterable[String]): String = {

    // fields is a sorted list of the field names
    // typeInfo is the type with all fields and all infos
    val typeInfo = typeHandler.typeInfos2(name)
    // this is just a set to have contains
    val fieldsSet = fields.toSet
    val fieldsInType = typeInfo.fields
    val fieldsHere = typeInfo.fields.filter(x => fieldsSet.contains(x.name))
    if (!types.contains(name)) {
      types(name) = "trait %s %s {\n%s\n} ".format(name,
        getSuperTraitsForTrait,
        fieldsInType.map {
          fi =>
            """def %s : %s = throw new RuntimeException("Should not try to access %s here, internal error")"""
              .format(fi.name, fi.niceName, fi.name)
        }.mkString("\n"))
    }
    val typeName = name + getNameFor(fieldsHere)
    if (!types.contains(typeName)) {
      val args = fieldsHere.map { fi => "override val %s : %s".format(fi.name, fi.niceName) }.mkString(", ")
      types(typeName) = """case class %s(%s) extends %s {
   override def toString() = {
        val sb = new StringBuilder()
        sb.append("%s(")
        %s
        sb.append(")")
        sb.toString()
   }
}""".format(typeName, args, name, name,
        fieldsInType
          .map(x => if (fieldsSet.contains(x.name)) x.name else "")
          .map(x => """%s sb.append(",")""".format(if (x.isEmpty) "" else "sb.append(%s); ".format(x)))
          .mkString(";\n"))
    }

    typeName
  }

}

trait WritableTypeFactoryUtils extends TypeFactoryUtils {
  val bitsetFieldName = "__bitset"
  val readBitSetLine = bitsetFieldName + " = WritableUtils.readVLong(in)"
  val writeBitSetLine = "WritableUtils.writeVLong(out, " + bitsetFieldName + ")"
  def getDefaultValueForType(x: Manifest[_]) = {
    x match {
      case _ if x <:< manifest[String] => """" """"
      case _ if x <:< manifest[Double] => "0"
      case _ if x <:< manifest[Long] => "0L"
      case _ if x <:< manifest[Int] => "0"
      case _ if x <:< manifest[Char] => "' '"
      case _ if x.toString == "ch.epfl.distributed.datastruct.Date" => "new ch.epfl.distributed.datastruct.Date()"
      case x => throw new RuntimeException("Implement default value for " + x)
    }
  }
  def generateWriteField(x: FieldInfo[_], read: Boolean) = {
    if (x.m.toString == "ch.epfl.distributed.datastruct.Date") {
      if (read) {
        x.name + ".readFields(in)"
      } else {
        x.name + ".write(out)"
      }
    } else if (x.m <:< manifest[Int]) {
      if (read) {
        x.name + "= WritableUtils.readVInt(in)"
      } else {
        "WritableUtils.writeVInt(out, " + x.name + ")"
      }
    } else if (x.m <:< manifest[Long]) {
      if (read) {
        x.name + "= WritableUtils.readVLong(in)"
      } else {
        "WritableUtils.writeVLong(out, " + x.name + ")"
      }
    } else {
      val t = x.m match {
        case _ if x.m <:< manifest[String] => "UTF"
        //            case _ if x.m <:< manifest[Int] => "Int"
        case _ if x.m <:< manifest[Double] => "Double"
        case _ if x.m <:< manifest[Char] => "Char"
        case _ => throw new RuntimeException("Implement read/write functionality for " + x)
      }
      val format = if (read) "%s = in.read%s" else "out.write%2$s(%1$s)"
      format.format(x.name, t)
    }
  }

  def getBitValue(fields: Iterable[FieldInfo[_]], typeInfo: TypeInfo[_]) = fields.map(x => x.position).map(1 << _).reduce(_ + _)

  def writeClass(name: String, bitset: Boolean, body: String) = {
    val fields = getFieldList(name)
    val bitSetField = if (bitset)
      "var %s: Long = %s;".format(bitsetFieldName, (1 << fields.size) - 1)
    else
      ""

    val constructor = "def this() = this(%s = %s)\n"
      .format(fields.head.name, getDefaultValueForType(fields.head.m))
    """
    case class %s(%s) extends Writable {
        %s
    	%s
  		%s
  	}
    """.format(name, getFieldList(name).map { fi =>
      """var %s : %s = %s""".format(fi.name, fi.niceName, getDefaultValueForType(fi.m))
    }.mkString(", "), constructor, bitSetField, body)
  }
}

trait WritableTypeFactory extends TypeFactory with WritableTypeFactoryUtils {
  val IR: DListOpsExp
  import IR.{ Sym, Def }

  var useBitset = true

  override def makeTypeFor(name: String, fields: Iterable[String]): String = {

    val fieldsHere = getFilteredFieldList(name, fields)

    def readAndWriteFields(fields: Iterable[FieldInfo[_]]) = {
      def getGuard(x: FieldInfo[_]) = {
        if (useBitset)
          "if ((%s & %s) > 0) ".format(bitsetFieldName, 1 << x.position)
        else
          ""
      }
      val readFieldsBody = fields.map { fi =>
        getGuard(fi) + " " + generateWriteField(fi, true)
      }.mkString("\n")
      val writeFieldsBody = fields.map { fi =>
        getGuard(fi) + " " + generateWriteField(fi, false)
      }.mkString("\n")
      val (readBitset, writeBitset) =
        if (useBitset)
          (readBitSetLine, writeBitSetLine)
        else
          ("", "")
      """  override def readFields(in : DataInput) {
      %s
      %s
  }
  override def write(out : DataOutput) {
      %s
      %s
  }""".format(readBitset, readFieldsBody, writeBitset, writeFieldsBody)
    }
    if (!types.contains(name)) {
      val body = readAndWriteFields(getFieldList(name))
      types(name) = writeClass(name, useBitset, body)
    }
    name
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]): Unit = {
    val out = rhs match {
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "(%s, %s)".format(quote(elems("_1")), quote(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
        //emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.SimpleStruct(IR.ClassTag(name), fields) => {
        try {
          val typeInfo = typeHandler.typeInfos2(name)
          val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
          val typeName = makeTypeFor(name, fieldsList.map(_._1))
          emitValDef(sym, "new %s(%s)".format(typeName, fieldsList.map { case (name, elem) => name + " = " + quote(elem) }.mkString(", ")))
          if (useBitset)
            stream.println(quote(sym) + "." + bitsetFieldName + " = " + fieldsList.map(x => typeInfo.getField(x._1).get.position).map(1 << _).reduce(_ + _))
        } catch {
          case e =>
            emitValDef(sym, "Exception " + e + " when accessing " + fields + " of " + name)
            e.printStackTrace
        }
      }

      case _ => super.emitNode(sym, rhs)
    }
  }

}
trait FastWritableTypeFactory extends TypeFactory with WritableTypeFactoryUtils {
  val IR: DListOpsExp
  import IR.{ Sym, Def }

  var useBitset = true

  val fastWritableTypeFactoryEnabled = true

  // the bitsets for all combinations that are encountered.
  var combinations: Map[String, mutable.Set[Long]] = Map.empty

  override def reset {
    combinations = Map.empty
    super.reset
  }

  // regenerate that type with all its combinations
  def updateType(name: String) {
    val fields = getFieldList(name)
    val readFields = """override def readFields(in : DataInput) { 
    %s
    %s match {
    %s case x => throw new RuntimeException("Unforeseen bit combination "+x)
	}
  }"""
    val writeFields = """override def write(out : DataOutput) { 
    %s
    %s match {
    %s case x => throw new RuntimeException("Unforeseen bit combination "+x)
	}
  }"""
    var caseBody = ""
    val bitsets = combinations(name)
    var methods = ""
    bitsets.foreach { long =>
      val fieldsHere = fields.filter(fi => ((1 << fi.position) & long) > 0)
      val suffix = getNameFor(fieldsHere)
      caseBody += "case %s => %%1$s%s(%%2$s)\n".format(long, suffix)
      val readFieldsBody = fieldsHere.map { fi =>
        generateWriteField(fi, true)
      }.mkString("\n")

      val writeFieldsBody = fieldsHere.map { fi =>
        generateWriteField(fi, false)
      }.mkString("\n")
      methods += """def readFields%s(in : DataInput) { 
    %s 
    } 
    	  """.format(suffix, readFieldsBody)
      methods += """def write%s(out : DataOutput) { 
    %s 
    } 
    	  """.format(suffix, writeFieldsBody)
    }
    val readFieldsBody = readFields.format(readBitSetLine, bitsetFieldName, caseBody.format("readFields", "in"))
    val writeFieldsBody = writeFields.format(writeBitSetLine, bitsetFieldName, caseBody.format("write", "out"))

    types(name) = writeClass(name, true, readFieldsBody + "\n" + writeFieldsBody + "\n\n" + methods)
  }

  override def makeTypeFor(name: String, fields: Iterable[String]): String = {
    if (!combinations.contains(name)) {
      combinations += name -> mutable.Set[Long]()
    }
    val typeInfo = typeHandler.typeInfos2(name)
    val fieldsHere = getFilteredFieldList(name, fields)
    combinations(name) += getBitValue(fieldsHere, typeInfo)
    updateType(name)
    name
  }

  override def emitNode(sym: Sym[Any], rhs: Def[Any]): Unit = {
    val out = rhs match {
      case IR.SimpleStruct(x: IR.ClassTag[_], elems) if (x.name == "tuple2s") => {
        emitValDef(sym, "(%s, %s)".format(quote(elems("_1")), quote(elems("_2")))) //fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
        //emitValDef(sym, "(%s)".format(fields.toList.sortBy(_._1).map(_._2).map(quote(_)).mkString(",")))
      }
      case IR.SimpleStruct(IR.ClassTag(name), fields) if fastWritableTypeFactoryEnabled => {
        try {
          val typeInfo = typeHandler.typeInfos2(name)
          val fieldsList = fields.toList.sortBy(x => typeInfo.getField(x._1).get.position)
          val typeName = makeTypeFor(name, fieldsList.map(_._1))
          emitValDef(sym, "new %s(%s)".format(typeName, fieldsList.map { case (name, elem) => name + " = " + quote(elem) }.mkString(", ")))
          if (useBitset)
            stream.println(quote(sym) + "." + bitsetFieldName + " = " + getBitValue(fieldsList.map(x => typeInfo.getField(x._1).get), typeInfo))
        } catch {
          case e =>
            emitValDef(sym, "Exception " + e + " when accessing " + fields + " of " + name)
            e.printStackTrace
        }
      }

      case _ => super.emitNode(sym, rhs)
    }
  }

}

trait SwitchingTypeFactory extends TypeFactory with CaseClassTypeFactory with FastWritableTypeFactory {
  val IR: DListOpsExp
  import IR.{ Sym, Def }
  var useWritables = true
  override def getParams(): List[(String, Any)] = {
    super.getParams() ++ List(("using writables", useWritables))
  }
  override def makeTypeFor(name: String, fields: Iterable[String]): String = {
    if (useWritables) {
      super[FastWritableTypeFactory].makeTypeFor(name, fields)
    } else {
      super[CaseClassTypeFactory].makeTypeFor(name, fields)
    }
  }
  override def emitNode(sym: Sym[Any], rhs: Def[Any]): Unit = {
    if (useWritables) {
      super[FastWritableTypeFactory].emitNode(sym, rhs)
    } else {
      super[CaseClassTypeFactory].emitNode(sym, rhs)
    }
  }
}