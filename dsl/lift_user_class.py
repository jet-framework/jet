#!/usr/local/bin/python

# Taken from https://github.com/stanford-ppl/Delite/blob/4e59cb689e5538faec4628f6179b0cddbe43ed86/bin/lift_user_class.py

from optparse import OptionParser
import os, errno


options = {}
classes = []

def main():
    usage = "usage: %prog [options] output_dir input_dir(s)"
    parser = OptionParser(usage)
    parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
    parser.add_option("--dsl", dest="dsl_name", default="optiml", help="specify which dsl you want to process ops on behalf")

    (opts, args) = parser.parse_args()
    if len(args) < 2:
        parser.error("incorrect number of arguments")

    ops_dir = args.pop(0)
    impls_dir = args.pop(0)

    initDir(ops_dir)
    initDir(impls_dir)

    loadOptions(opts)
    
    #create the application ops file
    fileOut = open (ops_dir + "/ApplicationOps.scala", 'w')

    #emit top-level stuff
    emitHeader(fileOut)

    #get the list of files
    files = os.listdir(impls_dir)
    for fname in files:
        if fname.endswith(".scala") and os.path.isdir(impls_dir + "/" + fname) == False:
            print "processing file:[" + fname + "]"
            liftClass(impls_dir, fname, fileOut)
    #now emit the application ops directory
    emitApplicationOps(fileOut)
    fileOut.close()

def emitApplicationOps(fileOut):
    out = []
    #package
    l = "trait ApplicationOps extends " + mixify(classes, "", "Ops") + "\n"
    l = l + "trait ApplicationOpsExp extends " + mixify(classes, "", "OpsExp") + "\n"
    out.append(l)
    fileOut.writelines(out)


def initDir(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
          pass
        else: raise

    
    
def emitHeader(fileOut):
    out = []
    l="/***********************************************************\n\
** AUTOGENERATED USING dsl/lift_user_class.py\n\
************************************************************/\n\n"
#    l = l + "package ppl.dsl." + options['dsl'] + ".user.applications\n\n"
    #take care of package and imports
    l = l + """
import java.io.PrintWriter
import scala.collection.immutable.ListMap
import scala.virtualization.lms.common.ScalaGenFat
import scala.virtualization.lms.util.OverloadHack
import scala.virtualization.lms.common.{ Base, StructExp, EffectExp, BaseFatExp, Variables, StringOps, ArrayOps, LiftPrimitives }
import ch.epfl.distributed.{ StringAndNumberOps, DateOps}
import ch.epfl.distributed.datastruct.Date

trait ParserOps extends StringOps with ArrayOps with StringAndNumberOps with DateOps with LiftPrimitives
"""

    out.append(l)
    fileOut.writelines(out)
#now emit DSL specific stuff
    if(options['dsl'] == "optiql"):
        emitHeaderOptiQL(fileOut)

def emitHeaderOptiQL(fileOut):
    out = []
    l = "//OptiQL Specific Header\n"
    l = l + "import ppl.dsl.optiql.datastruct.scala.util.Date\n\n"
    out.append(l)
    fileOut.writelines(out)

def mixify(classes, pre, post):
    l = ""
    for c in classes:
        l = l + pre + c + post
        if(classes.index(c) != len(classes) - 1):
            l = l + " with "
    return l


def liftClass(impls_dir, fname, fileOut):
   
    #dictionary to keep track of fields and their types
    fields = []
    types = {}
    clazz = ""
    out = []

    file = open(impls_dir + '/' + fname, 'r')
    for line in file:
        #take care of package and imports
        line = line.lstrip().rstrip()

        #take care of class definition
        if line.startswith("class "):
            if clazz != "":
                print "Found what I think are two class definitions [" + clazz + "] and also this line:\n" + line
            clazz = line[6:].lstrip().rstrip('{').rstrip('(').rstrip()
            classes.append(clazz)
            lclazz = clazz.lower()
            if options['verbose']:
                print "found class[" + clazz + "]"
            #add space after imports
            out.append("\n")
            continue
        #take care of vals
        if line.startswith("val "):
            #strip the val
            line = line [4:].lstrip().rstrip(",")
            tokens = line.split(":")
            name = tokens[0].rstrip()
            type = tokens[1].lstrip()
            if options['verbose']:
                print "found val:[" + name + ":" + type + "]"
            fields.append(name)
            types[name]= type
            continue
                
        #anything else just ignore
        if len(line) != 0:
            print "ignoring following line:[" + line + "]"
    file.close()

    
    
    #First handle the Ops part
    #Hack: Nested structures don't work yet, would need 2 passes
    newTrait = "" if clazz!="N1" else "with N2Ops"
    l = "trait " + clazz + "Ops extends Base with Variables with OverloadHack with ParserOps %s {\n\n" %newTrait
    l = l + " class " + clazz+ " \n\n"
    l = l + " object " + clazz + " {\n"
    l = l + " def apply(" + repify(fields, types) + ") = " + lclazz + "_obj_new(" + listify(fields) + ")\n"
    fieldParsers = parsify(fields, types)
    retType = " : Rep[%s] " % clazz
    if fieldParsers is not None:
	    l += """def parse(input: Rep[String], sep: Rep[String]) %s = fromArray(input.split(sep, %s))

def fromArray(input: Rep[Array[String]]) %s = {
	%s(%s)
	}
""" % (retType, len(fields), retType, clazz, fieldParsers)
    l = l + " }\n\n"

    l = l + " implicit def rep" + clazz + "To" + clazz + "Ops(x: Rep[" + clazz +"]) = new " + lclazz + "OpsCls(x)\n"

    l = l + " class " + lclazz + "OpsCls(__x: Rep[" + clazz + "]) {\n"
    for f in fields:
        l = l + " def " + f + " = " + lclazz + "_" + f + "(__x)\n"
    l = l + " }\n\n"
    
    l = l + " //object defs\n"
    l = l + " def " + lclazz + "_obj_new(" + repify(fields, types) + "): Rep[" + clazz + "]\n\n"
    
    l = l + " //class defs\n"
    for f in fields:
        l = l + " def " + lclazz + "_" + f + "(__x: Rep[" + clazz + "]): Rep[" + types[f] + "]\n"
    l = l + "}\n\n"

    #OpsExp
    l = l + "trait " + clazz + "OpsExp extends " + clazz + "Ops with StructExp with EffectExp with BaseFatExp {\n"
    #we are going to handle this once and for all using a FieldAccessOps
    #for f in fields:
    # l = l + " case class " + clazz + f.capitalize() + "(__x: Exp[" + clazz + "]) extends Def[" + types[f] + "]\n"
    #l = l + "\n"
    newStruct = """struct[%s](ClassTag[%s]("%s"), """ % (clazz, clazz, clazz)
    l = l + " def " + lclazz + "_obj_new(" + expify(fields, types) + ") = "+newStruct + mapify(fields) + ")\n"
    for f in fields:
        l = l + " def " + lclazz + "_" + f + "(__x: Rep[" + clazz + "]) = field["+ types[f] +"](__x, \"" + f + "\")\n"
    #emit Mirror

    l = l + "}\n\n"

    out.append(l)
    if options['verbose']:
        #print "resulting file:"
        for x in out:
            print x,
    fileOut.writelines(out)
    


def repify(fields, types):
    l = ""
    for f in fields:
        l = l + f + ": Rep[" + types[f] + "]"
        if(fields.index(f) != len(fields) - 1):
            l = l + ", "
    return l
def parsify(fields, types):
    l = ""
    canConvert = ["Long", "Int", "Float", "Double", "Boolean", "Short", "Byte", "Char", "Date"]
    noConvert = ["String"]
    for i, f in enumerate(fields):

	conversion = ""
	if types[f] in canConvert:
		conversion += ".to"+types[f]
	elif types[f] not in noConvert:
		print "Did not find type "+types[f]
		return None
	l += "input(%s)%s" % (i, conversion)
        if(fields.index(f) != len(fields) - 1):
            l = l + ", "
    return l

def expify(fields, types):
    l = ""
    for f in fields:
        l = l + f + ": Exp[" + types[f] + "]"
        if(fields.index(f) != len(fields) - 1):
            l = l + ", "
    return l

def quotify(fields):
    l = "\" + "
    for f in fields:
        l = l + "quote(" + f + ") "
        if(fields.index(f) != len(fields) - 1):
            l = l + " + \",\" + "
    l = l + " + \""
    return l


def listify(fields):
    l = ""
    for f in fields:
        l = l + f
        if(fields.index(f) != len(fields) - 1):
            l = l + ", "
    return l

def mapify(fields):
    l = "ListMap("
    for f in fields:
        l = l + """ "%s" -> %s""" % (f,f)
        if(fields.index(f) != len(fields) - 1):
            l = l + ", "
    return l+")"


def loadOptions(opts):
    options['verbose'] = opts.verbose
    options['dsl'] = opts.dsl_name

if __name__ == "__main__":
    main()
