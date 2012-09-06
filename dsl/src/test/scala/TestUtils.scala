import java.io.{ StringWriter, PrintWriter, File, FileWriter }
import scala.collection.mutable
import org.scalatest._
import ch.epfl.distributed.ScalaGenDList
import ch.epfl.distributed.TypeFactory

trait CodeGenerator {

  val pairs = mutable.HashMap[PrintWriter, StringWriter]()

  def setUpPrintWriter() = {
    val sw = new StringWriter()
    var pw = new PrintWriter(sw)
    pairs += pw -> sw
    pw
  }

  def writeToProject(pw: PrintWriter, projName: String, filename: String, pack: String = "", dotFile: String = "") {
    val packPath = if (pack == "") "" else pack + "/"
    writeToFile(pw, "%s/src/main/scala/generated/%s%s%s.scala".format(projName, packPath, filename, pack))
    if (false && dotFile != "") {
      val newPw = setUpPrintWriter()
      newPw.write(dotFile)
      writeToFile(newPw, "%s/src/main/scala/generated/%s%s%s.dot".format(projName, packPath, filename, pack))
      release(newPw)
    }
  }

  def writeToFile(pw: PrintWriter, dest: String) {
    val x = new File(dest.reverse.dropWhile(_ != '/').reverse)
    x.mkdirs
    println(x + " " + dest)
    val fw = new FileWriter(dest)
    fw.write(getContent(pw))
    fw.close
  }

  def getContent(pw: PrintWriter) = {
    pw.flush
    val sw = pairs(pw)
    sw.toString
  }

  def release(pw: PrintWriter) {
    pairs -= pw
    pw.close
  }

}

trait Versioned {
  def version: String
}

trait CodeGeneratorTestSuite extends Suite with CodeGenerator {

  def tryCompile(block: => Unit) {
    try {
      block
    } catch {
      case e =>
        e.printStackTrace
        println(e.getMessage)
        fail(e.getMessage)
    }

  }

}