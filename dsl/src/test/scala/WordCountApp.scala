import java.io.PrintWriter
import java.io.StringWriter
import java.io.FileWriter
import ch.epfl.distributed._
import org.scalatest._
import scala.virtualization.lms.common.{ Base, StructExp, PrimitiveOps }
import scala.util.Random

trait WordCountApp extends DListProgram with ApplicationOps with SparkDListOps with StringAndNumberOps {

  def parse(x: Rep[String]): Rep[String] = {
    val splitted = x.split("\\s+")
    splitted.apply(2)
  }

  def wikiArticleWordcount2012(x: Rep[Unit]) = {
    //    val splitted = stopwords.split("\\s").
    //      toList.filter(_.length > 1).
    //      flatMap { x => scala.collection.immutable.List(x, x.capitalize) }
    //    val stopWordsList = unit(splitted.toArray)
    //    val stopWordsSet = Set[String]()
    //    stopWordsList.foreach { x =>
    //      stopWordsSet.add(x)
    //    }
    val read = DList(getArgs(0))
    val parsed = read.map(WikiArticle.parse(_, "\t"))

    parsed
      .map(x => "\\n" + x.plaintext)
      .map(_.replaceAll("""\[\[.*?\]\]""", " "))
      //      .flatMap(_.replaceAll("""((\\n((\d+px)?(left|right)?thumb(nail)?)*)|(\.(thumb)+))""", " ")
      //          .split("[^a-zA-Z0-9']+").toSeq)
      //      .flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
      .flatMap(_.replaceAll("""\\[nNt]""", " ").split("[^a-zA-Z0-9']+").toSeq)
      .filter(x => x.length > 1)
      //      .filter(x => !stopWordsSet.contains(x))
      .map(x => if (x.matches("^((left|right)*(thumb)(nail|left|right)*)+[0-9A-Z].*?")) x.replaceAll("((left|right)*thumb(nail|left|right)*)+", "") else x)
      //      .filter(x => !((x.contains("thumb") || x.contains("0px")) && x.matches("(\\d+px|left|right|thumb|nail)+")))
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .save(getArgs(1))
    //    parsed.map(_.plaintext).save(getArgs(1))
    unit(())
  }

  def wikiArticleWordcount2009Stopwords(x: Rep[Unit]) = {
    val splitted = stopwords.split("\\s").
      toList.filter(_.length > 1).
      flatMap { x => scala.collection.immutable.List(x, x.capitalize) }
    val stopWordsList = unit(splitted.toArray)
    val stopWordsSet = Set[String]()
    stopWordsList.foreach { x =>
      stopWordsSet.add(x)
    }
    val read = DList(getArgs(0))
    val parsed = read.map(WikiArticle.parse(_, "\t"))

    parsed
      .map(_.plaintext)
      //.map(x => if (x.startsWith("thumb")) x.substring(5) else x)
      .flatMap(_.replaceAll("""((\\n(thumb)*)|(\.(thumb)+))""", " ").split("[^a-zA-Z0-9']+").toSeq)
      //.flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
      .map(x => if (x.matches("(thumb)+[A-Z].*?")) x.replaceAll("(thumb)+", "") else x)
      .filter(x => x.length > 1)
      .filter(x => !stopWordsSet.contains(x))
      .filter(x => !x.matches("(left|right)(thumb)+"))
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .save(getArgs(1))
    //    parsed.map(_.plaintext).save(getArgs(1))
    unit(())
  }

  def statistics(x: Rep[Unit]) = {
    val read = DList(getArgs(0))
    val parsed = read.map(parse)
      .filter(_.matches(".*?en\\.wiki.*?/wiki/.*"))
    //    .filter(_.contains("/wiki/"))
    val parts = parsed.map(_.split("/+").last)
      .filter(!_.matches("[A-Za-z_]+:(?!_).*"))
    parts
      .map(x => (x, unit(1)))
      .groupByKey
      .reduce(_ + _)
      .filter(_._2 >= 5)
      .save(getArgs(1))
    //    parsed.save(folder+"/output/")
    unit(())
  }

  val stopwords = """a
about
above
after
again
against
all
am
an
and
any
are
aren't
as
at
be
because
been
before
being
below
between
both
but
by
can't
cannot
could
couldn't
did
didn't
do
does
doesn't
doing
don't
down
during
each
few
for
from
further
had
hadn't
has
hasn't
have
haven't
having
he
he'd
he'll
he's
her
here
here's
hers
herself
him
himself
his
how
how's
i
i'd
i'll
i'm
i've
if
in
into
is
isn't
it
it's
its
itself
let's
me
more
most
mustn't
my
myself
no
nor
not
of
off
on
once
only
or
other
ought
our
ours
ourselves
out
over
own
same
shan't
she
she'd
she'll
she's
should
shouldn't
so
some
such
than
that
that's
the
their
theirs
them
themselves
then
there
there's
these
they
they'd
they'll
they're
they've
this
those
through
to
too
under
until
up
very
was
wasn't
we
we'd
we'll
we're
we've
were
weren't
what
what's
when
when's
where
where's
which
while
who
who's
whom
why
why's
with
won't
would
wouldn't
you
you'd
you'll
you're
you've
your
yours
yourself
yourselves
"""

  def wikiArticleWordcount2009(x: Rep[Unit]) = {
    val read = DList(getArgs(0))
    val parsed = read.map(WikiArticle.parse(_, "\t"))

    parsed
      .map("\\n" + _.plaintext)
      .map(_.replaceAll("""\[\[.*?\]\]""", " "))
      .map(_.replaceAll("""(\\[ntT]|\.)\s*(thumb|left|right)*""", " "))
      .flatMap(_.split("[^a-zA-Z0-9']+").toSeq)
      //.map(x => if (x.matches("(thumb)+[A-Z].*?")) x.replaceAll("(thumb)+", "") else x)
      .filter(x => x.length > 1)
      .filter(x => !x.matches("""(thumb|left|right|\d+px){2,}"""))
      .map(x => (x, unit(1)))
      .groupByKey(getArgs(2).toInt)
      .reduce(_ + _)
      .save(getArgs(1))
    unit(())
  }

}

class WordCountAppGenerator extends CodeGeneratorTestSuite {

  val appname = "WordCountApp"
  val unoptimizedAppname = appname + "_Orig"
  
  // format: OFF
  /**
   * Variants:
   *  		CM	FR	LF	IN
   * v0:	-	-	-	-
   * v1:	x	-	-	-
   * v2:	-	x	-	-
   * v3:	-	-	x	-
   * v4:	x 	x	-	-
   * v5:	x	x 	x	-
   * v6:	x	x 	x	x
   */
  // format: ON
  def testBoth {
    tryCompile {
      println("-- begin")
      var fusionEnabled = false
      val dsl = new WordCountApp with DListProgramExp with ApplicationOpsExp with SparkDListOpsExp
      val codegenSpark = new SparkGen { val IR: dsl.type = dsl }
      val codegenScoobi = new ScoobiGen { val IR: dsl.type = dsl
        override def shouldApplyFusion(currentScope: List[IR.Stm])(result: List[IR.Exp[Any]]): Boolean = fusionEnabled
      }
//      codegenScoobi.useWritables = true;
      val codegenCrunch = new CrunchGen { val IR: dsl.type = dsl }
//      val list = List(codegenSpark, codegenScoobi, codegenCrunch)
      val list = List(codegenSpark,codegenCrunch)
      def writeVersion(version: String) {
//        if (version != "v0") return
        val func = dsl.wikiArticleWordcount2009 _
        for (gen <- list) {
          val versionDesc = version+ (gen match {
            case x: Versioned => x.version
            case _ => ""
          })
          var pw = setUpPrintWriter
          gen.emitProgram(func, appname, pw, versionDesc)
          writeToProject(pw, gen.getProjectName, appname, versionDesc, codegenSpark.lastGraph)
        }
      }
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
        codegen.inlineInLoopFusion = false
        codegen.loopFusion = false
      }
      fusionEnabled = false
      dsl.useFastSplitter = false
      dsl.disablePatterns = true
      dsl.useFastRegex = false
      writeVersion("v0")
      
      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
        codegen.inlineInLoopFusion = true
        codegen.loopFusion = true
      }
      fusionEnabled = true
      writeVersion("v1")

      dsl.disablePatterns = false
      writeVersion("v2")
      
      dsl.useFastSplitter = true
      writeVersion("v3")
      
      dsl.useFastRegex = true
      writeVersion("v4")

      /*
      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
        codegen.inlineInLoopFusion = false
        codegen.loopFusion = false
      }

      dsl.disablePatterns = true
      writeVersion("v0")

      dsl.disablePatterns = false
      writeVersion("v1")

      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
      }
      writeVersion("v4")

      list.foreach { _.loopFusion = true }
      writeVersion("v5")
      list.foreach { _.inlineInLoopFusion = true }
      writeVersion("v6")

      list.foreach { codegen =>
        codegen.narrowExistingMaps = false
        codegen.insertNarrowingMaps = false
        codegen.inlineInLoopFusion = false
      }
      dsl.disablePatterns = true
      writeVersion("v3")

      list.foreach { _.loopFusion = false }
      list.foreach { codegen =>
        codegen.narrowExistingMaps = true
        codegen.insertNarrowingMaps = true
      }
      writeVersion("v2")
	  */
      println("-- end")
    }
  }

}
