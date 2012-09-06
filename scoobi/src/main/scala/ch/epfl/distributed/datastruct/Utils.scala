package ch.epfl.distributed.datastruct

// TODO License
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import util.control.Breaks._
import java.util.regex.Pattern

object RegexFrontend {
  private val javaRegexOnly: Array[String] = Array("&&", "??", "*?", "+?", "}?", "?+", "*+", "++", "}+", "^", "$", "(?")

  /**
   * This function determines the type of pattern we are working with
   * The return value of the function determines the type we are expecting
   * @param pattern
   * @return int, 0 means this is java.util.regex,
   * 1 means this is dk.brics.automaton
   *
   * NOTE: This method has been borrowed from pig trunk 1d0bd25e526a534e67444754add49ab81bd7c8ed, modified and converted to Scala by Intellij IDEA.
   * Big thanks to the authors.
   */
  final def determineBestRegexMethod(pattern: String): Int = {

    var i = 0
    while (i < javaRegexOnly.length) {
      var j: Int = pattern.length
      while (j > 0) {
        j = pattern.lastIndexOf(javaRegexOnly(i), j)
        if (j > 0) {
          var precedingEsc: Int = precedingEscapes(pattern, j)
          if (precedingEsc % 2 == 0) {
            val x = javaRegexOnly(i)
            if (x == "^" && j > 0 && pattern.charAt(j - 1) == '['
              && precedingEscapes(pattern, j - 1) % 2 == 1) {
              return 0
            } else
              j -= 1
          }
          j = j - precedingEsc
        } else if (j == 0)
          return 0
      }
      i += 1;
    }

    var index = pattern.indexOf('[')
    if (index >= 0) {
      var precedingEsc: Int = precedingEscapes(pattern, index)
      if (index != 0) {
        while (precedingEsc % 2 == 1) {
          index = pattern.indexOf('[', index + 1)
          precedingEsc = precedingEscapes(pattern, index)
        }
      }
      var index2: Int = 0
      var index3: Int = 0
      breakable {
        while (index != -1 && index < pattern.length) {
          index2 = pattern.indexOf(']', index)
          if (index2 == -1) {
            break
          }
          precedingEsc = precedingEscapes(pattern, index2)
          while (precedingEsc % 2 == 1) {
            index2 = pattern.indexOf(']', index2 + 1)
            precedingEsc = precedingEscapes(pattern, index2)
          }
          if (index2 == -1) {
            break
          }
          index3 = pattern.indexOf('[', index + 1)
          precedingEsc = precedingEscapes(pattern, index3)
          if (index3 == -1) {
            break
          }
          while (precedingEsc % 2 == 1) {
            index3 = pattern.indexOf('[', index3 + 1)
            precedingEsc = precedingEscapes(pattern, index3)
          }
          if (index3 == -1) {
            break
          }
          if (index3 < index2) {
            return 0
          }
          index = index3
        }
      }
    }
    index = pattern.lastIndexOf('\\')
    if (index > -1) {
      var precedingEsc = precedingEscapes(pattern, index)
      while (index != -1) {
        if (precedingEsc % 2 == 0 && (index + 1) < pattern.length) {
          pattern.charAt(index + 1) match {
            case '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9'
              | 'a' | 'e' | '0' | 'x' | 'u' | 'c' | 'Q' | 'w' | 'W'
              | 'd' | 'D' | 's' | 'S' | 'p' | 'P' | 'b' | 'B' | 'A'
              | 'G' | 'z' | 'Z' =>
              return 0
            case _ =>
          }
        }
        index = index - (precedingEsc + 1)
        precedingEsc = -1
        if (index >= 0) {
          index = pattern.lastIndexOf('\\', index)
          precedingEsc = precedingEscapes(pattern, index)
        }
      }
    }

    1 // optimized automaton
  }

  private final def precedingEscapes(pattern: String, startIndex: Int): Int = {
    if (startIndex > 0) {
      var precedingEscapes: Int = 0
      var j: Int = startIndex - 1
      breakable {
        while (j >= 0) {
          if (pattern.charAt(j) == '\\')
            precedingEscapes += 1;
          else
            break

          j -= 1;
        }
      }
      return precedingEscapes
    } else if (startIndex == 0) {
      return 0
    }
    -1
  }

}

import dk.brics.automaton.RegExp;
import dk.brics.automaton.RunAutomaton;

class RegexFrontend(val original: String, optimizeAll: Boolean = true, useFastSplitter: Boolean = true) extends Serializable {

  var matchByte: Byte = -1
  var pattern: Pattern = null
  var runAutomaton: RunAutomaton = null

  val matchOn = original.replaceAll("(?<!\\\\)\\\\d", "[0-9]")
    .replaceAll("(?<!\\\\)\\\\s", "[ \t\n\f\r]")
    .replaceAll("(?<!\\\\)\\\\w", "[a-zA-Z_0-9]")
  val method = if (optimizeAll) RegexFrontend.determineBestRegexMethod(matchOn) else 0

  lazy val changed = if (original == matchOn) "" else " (converted from " + original + ")"
  //  println("Method for " + matchOn + changed + " is " + method)

  val useFinder = if (useFastSplitter) {
    // TODO this logic is not correct, splitting on \d for example will yield incorrect results
    // Pig has logic for this in StorageUtil and PigStorage
    if (matchOn.charAt(0) == '\\' && matchOn.length == 2) {
      matchByte = matchOn.charAt(1).toByte
      //    matchByte = StorageUtil.parseFieldDel(matchOn)
      true
    } else if (matchOn.length == 1) {
      matchByte = matchOn.charAt(0).toByte
      //    matchByte = StorageUtil.parseFieldDel(matchOn)
      true
    } else {
      false
    }
  } else false
  method match {
    case 0 =>
      // pattern 
      pattern = Pattern.compile(matchOn)
    case 1 =>
      // initialize matcher
      runAutomaton = new RunAutomaton(new RegExp(matchOn).toAutomaton)
  }
  def split(s: String, max: Int = 0): IndexedSeq[String] = {
    if (useFinder && max > 0)
      new Finder(s, max)
    else
      method match {
        case 0 =>
          pattern.split(s, max)
        case 1 =>
          // initialize automaton
          new StringOpsOpt(runAutomaton, s).split(max)
      }
  }

  def matches(s: String): Boolean = {
    method match {
      case 0 =>
        pattern.matcher(s).matches
      case 1 =>
        runAutomaton.run(s)
    }
  }

  def replaceAll(input: String, subst: String): String = {
    method match {
      case 0 =>
        pattern.matcher(input).replaceAll(subst)
      case 1 =>
        new StringOpsOpt(runAutomaton, input).replaceAll(subst)
    }
  }

  class Finder(val s: String, val max: Int) extends IndexedSeq[String] {
    private[this] val byte = matchByte
    private[this] var at = 1
    private[this] var atChar = 0
    private[this] val ranges = Array.ofDim[Int](max)
    private[this] val ends = Array.ofDim[Int](max)
    ends(max - 1) = s.length
    ranges(0) = 0
    def apply(num: Int): String = {
      while (at <= num + 1 && at < max) {
        atChar = s.indexOf(byte, atChar)
        ends(at - 1) = atChar
        atChar += 1
        ranges(at) = atChar
        at += 1
      }
      if (at > num) {
        return s.substring(ranges(num), ends(num))
      }
      throw new RuntimeException("Did not find it")
    }
    def length = max
  }
}

class RegexFrontendVerifier(override val original: String, val o2: Boolean = true) extends RegexFrontend(original, true) {

  override def split(s: String, max: Int = 0): IndexedSeq[String] = {
    val correct = s.split(original, max)
    val optimized = super.split(s, max)
    if (correct.mkString("#########") != optimized.mkString("#########")) {
      throw new RuntimeException("split on " + matchOn + changed + " not correct for " + s + " " + max)
    }
    optimized
  }

  override def matches(s: String): Boolean = {
    val out = super.matches(s)
    if (s.matches(original) != out) {
      throw new RuntimeException("matches on " + matchOn + changed + " not correct for " + s)
    }
    return out
  }

  override def replaceAll(input: String, subst: String): String = {
    val correct = input.replaceAll(original, subst)
    val optimized = super.replaceAll(input, subst)
    if (correct != optimized) {
      throw new RuntimeException("replaceAll on " + matchOn + changed + " not correct for " + input + " " + subst)
    }
    return optimized
  }

}

import java.util.ArrayList
class StringOpsOpt(val runAutomaton: RunAutomaton, val text: CharSequence) {
  import java.lang.{ StringBuilder => JStringBuilder }
  // per input string
  val matcher = runAutomaton.newMatcher(text)

  def split(limit: Int): Array[String] = {
    var index: Int = 0
    val matchLimited = limit > 0
    val matchList = new scala.collection.mutable.ArrayBuffer[String]
    while (matcher.find) {
      if (!matchLimited || matchList.size < limit - 1) {
        val `match` = text.subSequence(index, matcher.start).toString
        matchList += (`match`)
        index = matcher.end
      } else if (matchList.size == limit - 1) {
        val `match` = text.subSequence(index, text.length).toString
        matchList += (`match`)
        index = matcher.end
      }
    }
    if (index == 0) return Array[String](text.toString)
    if (!matchLimited || matchList.size < limit) matchList += (text.subSequence(index, text.length).toString)
    var resultSize: Int = matchList.size
    if (limit == 0) while (resultSize > 0 && (matchList(resultSize - 1) == "")) ({
      resultSize -= 1;
      resultSize
    })
    matchList.take(resultSize).toArray
  }

  final def find: Boolean = {
    val out = matcher.find()
    if (out) {
      first = matcher.start
      last = matcher.end()
    } else {
      first = -1
      last = 0
    }
    out
  }

  final def start(group: Int): Int = matcher.start(group)

  final def end(group: Int): Int = matcher.end(group)

  final def groupCount: Int = matcher.groupCount

  /**
   * Resets this matcher.
   * <p/>
   * <p> Resetting a matcher discards all of its explicit state information
   * and sets its append position to zero. The matcher's region is set to the
   * default region, which is its entire character sequence. The anchoring
   * and transparency of this matcher's region boundaries are unaffected.
   *
   * @return This matcher
   */
  final def reset: Unit = {
    first = -1
    last = 0
    lastAppendPosition = 0
  }

  final def replaceAll(replacement: String): String = {
    reset
    var result: Boolean = find
    if (result) {
      val sb = new JStringBuilder
      do {
        appendReplacement(sb, replacement)
        result = find
      } while (result)
      appendTail(sb)
      return sb.toString()
    }
    text.toString
  }

  private final def appendReplacement(sb: JStringBuilder, replacement: String): Unit = {
    if (first < 0) throw new IllegalStateException("No match available")
    var cursor: Int = 0
    var result = new JStringBuilder
    while (cursor < replacement.length) {
      var nextChar: Char = replacement.charAt(cursor)
      if (nextChar == '\\') {
        cursor += 1;
        nextChar = replacement.charAt(cursor)
        result.append(nextChar)
        cursor += 1;
      } else if (nextChar == '$') {
        cursor += 1;
        var refNum: Int = replacement.charAt(cursor).asInstanceOf[Int] - '0'
        if ((refNum < 0) || (refNum > 9)) throw new IllegalArgumentException("Illegal group reference")
        cursor += 1;
        var done: Boolean = false
        breakable {
          while (!done) {
            if (cursor >= replacement.length) {
              break
            }
            var nextDigit: Int = replacement.charAt(cursor) - '0'
            if ((nextDigit < 0) || (nextDigit > 9)) {
              break
            }
            var newRefNum: Int = (refNum * 10) + nextDigit
            if (groupCount < newRefNum) {
              done = true
            } else {
              refNum = newRefNum
              cursor += 1;
            }
          }
        }
        if (start(refNum) != -1 && end(refNum) != -1) result.append(text, start(refNum), end(refNum))
      } else {
        result.append(nextChar)
        cursor += 1;
      }
    }
    sb.append(text, lastAppendPosition, first)
    sb.append(result)
    lastAppendPosition = last
  }

  private final def appendTail(sb: JStringBuilder): JStringBuilder = {
    sb.append(text, lastAppendPosition, getTextLength)
    sb
  }

  /**
   * Returns the end index of the text.
   *
   * @return the index after the last character in the text
   */
  def getTextLength: Int = text.length

  /**
   * The range of string that last matched the pattern. If the last
   * match failed then first is -1; last initially holds 0 then it
   * holds the index of the end of the last match (which is where the
   * next search starts).
   */
  private[this] var first: Int = -1
  private[this] var last: Int = 0
  /**
   * The index of the last position appended in a substitution.
   */
  private[this] var lastAppendPosition: Int = 0
}

