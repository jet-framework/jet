package ch.epfl.distributed.utils

import com.cloudera.crunch.CombineFn.Aggregator
import java.util.Collections
import org.apache.hadoop.io.Writable
import java.io.DataOutput
import java.io.DataInput
import com.cloudera.crunch.PTable
import com.cloudera.crunch.{ Pair => CPair }
import com.cloudera.crunch.DoFn
import com.cloudera.crunch.`type`.writable.Writables
import org.apache.hadoop.conf.Configuration
import com.cloudera.crunch.Emitter
import scala.collection.mutable
import com.cloudera.crunch.{ MapFn, CombineFn }
import com.cloudera.crunch.`type`.writable.WritableType
import org.apache.hadoop.mapreduce.Partitioner
import com.cloudera.crunch.GroupingOptions

class CombineWrapper[K, V](reduce: Function2[V, V, V]) extends CombineFn[K, V] {

  @Override
  def process(input: CPair[K, java.lang.Iterable[V]], emitter: Emitter[CPair[K, V]]) {
    val it = input.second.iterator
    if (it.hasNext) {
      var accum: V = it.next
      while (it.hasNext) {
        accum = reduce(accum, it.next)
      }
      emitter.emit(CPair.of(input.first, accum))
    }
  }
}

object PartitionerUtil {
  type IntType = java.lang.Integer
  trait ClosurePartitioner[K] extends Partitioner[K, Any] {
    def getPartition(key: K, value: Any, numPartitions: Int): Int = {
      f(key, numPartitions)
    }
    def f: (K, IntType) => IntType
  }
  def makeGroupingOptions[C <: ClosurePartitioner[_]: Manifest] = {
    val builder = new GroupingOptions.Builder()
    builder.partitionerClass(manifest[C].erasure.asSubclass(classOf[Partitioner[_, _]]))
    builder.build
  }
}

object JoinHelper {
  import com.cloudera.crunch.{ Tuple3 => CTuple3 }

  def joinNotNull[K, U: Manifest, V: Manifest](left: PTable[K, U], right: PTable[K, V]): PTable[K, CPair[U, V]] = {
    type TV = CPair[U, V]
    val ptf = left.getTypeFamily();
    val ptt = ptf.tableOf(left.getKeyType(), Writables.pairs(left.getValueType, right.getValueType));
    val j1 = left.parallelDo(new DoFn[CPair[K, U], CPair[K, TV]] {
      def process(x: CPair[K, U], emitter: Emitter[CPair[K, TV]]) {
        emitter.emit(CPair.of(x.first, CPair.of(x.second, null.asInstanceOf[V])))
      }
    }, ptt)

    val j2 = right.parallelDo(new DoFn[CPair[K, V], CPair[K, TV]] {
      def process(x: CPair[K, V], emitter: Emitter[CPair[K, TV]]) {
        emitter.emit(CPair.of(x.first, CPair.of(null.asInstanceOf[U], x.second)))
      }
    }, ptt)

    val joined = j1.union(j2)
    val joinedGrouped = joined.groupByKey
    joinedGrouped.parallelDo(
      new DoFn[CPair[K, java.lang.Iterable[TV]], CPair[K, CPair[U, V]]] {
        var left: mutable.Buffer[U] = null
        var right: mutable.Buffer[V] = null
        override def configure(conf: Configuration) {
          left = mutable.Buffer[U]()
          right = mutable.Buffer[V]()
        }
        def process(input: CPair[K, java.lang.Iterable[TV]], emitter: Emitter[CPair[K, CPair[U, V]]]) {
          val it = input.second().iterator
          while (it.hasNext) {
            val tv = it.next()
            if (tv.first != null)
              left += tv.first
            else
              right += tv.second
          }
          for (l <- left; r <- right) {
            val p1 = CPair.of(l, r)
            val p2 = CPair.of(input.first, p1)
            emitter.emit(p2)
          }
          left.clear()
          right.clear()
        }
      },
      Writables.tableOf(left.getKeyType, Writables.pairs(left.getValueType, right.getValueType)))
  }

  def join[K, U: Manifest, V: Manifest](left: PTable[K, U], right: PTable[K, V]): PTable[K, CPair[U, V]] = {
    type TV = CTuple3[java.lang.Boolean, U, V]
    val ptf = left.getTypeFamily();
    val ptt = ptf.tableOf(left.getKeyType(), Writables.triples(Writables.booleans, left.getValueType, right.getValueType));
    val j1 = left.parallelDo(new DoFn[CPair[K, U], CPair[K, TV]] {
      def process(x: CPair[K, U], emitter: Emitter[CPair[K, TV]]) {
        emitter.emit(CPair.of(x.first, CTuple3.of(true, x.second, null.asInstanceOf[V])))
      }
    }, ptt)

    val j2 = right.parallelDo(new DoFn[CPair[K, V], CPair[K, TV]] {
      def process(x: CPair[K, V], emitter: Emitter[CPair[K, TV]]) {
        emitter.emit(CPair.of(x.first, CTuple3.of(true, null.asInstanceOf[U], x.second)))
      }
    }, ptt)

    val joined = j1.union(j2)
    val joinedGrouped = joined.groupByKey
    joinedGrouped.parallelDo(
      new DoFn[CPair[K, java.lang.Iterable[TV]], CPair[K, CPair[U, V]]] {
        var left: mutable.Buffer[U] = null
        var right: mutable.Buffer[V] = null
        override def configure(conf: Configuration) {
          left = mutable.Buffer[U]()
          right = mutable.Buffer[V]()
        }
        def process(input: CPair[K, java.lang.Iterable[TV]], emitter: Emitter[CPair[K, CPair[U, V]]]) {
          val it = input.second().iterator
          while (it.hasNext) {
            val tv = it.next()
            if (tv.first)
              left += tv.second
            else
              right += tv.third
          }
          for (l <- left; r <- right) {
            val p1 = CPair.of(l, r)
            val p2 = CPair.of(input.first, p1)
            emitter.emit(p2)
          }
          left.clear()
          right.clear()
        }
      },
      Writables.tableOf(left.getKeyType, Writables.pairs(left.getValueType, right.getValueType)))
  }

  def joinWritables[TV <: TaggedValue[U, V], K, U <: Writable, V <: Writable](c: Class[TV], left: PTable[K, U], right: PTable[K, V], reducers: Int = -1): PTable[K, CPair[U, V]] = {
    val ptf = left.getTypeFamily();
    val ptt = ptf.tableOf(left.getKeyType(), Writables.records(c));

    val j1 = left.parallelDo(new DoFn[CPair[K, U], CPair[K, TV]] {
      var tv: TaggedValue[U, V] = null
      override def configure(conf: Configuration) {
        tv = c.newInstance
        tv.left = true
      }
      def process(x: CPair[K, U], emitter: Emitter[CPair[K, TV]]) {
        tv.v1 = x.second
        emitter.emit(CPair.of(x.first, tv.asInstanceOf[TV]))
      }
    }, ptt)

    val j2 = right.parallelDo(new DoFn[CPair[K, V], CPair[K, TV]] {
      var tv: TaggedValue[U, V] = null
      override def configure(conf: Configuration) {
        tv = c.newInstance
        tv.left = false
      }
      def process(x: CPair[K, V], emitter: Emitter[CPair[K, TV]]) {
        tv.v2 = x.second
        emitter.emit(CPair.of(x.first, tv.asInstanceOf[TV]))
      }
    }, ptt)

    val joined = j1.union(j2)
    val joinedGrouped =
      if (reducers <= 0)
        joined.groupByKey
      else
        joined.groupByKey(reducers)
    joinedGrouped.parallelDo(
      new DoFn[CPair[K, java.lang.Iterable[TV]], CPair[K, CPair[U, V]]] {
        var left: mutable.Buffer[U] = null
        var right: mutable.Buffer[V] = null
        override def configure(conf: Configuration) {
          left = mutable.Buffer[U]()
          right = mutable.Buffer[V]()
        }
        def process(input: CPair[K, java.lang.Iterable[TV]], emitter: Emitter[CPair[K, CPair[U, V]]]) {

          val it = input.second().iterator
          while (it.hasNext) {
            val tv = it.next()
            if (tv.left)
              left += tv.v1
            else
              right += tv.v2
          }
          for (l <- left; r <- right) {
            val p1 = CPair.of(l, r)
            val p2 = CPair.of(input.first, p1)
            emitter.emit(p2)
          }
          left.clear()
          right.clear()
        }
      },
      Writables.tableOf(left.getKeyType, Writables.pairs(left.getValueType, right.getValueType)))
  }
}

case class TaggedValue[V1 <: Writable, V2 <: Writable](var left: Boolean, var v1: V1, var v2: V2) extends Writable {
  override def readFields(in: DataInput) {
    left = in.readBoolean
    if (left) {
      v1.readFields(in)
    } else {
      v2.readFields(in)
    }
  }
  override def write(out: DataOutput) {
    out.writeBoolean(left)
    if (left) {
      v1.write(out)
    } else {
      v2.write(out)
    }
  }
}
