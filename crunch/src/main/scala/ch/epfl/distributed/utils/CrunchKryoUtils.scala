package ch.epfl.distributed.utils {

  import com.cloudera.crunch.`type`.writable.KryoWritableType
  trait KryoFormat
}

package com.cloudera.crunch.`type`.writable {

  import ch.epfl.distributed.utils.KryoFormat
  import org.apache.hadoop.io.Writable
  import com.cloudera.crunch.MapFn
  import org.apache.hadoop.io.BytesWritable

  class KryoWritableType[T <: KryoFormat: Manifest, W <: Writable: Manifest](val in: MapFn[W, T], val out: MapFn[T, W])
      extends WritableType[T, W](manifest[T].erasure.asInstanceOf[Class[T]], manifest[W].erasure.asInstanceOf[Class[W]],
        in, out) {

  }

}