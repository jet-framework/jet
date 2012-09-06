package ppl.delite.benchmarking.sorting.tpch

import ppl.delite.benchmarking.sorting.tpch.util.Date

class Order (
  val o_orderkey: Long,
  val o_custkey: Long,
  val o_orderstatus: Char,
  val o_totalprice: Double,
  val o_orderdate: Date,
  val o_orderpriority: String,
  val o_clerk: String,
  val o_shippriority: Int,
  val o_comment: String
)