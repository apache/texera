package edu.uci.ics.texera.service.util

import scala.math.pow

// Can use enums in Scala 3
sealed abstract class CpuUnit(val nanocores: Double)
object CpuUnit {
  case object Nanocores extends CpuUnit(1.0)
  case object Millicores extends CpuUnit(1e6)
  case object Cores extends CpuUnit(1e9)

  def convert(value: Double, from: CpuUnit, to: CpuUnit): Double = {
    value * (from.nanocores / to.nanocores)
  }
}

sealed abstract class MemoryUnit(val bytes: Double)
object MemoryUnit {
  case object Byte extends MemoryUnit(1)
  case object Kibibyte extends MemoryUnit(1024)
  case object Mebibyte extends MemoryUnit(pow(1024, 2).intValue)
  case object Gibibyte extends MemoryUnit(pow(1024, 3).intValue)

  def convert(value: Double, from: MemoryUnit, to: MemoryUnit): Double = {
    value * (from.bytes / to.bytes)
  }
}
