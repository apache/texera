package edu.uci.ics.amber.engine.common.error

class AmberException(val cause: String) extends RuntimeException with Serializable {}
