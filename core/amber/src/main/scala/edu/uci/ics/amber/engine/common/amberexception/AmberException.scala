package edu.uci.ics.amber.engine.common.amberexception

class AmberException(val cause: String) extends RuntimeException with Serializable {}
