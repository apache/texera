package engine.common.amberexception

class AmberException(val cause: String) extends RuntimeException with Serializable {}
