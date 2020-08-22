package Engine.Common.AmberException

class AmberException(val cause: String) extends RuntimeException with Serializable {}
