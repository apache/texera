package Engine.FaultTolerance.Recovery

import Engine.Common.AmberTag.AmberTag

final case class RecoveryPacket(tag: AmberTag, generatedCount: Long, processedCount: Long)
