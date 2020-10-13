package engine.faulttolerance.recovery

import engine.common.ambertag.AmberTag

final case class RecoveryPacket(tag: AmberTag, generatedCount: Long, processedCount: Long)
