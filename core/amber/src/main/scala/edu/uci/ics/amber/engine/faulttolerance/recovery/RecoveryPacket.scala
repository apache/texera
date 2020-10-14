package edu.uci.ics.amber.engine.faulttolerance.recovery

import edu.uci.ics.amber.engine.common.ambertag.AmberTag

final case class RecoveryPacket(tag: AmberTag, generatedCount: Long, processedCount: Long)
