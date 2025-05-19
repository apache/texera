package edu.uci.ics.texera.web

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.AmberConfig

object ConfigurationValidator extends LazyLogging {

  def validateConfiguration(): Unit = {
    if (AmberConfig.appDomain.isEmpty) {
      logger.warn(
        s"""
           |=======================================================
           |[WARN] The app.domain is not configured, and the "Sent from:" field in the email will not include a domain!
           |Please configure app.domain=your.domain.com or set the environment variable/system property APP_DOMAIN.
           |=======================================================
           |""".stripMargin
      )
    }
  }
}
