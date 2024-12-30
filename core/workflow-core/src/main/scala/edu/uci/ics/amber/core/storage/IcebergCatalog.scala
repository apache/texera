package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog

object IcebergCatalog {
  private var instance: Option[Catalog] = None

  def getInstance(): Catalog = {
    instance match {
      case Some(catalog) => catalog
      case None =>
        val jdbcCatalog = IcebergUtil.createJdbcCatalog(
          "operator-result",
          StorageConfig.fileStorageDirectoryUri,
          StorageConfig.icebergCatalogUrl,
          StorageConfig.icebergCatalogUsername,
          StorageConfig.icebergCatalogPassword
        )
        instance = Some(jdbcCatalog)
        jdbcCatalog
    }
  }

  def replaceInstance(catalog: Catalog): Unit = {
    instance = Some(catalog)
  }
}
