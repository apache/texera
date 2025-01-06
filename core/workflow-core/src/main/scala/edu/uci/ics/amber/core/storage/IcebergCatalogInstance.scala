package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.catalog.Catalog

/**
  * IcebergCatalogInstance is a singleton that manages the Iceberg catalog instance.
  * - Provides a single shared catalog for all Iceberg table-related operations in the Texera application.
  * - Lazily initializes the catalog on first access.
  * - Supports replacing the catalog instance primarily for testing or reconfiguration.
  */
object IcebergCatalogInstance {

  private var instance: Option[Catalog] = None

  /**
    * Retrieves the singleton Iceberg catalog instance.
    * - If the catalog is not initialized, it is lazily created using the configured properties.
    * @return the Iceberg catalog instance.
    */
  def getInstance(): Catalog = {
    instance match {
      case Some(catalog) => catalog
      case None =>
        val hadoopCatalog = IcebergUtil.createHadoopCatalog(
          "texera_iceberg",
          StorageConfig.fileStorageDirectoryPath
        )
        instance = Some(hadoopCatalog)
        hadoopCatalog
    }
  }

  /**
    * Replaces the existing Iceberg catalog instance.
    * - This method is useful for testing or dynamically updating the catalog.
    *
    * @param catalog the new Iceberg catalog instance to replace the current one.
    */
  def replaceInstance(catalog: Catalog): Unit = {
    instance = Some(catalog)
  }
}
