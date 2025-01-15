package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.core.storage.model.{
  DatasetFileDocument,
  ReadonlyLocalFileDocument,
  ReadonlyVirtualDocument,
  VirtualDocument
}
import FileResolver.DATASET_FILE_URI_SCHEME
import edu.uci.ics.amber.core.storage.VFSResourceType.{MATERIALIZED_RESULT, RESULT}
import edu.uci.ics.amber.core.storage.VFSURIFactory.{VFS_FILE_URI_SCHEME, decodeURI}
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}

import java.net.URI

object DocumentFactory {

  val MONGODB: String = "mongodb"
  val ICEBERG: String = "iceberg"

  private def sanitizeURIPath(uri: URI): String = uri.getPath.stripPrefix("/").replace("/", "_")

  /**
    * Open a document specified by the uri for read purposes only.
    * @param fileUri the uri of the document
    * @return ReadonlyVirtualDocument
    */
  def openReadonlyDocument(fileUri: URI): ReadonlyVirtualDocument[_] = {
    fileUri.getScheme match {
      case DATASET_FILE_URI_SCHEME =>
        new DatasetFileDocument(fileUri)

      case "file" =>
        new ReadonlyLocalFileDocument(fileUri)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: ${fileUri.getScheme} for creating the ReadonlyDocument"
        )
    }
  }

  /**
    * Create a document for storage specified by the uri.
    * This document is suitable for storing structural data, i.e. the schema is required to create such document.
    * @param uri the location of the document
    * @param schema the schema of the data stored in the document
    * @return the created document
    */
  def createDocument(uri: URI, schema: Schema): VirtualDocument[_] = {
    uri.getScheme match {
      case VFS_FILE_URI_SCHEME =>
        val (_, _, _, _, resourceType) = decodeURI(uri)

        resourceType match {
          case RESULT | MATERIALIZED_RESULT =>
            val storageKey = sanitizeURIPath(uri)

            StorageConfig.resultStorageMode.toLowerCase match {
              case ICEBERG =>
                val icebergSchema = IcebergUtil.toIcebergSchema(schema)
                IcebergUtil.createTable(
                  IcebergCatalogInstance.getInstance(),
                  StorageConfig.icebergTableNamespace,
                  storageKey,
                  icebergSchema,
                  overrideIfExists = true
                )
                val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
                val deserde: (IcebergSchema, Record) => Tuple = (_, record) =>
                  IcebergUtil.fromRecord(record, schema)

                new IcebergDocument[Tuple](
                  StorageConfig.icebergTableNamespace,
                  storageKey,
                  icebergSchema,
                  serde,
                  deserde
                )

              case _ =>
                throw new IllegalArgumentException(
                  s"Storage mode '${StorageConfig.resultStorageMode}' is not supported"
                )
            }

          case _ =>
            throw new IllegalArgumentException(
              s"Resource type $resourceType is not supported"
            )
        }

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: ${uri.getScheme} for creating the document"
        )
    }
  }

  /**
    * Open a document specified by the uri.
    * If the document is storing structural data, the schema will also be returned
    * @param uri the uri of the document
    * @return the VirtualDocument, which is the handler of the data; the Schema, which is the schema of the data stored in the document
    */
  def openDocument(uri: URI): (VirtualDocument[_], Option[Schema]) = {
    uri.getScheme match {
      case DATASET_FILE_URI_SCHEME =>
        (new DatasetFileDocument(uri), None)

      case VFS_FILE_URI_SCHEME =>
        val (_, _, _, _, resourceType) = decodeURI(uri)

        resourceType match {
          case RESULT | MATERIALIZED_RESULT =>
            val storageKey = sanitizeURIPath(uri)

            StorageConfig.resultStorageMode.toLowerCase match {
              case ICEBERG =>
                val table = IcebergUtil
                  .loadTableMetadata(
                    IcebergCatalogInstance.getInstance(),
                    StorageConfig.icebergTableNamespace,
                    storageKey
                  )
                  .getOrElse(
                    throw new IllegalArgumentException("No storage is found for the given URI")
                  )

                val amberSchema = IcebergUtil.fromIcebergSchema(table.schema())
                val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
                val deserde: (IcebergSchema, Record) => Tuple = (_, record) =>
                  IcebergUtil.fromRecord(record, amberSchema)

                (
                  new IcebergDocument[Tuple](
                    StorageConfig.icebergTableNamespace,
                    storageKey,
                    table.schema(),
                    serde,
                    deserde
                  ),
                  Some(amberSchema)
                )

              case _ =>
                throw new IllegalArgumentException(
                  s"Storage mode '${StorageConfig.resultStorageMode}' is not supported"
                )
            }

          case _ =>
            throw new IllegalArgumentException(
              s"Resource type $resourceType is not supported"
            )
        }

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: ${uri.getScheme} for opening the document"
        )
    }
  }
}
