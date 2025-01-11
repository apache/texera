package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.core.storage.model.{
  DatasetFileDocument,
  ReadonlyLocalFileDocument,
  ReadonlyVirtualDocument,
  VirtualDocument
}
import FileResolver.{DATASET_FILE_URI_SCHEME, VFS_FILE_URI_SCHEME, decodeVFSUri}
import edu.uci.ics.amber.core.storage.VFSResourceType.{RESULT, MATERIALIZED_RESULT}
import edu.uci.ics.amber.core.storage.result.MongoDocument
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}

import java.net.URI

object DocumentFactory {

  val MONGODB: String = "mongodb"
  val ICEBERG: String = "iceberg"

  def newReadonlyDocument(fileUri: URI): ReadonlyVirtualDocument[_] = {
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

  def createDocument(uri: URI, schema: Schema): VirtualDocument[_] = {
    uri.getScheme match {
      case VFS_FILE_URI_SCHEME =>
        val uriComponents = decodeVFSUri(uri)

        uriComponents.resourceType match {
          case RESULT | MATERIALIZED_RESULT =>
            val prefix =
              if (uriComponents.resourceType == MATERIALIZED_RESULT) "materialized_" else ""
            val storageKey = prefix + uri.getPath.stripPrefix("/").replace("/", "_")

            StorageConfig.resultStorageMode.toLowerCase match {
              case MONGODB =>
                new MongoDocument[Tuple](
                  storageKey,
                  Tuple.toDocument,
                  Tuple.fromDocument(schema)
                )

              case ICEBERG | _ =>
                val icebergSchema = IcebergUtil.toIcebergSchema(schema)
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
            }

          case _ =>
            throw new IllegalArgumentException(
              s"Resource type ${uriComponents.resourceType} is not supported"
            )
        }

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: ${uri.getScheme} for creating the document"
        )
    }
  }
}
