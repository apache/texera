/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.core.storage

import edu.uci.ics.amber.core.storage.FileResolver.DATASET_FILE_URI_SCHEME
import edu.uci.ics.amber.core.storage.model._
import edu.uci.ics.amber.core.storage.VFSResourceType._
import edu.uci.ics.amber.core.storage.VFSURIFactory.{VFS_FILE_URI_SCHEME, decodeURI}
import edu.uci.ics.amber.core.storage.result.iceberg.IcebergDocument
import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.util.IcebergUtil
import org.apache.iceberg.data.Record
import org.apache.iceberg.{Schema => IcebergSchema}

import java.net.URI

object DocumentFactory {

  val MONGODB = "mongodb"
  val ICEBERG = "iceberg"

  private def sanitizeURIPath(uri: URI): String =
    uri.getPath.stripPrefix("/").replace("/", "_")

  /**
    * Open a document specified by the uri for read purposes only.
    * @param fileUri the uri of the document
    * @return ReadonlyVirtualDocument
    */
  def openReadonlyDocument(fileUri: URI): ReadonlyVirtualDocument[_] = {
    fileUri.getScheme match {
      case DATASET_FILE_URI_SCHEME => new DatasetFileDocument(fileUri)
      case "file"                  => new ReadonlyLocalFileDocument(fileUri)
      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for creating the ReadonlyDocument"
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
        val (_, _, _, resourceType) = decodeURI(uri)
        val storageKey = sanitizeURIPath(uri)

        val namespace = resourceType match {
          case RESULT             => StorageConfig.icebergTableResultNamespace
          case CONSOLE_MESSAGES   => StorageConfig.icebergTableConsoleMessagesNamespace
          case RUNTIME_STATISTICS => StorageConfig.icebergTableRuntimeStatisticsNamespace
          case _ =>
            throw new IllegalArgumentException(s"Resource type $resourceType is not supported")
        }

        StorageConfig.resultStorageMode.toLowerCase match {
          case ICEBERG =>
            val icebergSchema = IcebergUtil.toIcebergSchema(schema)
            IcebergUtil.createTable(
              IcebergCatalogInstance.getInstance(),
              namespace,
              storageKey,
              icebergSchema,
              overrideIfExists = true
            )
            val serde: (IcebergSchema, Tuple) => Record = IcebergUtil.toGenericRecord
            val deserde: (IcebergSchema, Record) => Tuple = (_, record) =>
              IcebergUtil.fromRecord(record, schema)

            new IcebergDocument[Tuple](
              namespace,
              storageKey,
              icebergSchema,
              serde,
              deserde
            )
          case unsupportedMode =>
            throw new IllegalArgumentException(
              s"Storage mode '$unsupportedMode' is not supported"
            )
        }
      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for creating the document"
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
      case DATASET_FILE_URI_SCHEME => (new DatasetFileDocument(uri), None)
      case VFS_FILE_URI_SCHEME =>
        val (_, _, _, resourceType) = decodeURI(uri)
        val storageKey = sanitizeURIPath(uri)

        val namespace = resourceType match {
          case RESULT             => StorageConfig.icebergTableResultNamespace
          case CONSOLE_MESSAGES   => StorageConfig.icebergTableConsoleMessagesNamespace
          case RUNTIME_STATISTICS => StorageConfig.icebergTableRuntimeStatisticsNamespace
          case _ =>
            throw new IllegalArgumentException(s"Resource type $resourceType is not supported")
        }

        StorageConfig.resultStorageMode.toLowerCase match {
          case ICEBERG =>
            val table = IcebergUtil
              .loadTableMetadata(
                IcebergCatalogInstance.getInstance(),
                namespace,
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
                namespace,
                storageKey,
                table.schema(),
                serde,
                deserde
              ),
              Some(amberSchema)
            )
          case mode =>
            throw new IllegalArgumentException(
              s"Storage mode '$mode' is not supported"
            )
        }
      case unsupportedScheme =>
        throw new UnsupportedOperationException(
          s"Unsupported URI scheme: $unsupportedScheme for opening the document"
        )
    }
  }
}
