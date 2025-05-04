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

package edu.uci.ics.amber.core.storage.model

import java.io.{File, FileInputStream, InputStream}
import java.net.URI

/**
  * ReadonlyLocalFileDocument provides a read-only abstraction over a local file.
  * The data type T is not required, as all iterator-related methods are unsupported
  */
private[storage] class ReadonlyLocalFileDocument(uri: URI)
    extends ReadonlyVirtualDocument[Nothing] {

  /**
    * Get the URI of the corresponding document.
    * @return the URI of the document
    */
  override def getURI: URI = uri

  /**
    * Get the file as an input stream for read operations.
    * @return InputStream to read from the file
    */
  override def asInputStream(): InputStream = new FileInputStream(new File(uri))

  /**
    * Get the file as an input stream for read operations.
    *
    * @return File object based on the URI
    */
  override def asFile(): File = new File(uri)

  override def getItem(i: Int): Nothing =
    throw new NotImplementedError("getItem is not supported for ReadonlyLocalFileDocument")

  override def get(): Iterator[Nothing] =
    throw new NotImplementedError("get is not supported for ReadonlyLocalFileDocument")

  override def getRange(from: Int, until: Int): Iterator[Nothing] =
    throw new NotImplementedError("getRange is not supported for ReadonlyLocalFileDocument")

  override def getAfter(offset: Int): Iterator[Nothing] =
    throw new NotImplementedError("getAfter is not supported for ReadonlyLocalFileDocument")

  override def getCount: Long =
    throw new NotImplementedError("getCount is not supported for ReadonlyLocalFileDocument")
}
