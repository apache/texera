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

package org.apache.texera.amber.util

import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{LocalFileSystem, Path, RawLocalFileSystem}
import org.apache.hadoop.util.Shell

/**
  * A Hadoop local file system that never shells out to `winutils.exe`.
  *
  * On Windows, Hadoop's default local file system implements chmod/chown by invoking
  * `%HADOOP_HOME%\bin\winutils.exe`, and every file or directory creation applies POSIX
  * permissions through that path. Without a native Hadoop installation this fails with
  * "Hadoop bin directory does not exist". POSIX permission bits carry no meaning on NTFS,
  * so these operations can be skipped safely.
  *
  * Selected via `fs.file.impl` by [[IcebergUtil]] on Windows hosts where winutils is
  * unavailable. Additionally self-gating: anywhere winutils is available — or on
  * non-Windows systems — it behaves exactly like Hadoop's default local file system.
  */
class WinutilsFreeLocalFileSystem extends LocalFileSystem(new WinutilsFreeRawLocalFileSystem)

class WinutilsFreeRawLocalFileSystem extends RawLocalFileSystem {

  private def skipPermissionOps: Boolean = Shell.WINDOWS && !Shell.hasWinutilsPath()

  override def setPermission(p: Path, permission: FsPermission): Unit =
    if (!skipPermissionOps) super.setPermission(p, permission)

  override def setOwner(p: Path, username: String, groupname: String): Unit =
    if (!skipPermissionOps) super.setOwner(p, username, groupname)
}
