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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class WinutilsFreeLocalFileSystemSpec extends AnyFlatSpec {

  private def newFs(): FileSystem = {
    val conf = new Configuration()
    // Select the file system the same way production code does: through `fs.file.impl`.
    conf.set("fs.file.impl", classOf[WinutilsFreeLocalFileSystem].getName)
    conf.setBoolean("fs.file.impl.disable.cache", true)
    FileSystem.get(URI.create("file:///"), conf)
  }

  "WinutilsFreeLocalFileSystem" should "be selected through fs.file.impl" in {
    assert(newFs().isInstanceOf[WinutilsFreeLocalFileSystem])
  }

  it should "create directories and read/write files without winutils" in {
    val fs = newFs()
    val tmp = Files.createTempDirectory("winutils-free-fs")
    val dir = new Path(tmp.toUri.toString, "a/b/c")
    assert(fs.mkdirs(dir))

    val file = new Path(dir, "data.txt")
    val out = fs.create(file, true)
    out.write("hello".getBytes(StandardCharsets.UTF_8))
    out.close()

    val in = fs.open(file)
    val buf = new Array[Byte](5)
    in.readFully(buf)
    in.close()
    assert(new String(buf, StandardCharsets.UTF_8) == "hello")

    assert(fs.getFileStatus(file).getLen == 5)
    assert(fs.delete(file, false))
  }

  it should "not fail on setPermission" in {
    val fs = newFs()
    val tmp = Files.createTempDirectory("winutils-free-fs-perm")
    val file = new Path(tmp.toUri.toString, "perm.txt")
    fs.create(file, true).close()
    // No-op on Windows hosts without winutils; delegates to Hadoop's default elsewhere.
    fs.setPermission(file, new FsPermission("755"))
  }

  it should "not fail on setOwner" in {
    val fs = newFs()
    val tmp = Files.createTempDirectory("winutils-free-fs-owner")
    val file = new Path(tmp.toUri.toString, "owner.txt")
    fs.create(file, true).close()
    // Chown-to-self is permitted everywhere; no-op on Windows hosts without winutils.
    // (Not FileStatus.getOwner: reading the owner itself requires winutils on Windows.)
    fs.setOwner(file, System.getProperty("user.name"), null)
  }
}
