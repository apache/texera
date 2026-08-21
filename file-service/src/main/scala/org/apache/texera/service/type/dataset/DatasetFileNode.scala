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

package org.apache.texera.service.`type`

import io.lakefs.clients.sdk.model.ObjectStats
import org.apache.texera.amber.core.storage.ResourceType

import scala.collection.mutable

// DatasetFileNode represents a unique file in dataset, its full path is in the format of:
// /dataset/ownerEmail/datasetName/versionName/fileRelativePath
// e.g. /dataset/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
class DatasetFileNode(
    val name: String, // direct name of this node
    val nodeType: String, // "file" or "directory"
    val parent: DatasetFileNode, // the parent node
    val ownerEmail: String,
    val size: Option[Long] = None, // size of the file in bytes, None if directory
    var children: Option[List[DatasetFileNode]] = None // Only populated if 'type' is 'directory'
) {

  // Ensure the type is either "file" or "directory"
  require(nodeType == "file" || nodeType == "directory", "type must be 'file' or 'directory'")

  // Getters for the parameters
  def getName: String = name

  def getNodeType: String = nodeType

  def getParent: DatasetFileNode = parent

  def getOwnerEmail: String = ownerEmail

  def getSize: Option[Long] = size

  def getChildren: List[DatasetFileNode] = children.getOrElse(List())

  // Method to get the full file path
  def getFilePath: String = {
    val pathComponents = new mutable.ArrayBuffer[String]()
    var currentNode: DatasetFileNode = this
    while (currentNode != null) {
      if (currentNode.parent != null) { // Skip the root node to avoid double slashes
        pathComponents.prepend(currentNode.name)
      }
      currentNode = currentNode.parent
    }
    "/" + pathComponents.mkString("/")
  }
}

object DatasetFileNode {

  /**
    * Converts a map of LakeFS committed objects into a structured dataset file node tree.
    *
    * @param map A mapping from `(ownerEmail, datasetName, versionName)` to a list of committed objects.
    * @return A list of root-level dataset file nodes.
    */
  def fromLakeFSRepositoryCommittedObjects(
      map: Map[(String, String, String), List[ObjectStats]]
  ): List[DatasetFileNode] = {
    val rootNode = new DatasetFileNode("/", "directory", null, "")

    // Root the tree at the dataset prefix node (a directory node named "dataset").
    val prefixNode =
      new DatasetFileNode(ResourceType.Dataset.toString, "directory", rootNode, "")
    rootNode.children = Some(List(prefixNode))

    // Owner level nodes map
    val ownerNodes = mutable.Map[String, DatasetFileNode]()

    map.foreach {
      case ((ownerEmail, datasetName, versionName), objects) =>
        val ownerNode = ownerNodes.getOrElseUpdate(
          ownerEmail, {
            val newNode = new DatasetFileNode(ownerEmail, "directory", prefixNode, ownerEmail)
            prefixNode.children = Some(prefixNode.getChildren :+ newNode)
            newNode
          }
        )

        val datasetNode = ownerNode.getChildren.find(_.getName == datasetName).getOrElse {
          val newNode = new DatasetFileNode(datasetName, "directory", ownerNode, ownerEmail)
          ownerNode.children = Some(ownerNode.getChildren :+ newNode)
          newNode
        }

        val versionNode = datasetNode.getChildren.find(_.getName == versionName).getOrElse {
          val newNode = new DatasetFileNode(versionName, "directory", datasetNode, ownerEmail)
          datasetNode.children = Some(datasetNode.getChildren :+ newNode)
          newNode
        }

        // Directory map for efficient lookups
        val directoryMap = mutable.Map[String, DatasetFileNode]()
        directoryMap("") = versionNode // Root of the dataset version

        // Process each object (file or directory) from LakeFS
        objects.foreach { obj =>
          val pathParts = obj.getPath.split("/").toList
          var currentPath = ""
          var parentNode: DatasetFileNode = versionNode

          pathParts.foreach { part =>
            currentPath = if (currentPath.isEmpty) part else s"$currentPath/$part"

            val isFile = pathParts.last == part
            val nodeType = if (isFile) "file" else "directory"
            val fileSize = if (isFile) Some(obj.getSizeBytes.longValue()) else None

            val existingNode = directoryMap.get(currentPath)

            val node = existingNode.getOrElse {
              val newNode = new DatasetFileNode(part, nodeType, parentNode, ownerEmail, fileSize)
              parentNode.children = Some(parentNode.getChildren :+ newNode)
              if (!isFile) directoryMap(currentPath) = newNode
              newNode
            }

            parentNode = node // Move parent reference deeper for next iteration
          }
        }
    }

    // Sorting function to sort children of a node alphabetically in descending order
    def sortChildren(node: DatasetFileNode): Unit = {
      node.children = Some(node.getChildren.sortBy(_.getName)(Ordering.String.reverse))
      node.getChildren.foreach(sortChildren)
    }

    // Apply the sorting to the root node
    sortChildren(rootNode)

    rootNode.getChildren
  }

  /**
    * Traverses a given list of DatasetFileNode and returns the total size of all files.
    *
    * @param nodes List of root-level DatasetFileNode.
    * @return Total size in bytes.
    */
  def calculateTotalSize(nodes: List[DatasetFileNode]): Long = {
    def traverse(node: DatasetFileNode): Long = {
      val fileSize = node.getSize.getOrElse(0L)
      val childrenSize = node.getChildren.map(traverse).sum
      fileSize + childrenSize
    }

    nodes.map(traverse).sum
  }
}
