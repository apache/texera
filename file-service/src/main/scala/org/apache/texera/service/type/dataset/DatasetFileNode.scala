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
import org.apache.texera.amber.core.storage.util.dataset.PhysicalFileNode

import java.util
import scala.collection.mutable

// DatasetFileNode represents a unique file in a versioned resource. Its full path is in the
// format of: /<resourceType>/ownerEmail/resourceName/versionName/fileRelativePath
// e.g. /datasets/bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
//      /models/bob@texera.com/resnet/v1/weights/model.pt
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
    * Builds the `/<resourceType>/ownerEmail/resourceName/versionName` skeleton that every tree
    * shares, and returns the root together with the version node each key hangs its files from.
    */
  private def buildVersionSkeleton(
      resourceType: ResourceType.Value,
      keys: Iterable[(String, String, String)]
  ): (DatasetFileNode, Map[(String, String, String), DatasetFileNode]) = {
    val rootNode = new DatasetFileNode("/", "directory", null, "")

    // The prefix node names the resource kind, e.g. a directory node named "datasets".
    val prefixNode = new DatasetFileNode(resourceType.toString, "directory", rootNode, "")
    rootNode.children = Some(List(prefixNode))

    val ownerNodes = mutable.Map[String, DatasetFileNode]()
    val versionNodes = mutable.Map[(String, String, String), DatasetFileNode]()

    keys.foreach { key =>
      val (ownerEmail, resourceName, versionName) = key

      val ownerNode = ownerNodes.getOrElseUpdate(
        ownerEmail, {
          val newNode = new DatasetFileNode(ownerEmail, "directory", prefixNode, ownerEmail)
          prefixNode.children = Some(prefixNode.getChildren :+ newNode)
          newNode
        }
      )

      val resourceNode = ownerNode.getChildren.find(_.getName == resourceName).getOrElse {
        val newNode = new DatasetFileNode(resourceName, "directory", ownerNode, ownerEmail)
        ownerNode.children = Some(ownerNode.getChildren :+ newNode)
        newNode
      }

      val versionNode = resourceNode.getChildren.find(_.getName == versionName).getOrElse {
        val newNode = new DatasetFileNode(versionName, "directory", resourceNode, ownerEmail)
        resourceNode.children = Some(resourceNode.getChildren :+ newNode)
        newNode
      }

      versionNodes(key) = versionNode
    }

    (rootNode, versionNodes.toMap)
  }

  // Sorts children of every node alphabetically in descending order
  private def sortChildrenRecursively(node: DatasetFileNode): Unit = {
    node.children = Some(node.getChildren.sortBy(_.getName)(Ordering.String.reverse))
    node.getChildren.foreach(sortChildrenRecursively)
  }

  /**
    * Converts a map of LakeFS committed objects into a structured file node tree.
    *
    * @param map A mapping from `(ownerEmail, resourceName, versionName)` to a list of committed objects.
    * @param resourceType The resource kind the paths belong to; becomes the tree's leading segment.
    * @return A list of root-level file nodes.
    */
  def fromLakeFSRepositoryCommittedObjects(
      map: Map[(String, String, String), List[ObjectStats]],
      resourceType: ResourceType.Value
  ): List[DatasetFileNode] = {
    val (rootNode, versionNodes) = buildVersionSkeleton(resourceType, map.keys)

    map.foreach {
      case (key, objects) =>
        val ownerEmail = key._1
        val versionNode = versionNodes(key)

        // Directory map for efficient lookups
        val directoryMap = mutable.Map[String, DatasetFileNode]()
        directoryMap("") = versionNode // Root of the version

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

    sortChildrenRecursively(rootNode)

    rootNode.getChildren
  }

  /** Physical (on-disk) file trees exist only for datasets. */
  def fromPhysicalFileNodes(
      map: Map[(String, String, String), List[PhysicalFileNode]]
  ): List[DatasetFileNode] = {
    val (rootNode, versionNodes) = buildVersionSkeleton(ResourceType.Datasets, map.keys)

    map.foreach {
      case (key, physicalNodes) =>
        physicalNodes.foreach(node => addNodeToTree(versionNodes(key), node, key._1))
    }

    sortChildrenRecursively(rootNode)

    rootNode.getChildren
  }

  private def addNodeToTree(
      parentNode: DatasetFileNode,
      physicalNode: PhysicalFileNode,
      ownerEmail: String
  ): Unit = {
    val queue = new util.LinkedList[(DatasetFileNode, PhysicalFileNode)]()
    queue.add((parentNode, physicalNode))

    while (!queue.isEmpty) {
      val (currentParent, currentPhysicalNode) = queue.poll()
      val relativePath = currentPhysicalNode.getRelativePath.toString.split("/").toList
      val nodeName = relativePath.last

      val fileType =
        if (currentPhysicalNode.isDirectory) "directory" else "file"
      val fileSize =
        if (fileType == "file") Some(currentPhysicalNode.getSize) else None
      val existingNode = currentParent.getChildren.find(child =>
        child.getName == nodeName && child.getNodeType == fileType
      )
      val fileNode = existingNode.getOrElse {
        val newNode = new DatasetFileNode(
          nodeName,
          fileType,
          currentParent,
          ownerEmail,
          fileSize
        )
        currentParent.children = Some(currentParent.getChildren :+ newNode)
        newNode
      }

      // Add children of the current physical node to the queue
      currentPhysicalNode.getChildren.forEach(child => queue.add((fileNode, child)))
    }
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
