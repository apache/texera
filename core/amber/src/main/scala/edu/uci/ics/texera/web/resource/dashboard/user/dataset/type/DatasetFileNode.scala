package edu.uci.ics.texera.web.resource.dashboard.user.dataset.`type`

import java.nio.file.Files
import java.util
import scala.collection.mutable

// DatasetFileNode represents a unique file in dataset, its full path is in the format of:
// /ownerEmail/datasetName/versionName/fileRelativePath
// e.g. /bob@texera.com/twitterDataset/v1/california/irvine/tw1.csv
// ownerName is bob@texera.com; datasetName is twitterDataset, versionName is v1, fileRelativePath is california/irvine/tw1.csv
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
  def fromPhysicalFileNodes(
      map: Map[(String, String, String), List[PhysicalFileNode]]
  ): List[DatasetFileNode] = {
    val rootNode = new DatasetFileNode("/", "directory", null, "")
    val ownerNodes = mutable.Map[String, DatasetFileNode]()

    map.foreach {
      case ((ownerEmail, datasetName, versionName), physicalNodes) =>
        val ownerNode = ownerNodes.getOrElseUpdate(
          ownerEmail, {
            val newNode = new DatasetFileNode(ownerEmail, "directory", rootNode, ownerEmail)
            rootNode.children = Some(rootNode.getChildren :+ newNode)
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

        physicalNodes.foreach(node => addNodeToTree(versionNode, node, ownerEmail))
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
        if (Files.isDirectory(currentPhysicalNode.getAbsolutePath)) "directory" else "file"
      val fileSize =
        if (fileType == "file") Some(Files.size(currentPhysicalNode.getAbsolutePath)) else None
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
}
