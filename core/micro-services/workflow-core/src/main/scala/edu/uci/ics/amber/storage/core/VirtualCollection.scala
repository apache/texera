package edu.uci.ics.amber.storage.core

import java.net.URI

/**
  * VirtualCollection provides the abstraction of managing a collection of children VirtualDocument
  */
abstract class VirtualCollection {
  def getURI: URI

  /**
    * get children documents that are directly underneath the current collection
    * @return the children documents
    */
  def getDocuments: List[VirtualDocument[_]]

  /**
    * get a child document with certain name under this collection and return
    * @param name the child document's name
    * @return the document
    */
  def getDocument(name: String): VirtualDocument[_]

  /**
    * physically remove current collection from the system. All children documents underneath will be removed
    */
  def remove(): Unit
}
