package edu.uci.ics.amber.engine.common.storage

/**
  * TexeraCollection provides the abstraction of managing a collection of children TexeraDocument
  * - TexeraCollection is identified uniquely by the URI
  */
abstract class TexeraCollection {
  def getURI: TexeraURI

  /**
    * get children documents that are directly underneath the current TexeraCollection
    * @return the children documents
    */
  def getDocuments: List[TexeraDocument[_]]

  /**
    * get a child document with certain name under this collection and return
    * @param name the child document's name
    * @return the document
    */
  def getDocument(name: String): TexeraDocument[_]

  /**
    * physically remove current collection from the system. All children documents underneath will be removed
    */
  def rm(): Unit
}
