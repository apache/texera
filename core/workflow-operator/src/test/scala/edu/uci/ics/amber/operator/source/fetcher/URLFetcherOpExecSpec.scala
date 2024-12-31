package edu.uci.ics.amber.operator.source.fetcher

import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
class URLFetcherOpExecSpec extends AnyFlatSpec with BeforeAndAfter {

  val resultSchema: Schema = new URLFetcherOpDesc().sourceSchema()

  val opDesc: URLFetcherOpDesc = new URLFetcherOpDesc()

  it should "fetch url and output one tuple with raw bytes" in {
    opDesc.url = "https://www.google.com"
    opDesc.decodingMethod = DecodingMethod.RAW_BYTES
    val fetcherOpExec = new URLFetcherOpExec(objectMapper.writeValueAsString(opDesc))
    val iterator = fetcherOpExec.produceTuple()
    assert(iterator.next().getFields.toList.head.isInstanceOf[Array[Byte]])
    assert(!iterator.hasNext)
  }

  it should "fetch url and output one tuple with UTF-8 string" in {
    opDesc.url = "https://www.google.com"
    opDesc.decodingMethod = DecodingMethod.UTF_8
    val fetcherOpExec = new URLFetcherOpExec(objectMapper.writeValueAsString(opDesc))
    val iterator = fetcherOpExec.produceTuple()
    assert(iterator.next().getFields.toList.head.isInstanceOf[String])
    assert(!iterator.hasNext)
  }

}
