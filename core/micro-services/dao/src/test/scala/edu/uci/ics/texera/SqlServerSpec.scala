package edu.uci.ics.texera

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfter
import org.jooq.DSLContext

class SqlServerSpec extends AnyFlatSpec with BeforeAndAfter {

  var dslContext: DSLContext = _

  before {
    dslContext = SqlServer.createDSLContext()
  }

  "SqlServer" should "establish a connection successfully" in {
    assert(dslContext != null, "The DSLContext should not be null")
  }

  it should "execute a simple query" in {
    val result = dslContext.fetch("SELECT 1")
    assert(result.size() == 1, "The query should return one row")
    assert(result.get(0).getValue(0) == 1, "The value should be 1")
  }

}
