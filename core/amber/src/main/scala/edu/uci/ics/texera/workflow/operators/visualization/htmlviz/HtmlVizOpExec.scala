package edu.uci.ics.texera.workflow.operators.visualization.htmlviz

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}

import scala.collection.Iterator
import scala.util.Either

/**
 * HTML Visualization operator to render any given HTML code
 *
 */
class HtmlVizOpExec extends OperatorExecutor {

  override def open(): Unit = {
  }

  override def close(): Unit = {
  }

  override def processTexeraTuple(tuple: Either[Tuple, InputExhausted], input: LinkIdentity): Iterator[Tuple] =
    tuple match {
      case Left(t) => Iterator()
      case Right(_) =>
        //read file
        val file = scala.io.Source.fromFile("/Users/sadeem/Downloads/50topics.html")
        val content = try file.mkString finally file.close()
        //then build a tuple

        val t = Tuple
          .newBuilder()
          .add( "HTML_content", AttributeType.STRING, content).build()
        //then return iterator t
        Iterator(t)
    }
}