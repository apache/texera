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

package org.apache.texera.amber.operator

import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

trait StandaloneCodeGenerator {

  /**
    * The Python this operator contributes to an exported script.
    *
    * Frames are named by placeholder rather than outright: `in1df`, `in2df` and
    * so on for what the operator reads, `out1df` and so on for what it writes.
    * A file the operator writes is named the same way, `outputHtml` or
    * `outputJson`. The translator puts the names it assigned in their place.
    */
  def generateStandaloneCode(): String

  /**
    * The same Python, for an operator that has to know what the columns it reads
    * are DECLARED as. See [[renderedAsText]] for what the file cannot carry.
    * Defaults to the schema-free form.
    */
  def generateStandaloneCode(inputSchemas: Map[PortIdentity, Schema]): String =
    generateStandaloneCode()

  /**
    * A column's name as the input table spells it.
    *
    * The engine looks a column up ignoring case, `Schema` lowercasing the name, so a
    * setting that names `Name` reads the column `name`. pandas indexes a frame by the
    * exact name. A name the schema does not hold, or no schema at all, comes back as
    * written, leaving the script to fail where the run would have.
    */
  protected def columnAsStored(column: String, schema: Option[Schema]): String =
    schema
      .flatMap(s => scala.util.Try(s.getAttribute(column)).toOption)
      .map(_.getName)
      .getOrElse(column)

  /**
    * A column as the text the engine's `toString` would have produced.
    *
    * A hole costs the column its type: pandas reads a holed integer column as a
    * float and a holed boolean one as 1.0 and 0.0, so 6 renders as "6.0" and true
    * as "1.0". Only the declared type can say which was meant, a real DOUBLE
    * holding 6.0 looking the same. `None` reads the column as it arrives.
    *
    * The caller declares [[StandaloneHelpers.AttributeCasts]] itself; doing it
    * here would emit the helper into every script.
    */
  protected def renderedAsText(column: String, declared: Option[AttributeType]): String = {
    val narrowed = declared match {
      case Some(AttributeType.INTEGER) | Some(AttributeType.LONG) =>
        s"""$column.astype("Int64")"""
      case Some(AttributeType.BOOLEAN) => s"""$column.astype("boolean")"""
      case _                           => column
    }
    s"""_texera_cast_string($narrowed)"""
  }

  /**
    * The file this operator reads, as Texera resolved it, or None for one that
    * reads no file.
    *
    * The script cannot open a resolved URI, so the body writes
    * [[StandaloneCodeGenerator.SourceFilePlaceholder]] where the file should be
    * named and the translator puts a name there. The name has to come from the
    * plan: two sources reading different files whose paths end in the same
    * segment both asked for `data.csv`, and the script read one of them twice.
    */
  def standaloneSourcePath(): Option[String] = None

  /**
    * The name to offer for [[standaloneSourcePath]], before the plan has had a
    * chance to say whether another source already wants it.
    *
    * Taken from the last path segment instead of by parsing the whole string as a
    * URI: the resolver percent-encodes the file-relative segments but leaves the
    * repository and version names as the user typed them, so a dataset version
    * called `v3 - with long text` makes `new URI` throw on the space and no code
    * is generated at all.
    */
  final def standaloneSourceName(): Option[String] =
    standaloneSourcePath().map { rawPath =>
      val segment = rawPath.split("/").lastOption.getOrElse("")
      // Percent-decoding only, matching what `URI.getPath` used to return here: form
      // decoding would also turn a literal `+` in a file name into a space.
      URLDecoder.decode(segment.replace("+", "%2B"), StandardCharsets.UTF_8)
    }

  def producesDataFrame(): Boolean = true

  /**
    * Definitions this operator's standalone code depends on, emitted once near
    * the top of the script rather than inline.
    *
    * The translator concatenates operator bodies into a single module, so an
    * operator needing a helper class has nowhere to put it that another operator
    * would not duplicate. Helpers returned here are collected across the whole
    * plan and deduplicated by their text, so two sampling operators in one
    * workflow yield one copy of the generator they share.
    */
  def standaloneHelpers(): Seq[String] = Seq.empty

  /**
    * Modules this operator's standalone code needs, written as the import
    * statements themselves, collected across the plan and emitted once at the
    * top of the script.
    *
    * pandas is not named here: the translator emits it for every script, since
    * an operator body reads and writes frames whatever else it does. What an
    * operator states here is what it needs beyond that, so a script built from
    * operators that only reshape a table does not require a plotting library
    * to start.
    */
  def standaloneImports(): Seq[String] = Seq.empty

  /**
    * A helper each input frame goes through before this operator's code runs,
    * for an operator the engine hands its table in a different form than the
    * script's upstream does. The translator defines it once and feeds the
    * operator what it returns, leaving the upstream frame as it was.
    *
    * A Python operator runs in pyamber, which hands it a DOUBLE column as
    * float64, where a NaN and a null are one value. A source can hand the script
    * a Float64 column that keeps them apart, and an operator written against
    * float64 then answers differently.
    */
  def standaloneInputAdapter(): Option[StandaloneCodeGenerator.InputAdapter] =
    this match {
      case _: PythonOperatorDescriptor => Some(StandaloneCodeGenerator.PythonOperatorInput)
      case _                           => None
    }
}

object StandaloneCodeGenerator {

  /** A helper's name and the definition that binds it. */
  final case class InputAdapter(name: String, definition: String)

  /** `_texera_python_operator_input`: a copy with Float64 columns as float64, the way pyamber
    * hands them over, so the upstream frame another operator reads stays as it was.
    */
  val PythonOperatorInput: InputAdapter = InputAdapter(
    "_texera_python_operator_input",
    """def _texera_python_operator_input(df):
      |    df = df.copy()
      |    for _c in df.columns:
      |        if isinstance(df[_c].dtype, (pd.Float64Dtype, pd.Float32Dtype)):
      |            df[_c] = df[_c].astype("float64")
      |    return df""".stripMargin
  )

  /**
    * What a source writes where the file it reads should be named.
    *
    * A bare identifier rather than a string literal, because the translator only
    * rewrites the code parts of a body and leaves literals and comments alone.
    */
  val SourceFilePlaceholder: String = "sourceFile"

  /** Python that gives an empty read the columns and types its schema declares.
    *
    * With no row there is nothing to infer a type from, so pandas leaves every
    * column an object, and a JSONL read of no lines has no columns at all. The
    * executor still declares the schema it inferred from the file, so the next
    * step looks for those columns in those types. A read with rows is left alone.
    */
  def typeAnEmptyRead(frame: String, schema: Schema): String = {
    val names = schema.getAttributes.map(a => pyStringLiteral(a.getName))
    val dtypes = schema.getAttributes.flatMap { a =>
      emptyDtypes.get(a.getType).map(d => s"""${pyStringLiteral(a.getName)}: "$d"""")
    }
    s"""if $frame.empty:
       |    $frame = $frame.reindex(columns=[${names.mkString(", ")}]).astype({${dtypes
      .mkString(", ")}})""".stripMargin
  }

  /** Python that gives a read the schema's column names and reads each column the way the
    * engine does, with every name and its type stated once, in a `_schema` dict.
    *
    * A TIMESTAMP column goes through `_texera_text_to_timestamp` and a DOUBLE one through
    * `_texera_read_doubles`, both from [[StandaloneHelpers.AttributeCasts]], which the caller
    * declares. The loop names only the readers the schema needs, so a table with no such
    * column leaves the helper out of the script. An empty read takes the declared types.
    */
  def readAsDeclared(frame: String, schema: Schema): String = {
    val attributes = schema.getAttributes
    val entries = attributes.map { a =>
      s"""${pyStringLiteral(a.getName)}: "${emptyDtypes.getOrElse(a.getType, "object")}""""
    }
    val readers = Seq(
      AttributeType.TIMESTAMP -> "_texera_text_to_timestamp",
      AttributeType.DOUBLE -> "_texera_read_doubles"
    ).filter { case (t, _) => attributes.exists(_.getType == t) }
    val branches = readers.zipWithIndex.map {
      case ((t, reader), i) =>
        s"""    ${if (i == 0) "if" else "elif"} _t == "${emptyDtypes(t)}":
           |        $frame[_c] = $reader($frame[_c])""".stripMargin
    }
    val loop =
      if (branches.isEmpty) Seq.empty
      else Seq("for _c, _t in _schema.items():") ++ branches
    (Seq(s"_schema = {${entries.mkString(", ")}}", s"$frame.columns = list(_schema)") ++ loop ++
      Seq(s"if $frame.empty:", s"    $frame = $frame.astype(_schema)")).mkString("\n")
  }

  /** Python that reads each TIMESTAMP column of `schema`, held as text, the way
    * the engine reads it, with `_texera_text_to_timestamp` from
    * [[StandaloneHelpers.AttributeCasts]], which the caller declares.
    *
    * pandas parses a date column into nanoseconds, which end in 2262, and left a
    * later year as text. The engine hands each field to DateParserUtils, so a
    * row states its own format, any year reads, and the reading is cut to the
    * millisecond.
    */
  def parseTimestamps(frame: String, schema: Schema): String =
    readEach(frame, schema, AttributeType.TIMESTAMP, "_texera_text_to_timestamp")

  /** Python that reads each DOUBLE column of `schema` that pandas left as text
    * the way the engine reads it, with `_texera_read_doubles` from
    * [[StandaloneHelpers.AttributeCasts]], which the caller declares.
    *
    * The engine reads a field with `toDouble`, so "NaN", "2d" and "0x1p3" are
    * numbers to it, and the column was inferred DOUBLE. pandas' float parser
    * takes none of them and kept the whole column as strings.
    */
  def parseDoubles(frame: String, schema: Schema): String =
    readEach(frame, schema, AttributeType.DOUBLE, "_texera_read_doubles")

  // Python that passes each column of `attributeType` through `reader`: one line
  // for a single column, and a loop over the names for several, so a wide table
  // does not spend a line per column.
  private def readEach(
      frame: String,
      schema: Schema,
      attributeType: AttributeType,
      reader: String
  ): String =
    schema.getAttributes
      .filter(_.getType == attributeType)
      .map(a => pyStringLiteral(a.getName)) match {
      case Seq()     => ""
      case Seq(name) => s"$frame[$name] = $reader($frame[$name])"
      case names =>
        s"""for _c in [${names.mkString(", ")}]:
           |    $frame[_c] = $reader($frame[_c])""".stripMargin
    }

  private val emptyDtypes: Map[AttributeType, String] = Map(
    AttributeType.INTEGER -> "Int32",
    AttributeType.LONG -> "Int64",
    AttributeType.DOUBLE -> "float64",
    AttributeType.BOOLEAN -> "boolean",
    AttributeType.TIMESTAMP -> "datetime64[us]",
    AttributeType.STRING -> "object"
  )
}
