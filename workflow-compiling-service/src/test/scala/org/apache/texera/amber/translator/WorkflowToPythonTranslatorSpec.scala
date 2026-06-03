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

package org.apache.texera.amber.translator

import org.apache.texera.amber.compiler.model.{LogicalLink, LogicalPlan}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import org.apache.texera.amber.operator.filter.{
  ComparisonType,
  FilterPredicate,
  SpecializedFilterOpDesc
}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.sort.{SortCriteriaUnit, SortOpDesc, SortPreference}
import org.apache.texera.amber.operator.source.scan.arrow.ArrowSourceOpDesc
import org.apache.texera.amber.operator.source.scan.csv.{CSVScanSourceOpDesc, ParallelCSVScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpDesc
import org.apache.texera.amber.operator.source.fetcher.{DecodingMethod, URLFetcherOpDesc}
import org.apache.texera.amber.operator.source.scan.file.{FileScanOpDesc, FileScanSourceOpDesc}
import org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpDesc
import org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpDesc
import org.apache.texera.amber.operator.source.scan.FileAttributeType
import org.apache.texera.amber.operator.union.UnionOpDesc
import org.apache.texera.amber.operator.visualization.barChart.BarChartOpDesc
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
  * Tests for [[WorkflowToPythonTranslator]].
  *
  * Two groups:
  *   1. Characterization tests pinning the CURRENT emitted code for the four
  *      operators that already implement `generateStandaloneCode` (Sort,
  *      SpecializedFilter, BarChart, CSVScan). These exist so future operator
  *      work can't silently change translator/operator output. They assert on
  *      substrings, not the whole script, so unrelated additions don't break
  *      them — tighten an assertion only when that operator's contract changes.
  *   2. A regression test for multi-input port ordering (joins / set ops):
  *      `in1df`..`inNdf` must map to upstreams by the consuming operator's
  *      input-port index (`link.toPortId`), NOT the order links appear in the
  *      plan's link list.
  *
  * Harness: build a `LogicalPlan` directly from operator descriptors + links
  * (no JSON / HTTP round-trip) and run the translator in-process. Use the
  * `op` / `link` / `translate` helpers below for new operator tests; for a
  * multi-input operator follow `multi-input` example and the batch template.
  */
class WorkflowToPythonTranslatorSpec extends AnyFlatSpec with Matchers {

  private def translate(ops: List[LogicalOp], links: List[LogicalLink]): String =
    new WorkflowToPythonTranslator().translate(LogicalPlan(ops, links))

  /** A single-port link from `from`'s output port 0 to `to`'s input port 0. */
  private def link(from: LogicalOp, to: LogicalOp): LogicalLink =
    LogicalLink(from.operatorIdentifier, PortIdentity(0), to.operatorIdentifier, PortIdentity(0))

  /** A link onto an explicit input port of a multi-input consumer. */
  private def linkToPort(from: LogicalOp, to: LogicalOp, toPort: Int): LogicalLink =
    LogicalLink(
      from.operatorIdentifier,
      PortIdentity(0),
      to.operatorIdentifier,
      PortIdentity(toPort)
    )

  private def csvSource(fileUri: String): CSVScanSourceOpDesc = {
    val csv = new CSVScanSourceOpDesc()
    csv.fileName = Some(fileUri)
    csv.customDelimiter = Some(",")
    csv.hasHeader = true
    csv
  }

  private def sortBy(column: String, ascending: Boolean): SortOpDesc = {
    val sort = new SortOpDesc()
    val criteria = new SortCriteriaUnit()
    criteria.attributeName = column
    criteria.sortPreference = if (ascending) SortPreference.ASC else SortPreference.DESC
    sort.attributes = List(criteria)
    sort
  }

  // Returns the `dfN` variable the translator assigned to the CSV source that
  // reads the given basename, so multi-input assertions don't depend on the
  // topological numbering of the two sources.
  private def varReading(script: String, basename: String): String = {
    val pattern = ("""(df\d+) = pd\.read_csv\(filepath_or_buffer="""" + basename + """"""").r
    pattern.findFirstMatchIn(script).map(_.group(1)).getOrElse {
      fail(s"""no read_csv assignment for "$basename" in:\n$script""")
    }
  }

  behavior of "WorkflowToPythonTranslator"

  it should "emit shared imports once at the top" in {
    val script = translate(List(csvSource("file:/tmp/data.csv")), Nil)
    script should startWith("import pandas as pd")
    script should include("import plotly.express as px")
  }

  // --- Characterization: CSVScanSourceOpDesc (source) ---
  it should "translate CSVScanSourceOpDesc into a pd.read_csv assignment" in {
    val csv = csvSource("file:/tmp/data.csv")
    val script = translate(List(csv), Nil)

    script should include("""df1 = pd.read_csv(filepath_or_buffer="data.csv"""")
    script should include("sep=\",\"")
    script should include("header=0")
    // Single DataFrame-producing leaf -> translator prints a preview.
    script should include("print(df1.head())")
  }

  // --- Characterization: SortOpDesc (transform, single input) ---
  it should "translate SortOpDesc and substitute in1df/out1df with real vars" in {
    val csv = csvSource("file:/tmp/data.csv")
    val sort = sortBy("age", ascending = true)
    val script = translate(List(csv, sort), List(link(csv, sort)))

    script should include("""df2 = df1.sort_values(by=["age"], ascending=[True])""")
    script should not include "in1df"
    script should not include "out1df"
  }

  // --- Characterization: SpecializedFilterOpDesc (filter) ---
  it should "translate SpecializedFilterOpDesc with a numeric predicate" in {
    val csv = csvSource("file:/tmp/data.csv")
    val filter = new SpecializedFilterOpDesc()
    filter.predicates = List(new FilterPredicate("age", ComparisonType.GREATER_THAN, "18"))
    val script = translate(List(csv, filter), List(link(csv, filter)))

    script should include("""df2 = df1[(df1["age"] > 18)].reset_index(drop=True)""")
  }

  it should "translate an empty-predicate SpecializedFilterOpDesc as a copy" in {
    val csv = csvSource("file:/tmp/data.csv")
    val filter = new SpecializedFilterOpDesc()
    filter.predicates = List.empty
    val script = translate(List(csv, filter), List(link(csv, filter)))

    script should include("df2 = df1.copy()")
  }

  // --- Characterization: BarChartOpDesc (visualization, no DataFrame out) ---
  it should "translate BarChartOpDesc and not print a preview for it" in {
    val csv = csvSource("file:/tmp/data.csv")
    val bar = new BarChartOpDesc()
    bar.value = "sales"
    bar.fields = "region"
    val script = translate(List(csv, bar), List(link(csv, bar)))

    script should include("""df1 = df1.dropna(subset=["sales", "region"])""")
    script should include("""fig.write_html("output.html")""")
    // producesDataFrame == false -> no trailing head() preview for the chart.
    script should not include "print(df2.head())"
  }

  // --- Characterization: CSVOldScanSourceOpDesc ---
  it should "translate CSVOldScanSourceOpDesc into a pd.read_csv assignment" in {
    val op = new CSVOldScanSourceOpDesc()
    op.fileName = Some("file:/tmp/data.csv")
    op.customDelimiter = Some(",")
    op.hasHeader = true
    val script = translate(List(op), Nil)

    script should include("""df1 = pd.read_csv(filepath_or_buffer="data.csv"""")
    script should include("""sep=","""")
    script should include("header=0")
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate CSVOldScanSourceOpDesc with tab delimiter and no header" in {
    val op = new CSVOldScanSourceOpDesc()
    op.fileName = Some("file:/tmp/data.tsv")
    op.customDelimiter = Some("\t")
    op.hasHeader = false
    val script = translate(List(op), Nil)

    script should include("sep=\"\t\"")
    script should include("header=None")
    script should include("""df1.columns = [f"column-{i + 1}""")
    script should not include "out1df"
  }

  // --- Characterization: ParallelCSVScanSourceOpDesc ---
  it should "translate ParallelCSVScanSourceOpDesc into a pd.read_csv assignment" in {
    val op = new ParallelCSVScanSourceOpDesc()
    op.fileName = Some("file:/tmp/data.csv")
    op.customDelimiter = Some(",")
    op.hasHeader = true
    val script = translate(List(op), Nil)

    script should include("""df1 = pd.read_csv(filepath_or_buffer="data.csv"""")
    script should include("header=0")
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate ParallelCSVScanSourceOpDesc with limit" in {
    val op = new ParallelCSVScanSourceOpDesc()
    op.fileName = Some("file:/tmp/data.csv")
    op.customDelimiter = Some("|")
    op.hasHeader = true
    op.limit = Some(50)
    val script = translate(List(op), Nil)

    script should include("""sep="|"""")
    script should include("nrows=50")
    script should not include "out1df"
  }

  // --- Characterization: JSONLScanSourceOpDesc ---
  it should "translate JSONLScanSourceOpDesc into a pd.read_json assignment" in {
    val op = new JSONLScanSourceOpDesc()
    op.fileName = Some("file:/tmp/data.jsonl")
    op.flatten = false
    val script = translate(List(op), Nil)

    script should include("""df1 = pd.read_json("data.jsonl", lines=True""")
    script should not include "json_normalize"
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate JSONLScanSourceOpDesc with flatten=true into pd.json_normalize" in {
    val op = new JSONLScanSourceOpDesc()
    op.fileName = Some("file:/tmp/nested.jsonl")
    op.flatten = true
    val script = translate(List(op), Nil)

    script should include("pd.json_normalize(")
    script should include("""pd.read_json("nested.jsonl", lines=True""")
    script should not include "out1df"
  }

  // --- Characterization: ArrowSourceOpDesc ---
  it should "translate ArrowSourceOpDesc into a pd.read_feather assignment" in {
    val op = new ArrowSourceOpDesc()
    op.fileName = Some("file:/tmp/data.arrow")
    val script = translate(List(op), Nil)

    script should include("""df1 = pd.read_feather("data.arrow")""")
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate ArrowSourceOpDesc substituting the correct dfN variable" in {
    val arrow = new ArrowSourceOpDesc()
    arrow.fileName = Some("file:/tmp/data.arrow")
    val sort = sortBy("value", ascending = false)
    val script = translate(List(arrow, sort), List(link(arrow, sort)))

    script should include("""df1 = pd.read_feather("data.arrow")""")
    script should include("""df2 = df1.sort_values""")
    script should not include "out1df"
  }

  // --- Characterization: FileScanSourceOpDesc ---
  it should "translate FileScanSourceOpDesc in STRING mode into a line-by-line read" in {
    val op = new FileScanSourceOpDesc()
    op.fileName = Some("file:/tmp/notes.txt")
    val script = translate(List(op), Nil)

    script should include("""with open("notes.txt", "r", encoding="utf-8") as _f:""")
    script should include("""df1 = pd.DataFrame({"line": [l.rstrip("\n") for l in _f]})""")
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate FileScanSourceOpDesc in SINGLE_STRING mode as a whole-file read" in {
    val op = new FileScanSourceOpDesc()
    op.fileName = Some("file:/tmp/doc.txt")
    op.attributeType = FileAttributeType.SINGLE_STRING
    op.attributeName = "content"
    val script = translate(List(op), Nil)

    script should include("""with open("doc.txt", "r", encoding="utf-8") as _f:""")
    script should include("""df1 = pd.DataFrame({"content": [_f.read()]})""")
    script should not include "out1df"
  }

  // --- Characterization: TextInputSourceOpDesc ---
  it should "translate TextInputSourceOpDesc in STRING mode into a splitlines read" in {
    val op = new TextInputSourceOpDesc()
    op.textInput = "alice\nbob\ncharlie"
    val script = translate(List(op), Nil)

    script should include("""_text = "alice\nbob\ncharlie"""")
    script should include("""df1 = pd.DataFrame({"line": [l for l in _text.splitlines()]})""")
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate TextInputSourceOpDesc in SINGLE_STRING mode as a whole-input row" in {
    val op = new TextInputSourceOpDesc()
    op.textInput = "alice\nbob\ncharlie"
    op.attributeType = FileAttributeType.SINGLE_STRING
    op.attributeName = "content"
    val script = translate(List(op), Nil)

    script should include("""_text = "alice\nbob\ncharlie"""")
    script should include("""df1 = pd.DataFrame({"content": [_text]})""")
    script should not include "out1df"
  }

  // --- Characterization: FileScanOpDesc (input-driven file reader) ---
  it should "translate FileScanOpDesc in STRING mode into a per-file flattened read" in {
    val upstream = csvSource("file:/tmp/filenames.csv")
    val op = new FileScanOpDesc()
    val script = translate(List(upstream, op), List(link(upstream, op)))

    script should include("_rows = []")
    script should include("for _fn in df1.iloc[:, 0]:")
    script should include("""    with open(_fn, "r", encoding="utf-8") as _f:""")
    script should include("""        _rows.extend(l.rstrip("\n") for l in _f)""")
    script should include("""df2 = pd.DataFrame({"line": _rows})""")
    script should not include "in1df"
    script should not include "out1df"
  }

  it should "translate FileScanOpDesc in SINGLE_STRING mode with outputFileName=true" in {
    val upstream = csvSource("file:/tmp/filenames.csv")
    val op = new FileScanOpDesc()
    op.attributeType = FileAttributeType.SINGLE_STRING
    op.attributeName = "content"
    op.outputFileName = true
    val script = translate(List(upstream, op), List(link(upstream, op)))

    script should include("for _fn in df1.iloc[:, 0]:")
    script should include("""    with open(_fn, "r", encoding="utf-8") as _f:""")
    script should include("        _rows.append((_fn, _f.read()))")
    script should include("""df2 = pd.DataFrame(_rows, columns=["filename", "content"])""")
    script should not include "in1df"
    script should not include "out1df"
  }

  // --- Characterization: URLFetcherOpDesc ---
  it should "translate URLFetcherOpDesc with UTF_8 into a urllib fetch + decode" in {
    val op = new URLFetcherOpDesc()
    op.url = "https://example.com"
    op.decodingMethod = DecodingMethod.UTF_8
    val script = translate(List(op), Nil)

    script should include("""_url = "https://example.com"""")
    script should include("    with urllib.request.urlopen(_url) as _resp:")
    script should include("        _content = _resp.read()")
    script should include("""    _content = f"Fetch failed for URL: {_url}".encode("utf-8")""")
    script should include("""df1 = pd.DataFrame({"URL content": [_content.decode("utf-8")]})""")
    script should not include "out1df"
  }

  it should "translate URLFetcherOpDesc with RAW_BYTES into a urllib fetch without decode" in {
    val op = new URLFetcherOpDesc()
    op.url = "https://example.com/data.bin"
    op.decodingMethod = DecodingMethod.RAW_BYTES
    val script = translate(List(op), Nil)

    script should include("""_url = "https://example.com/data.bin"""")
    script should include("    with urllib.request.urlopen(_url) as _resp:")
    script should include("""df1 = pd.DataFrame({"URL content": [_content]})""")
    script should not include "[_content.decode"
    script should not include "out1df"
  }

  // --- SetOp: UnionOpDesc (variadic single port; 2-input concat, UNION ALL) ---
  it should "translate UnionOpDesc into a pd.concat that keeps duplicates" in {
    val csvA = csvSource("file:/tmp/a.csv")
    val csvB = csvSource("file:/tmp/b.csv")
    val union = new UnionOpDesc()

    // Union has ONE variadic input port; both upstreams land on port 0. The
    // tie on toPortId keeps link-list order, so in1df = first link (a.csv).
    val links = List(
      linkToPort(csvA, union, toPort = 0),
      linkToPort(csvB, union, toPort = 0)
    )
    val script = translate(List(csvA, csvB, union), links)

    val aVar = varReading(script, "a.csv")
    val bVar = varReading(script, "b.csv")
    script should include(s"= pd.concat([$aVar, $bVar], ignore_index=True)")
    script should not include "in1df"
    script should not include "in2df"
    script should not include "out1df"
  }

  it should "only concat the first two inputs of a 3-input Union (variadic limitation)" in {
    val csvA = csvSource("file:/tmp/a.csv")
    val csvB = csvSource("file:/tmp/b.csv")
    val csvC = csvSource("file:/tmp/c.csv")
    val union = new UnionOpDesc()

    val links = List(
      linkToPort(csvA, union, toPort = 0),
      linkToPort(csvB, union, toPort = 0),
      linkToPort(csvC, union, toPort = 0)
    )
    val script = translate(List(csvA, csvB, csvC, union), links)

    val aVar = varReading(script, "a.csv")
    val bVar = varReading(script, "b.csv")
    // KNOWN LIMITATION: the in1df/in2df scheme can't express N-unknown inputs,
    // so a 3rd upstream is silently dropped — only the first two are concatted.
    script should include(s"= pd.concat([$aVar, $bVar], ignore_index=True)")
  }

  // --- Regression: multi-input port ordering ---
  it should "map in1df/in2df to upstreams by input-port index, not link order" in {
    val csvPort0 = csvSource("file:/tmp/left.csv")
    val csvPort1 = csvSource("file:/tmp/right.csv")
    val join = new TwoInputStub("out1df = merge(in1df, in2df)")

    // Deliberately list the port-1 link BEFORE the port-0 link. The old
    // implementation used link-list order and would (wrongly) map
    // in1df -> right.csv. Correct behavior: in1df -> port 0 (left.csv).
    val links = List(
      linkToPort(csvPort1, join, toPort = 1),
      linkToPort(csvPort0, join, toPort = 0)
    )
    val script = translate(List(csvPort1, csvPort0, join), links)

    val leftVar = varReading(script, "left.csv") // wired to input port 0
    val rightVar = varReading(script, "right.csv") // wired to input port 1
    script should include(s"= merge($leftVar, $rightVar)")
  }
}

/**
  * Minimal two-input `StandaloneCodeGenerator` used only to exercise the
  * translator's multi-input port-ordering. `getPhysicalOp` defaults to `???`
  * in `LogicalOp` and the translator never calls it, so only `operatorInfo`
  * and `generateStandaloneCode` need defining.
  */
private class TwoInputStub(code: String) extends LogicalOp with StandaloneCodeGenerator {
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Two Input Stub",
      "test-only multi-input operator",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort(PortIdentity(0)), InputPort(PortIdentity(1))),
      outputPorts = List(OutputPort())
    )

  override def generateStandaloneCode(): String = code
}
