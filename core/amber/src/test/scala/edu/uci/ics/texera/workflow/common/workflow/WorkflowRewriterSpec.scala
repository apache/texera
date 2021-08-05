//package edu.uci.ics.texera.workflow.common.workflow
//
//import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
//import edu.uci.ics.texera.workflow.common.tuple.Tuple
//import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
//import edu.uci.ics.texera.workflow.operators.regex.RegexOpDesc
//import edu.uci.ics.texera.workflow.operators.sink.{CacheSinkOpDesc, SimpleSinkOpDesc}
//import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
//import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
//import org.scalatest.BeforeAndAfter
//import org.scalatest.flatspec.AnyFlatSpec
//
//import scala.collection.mutable
//
//class WorkflowRewriterSpec extends AnyFlatSpec with BeforeAndAfter {
//  var rewriter: WorkflowRewriter = _
//
//  def operatorToString(operator: OperatorDescriptor): OperatorDescriptor = {
//    operator
//  }
//
//  it should "throw exception for null input" in {
//    rewriter = new WorkflowRewriter(null, null, null, null, null, null)
//    assertThrows[Exception](rewriter.rewrite == null)
//  }
//
//  it should "return empty workflowInfo" in {
//    val workflowInfo: WorkflowInfo = new WorkflowInfo(
//      mutable.MutableList[OperatorDescriptor](),
//      mutable.MutableList[OperatorLink](),
//      mutable.MutableList[BreakpointInfo]()
//    )
//    rewriter = new WorkflowRewriter(workflowInfo, null, null, null, null, null)
//    assert(rewriter.rewrite.equals(workflowInfo))
//  }
//
//  it should "modify no operator" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList[String]()
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      mutable.HashMap[String, mutable.MutableList[Tuple]](),
//      mutable.HashMap[String, OperatorDescriptor](),
//      mutable.HashMap[String, CacheSourceOpDesc](),
//      mutable.HashMap[String, CacheSinkOpDesc](),
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//    val rewrittenWorkflowInfo = rewriter.rewrite
//    rewrittenWorkflowInfo.operators.foreach(operator => {
//      assert(operators.contains(operator))
//    })
//  }
//
//  it should "replace source with cache" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDesc(tuples)
//    val cacheSinkOperator = new CacheSinkOpDesc(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//    operatorOutputCache += ((sourceOperator.operatorID, tuples))
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((sourceOperator.operatorID, operatorToString(sourceOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    cacheSourceOperators += ((sourceOperator.operatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//    cacheSinkOperators += ((sourceOperator.operatorID, cacheSinkOperator))
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(sourceOperator)
//      )
//    )
//    val rewrittenWorkflowInfo = rewriter.rewrite
//    assert(2.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(1.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "add a CacheSinkOpDesc" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite
//    assert(3.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators(1).isInstanceOf[CacheSinkOpDesc])
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(2.equals(rewrittenWorkflowInfo.links.size))
//    assert(0.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "add correct numbers of operators and links" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    val sinkOperator2 = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//    operators += sinkOperator2
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val destination2 = OperatorPort(sinkOperator2.operatorID, 0)
//    links += OperatorLink(origin, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite
//    assert(4.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(3.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "replace source and filter with cache" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val filterOperator = new RegexOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += filterOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(filterOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val origin2 = OperatorPort(filterOperator.operatorID, 0)
//    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin2, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDesc(tuples)
//    val cacheSinkOperator = new CacheSinkOpDesc(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//
//    val cachedOperatorID = filterOperator.operatorID
//
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(cachedOperatorID)
//    operatorOutputCache += ((cachedOperatorID, tuples))
//
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(sourceOperator)
//      )
//    )
//    rewriter.operatorRecord += (
//      (
//        filterOperator.operatorID,
//        rewriter.getWorkflowVertex(filterOperator)
//      )
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite
//    assert(2.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(1.equals(rewrittenWorkflowInfo.links.size))
//    assert(0.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "invalidate cache and replace no operator" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val filterOperator = new RegexOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += filterOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(filterOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val origin2 = OperatorPort(filterOperator.operatorID, 0)
//    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin2, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDesc(tuples)
//    val cacheSinkOperator = new CacheSinkOpDesc(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//
//    val cachedOperatorID = filterOperator.operatorID
//
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(cachedOperatorID)
//    operatorOutputCache += ((cachedOperatorID, tuples))
//
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val modifiedSourceOperator = new CSVScanSourceOpDesc()
//    modifiedSourceOperator.hasHeader = false
//    modifiedSourceOperator.operatorID = sourceOperator.operatorID
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(modifiedSourceOperator)
//      )
//    )
//    rewriter.operatorRecord += (
//      (
//        filterOperator.operatorID,
//        rewriter.getWorkflowVertex(filterOperator)
//      )
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite
//    assert(4.equals(rewrittenWorkflowInfo.operators.size))
//    assert(!rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(filterOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(3.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//    assert(3.equals(rewriter.operatorRecord.size))
//  }
//
//  it should "throw exception for null input v2" in {
//    rewriter = new WorkflowRewriter(null, null, null, null, null, null)
//    assertThrows[Exception](rewriter.rewrite == null)
//  }
//
//  it should "return empty workflowInfo v2" in {
//    val workflowInfo: WorkflowInfo = new WorkflowInfo(
//      mutable.MutableList[OperatorDescriptor](),
//      mutable.MutableList[OperatorLink](),
//      mutable.MutableList[BreakpointInfo]()
//    )
//    rewriter = new WorkflowRewriter(workflowInfo, null, null, null, null, null)
//    assert(rewriter.rewrite_v2.equals(workflowInfo))
//  }
//
//  it should "modify no operator v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList[String]()
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      mutable.HashMap[String, mutable.MutableList[Tuple]](),
//      mutable.HashMap[String, OperatorDescriptor](),
//      mutable.HashMap[String, CacheSourceOpDesc](),
//      mutable.HashMap[String, CacheSinkOpDesc](),
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    rewrittenWorkflowInfo.operators.foreach(operator => {
//      assert(operators.contains(operator))
//    })
//  }
//
//  it should "replace source with cache v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDesc(tuples)
//    val cacheSinkOperator = new CacheSinkOpDesc(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//    operatorOutputCache += ((sourceOperator.operatorID, tuples))
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((sourceOperator.operatorID, operatorToString(sourceOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    cacheSourceOperators += ((sourceOperator.operatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//    cacheSinkOperators += ((sourceOperator.operatorID, cacheSinkOperator))
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(sourceOperator)
//      )
//    )
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(2.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(1.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "add a CacheSinkOpDesc v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(3.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators(1).isInstanceOf[CacheSinkOpDesc])
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(2.equals(rewrittenWorkflowInfo.links.size))
//    assert(0.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "add correct numbers of operators and links v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    val sinkOperator2 = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += sinkOperator
//    operators += sinkOperator2
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val destination2 = OperatorPort(sinkOperator2.operatorID, 0)
//    links += OperatorLink(origin, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(sourceOperator.operatorID)
//
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(4.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(3.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "replace source and filter with cache v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val filterOperator = new RegexOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += filterOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(filterOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val origin2 = OperatorPort(filterOperator.operatorID, 0)
//    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin2, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDesc(tuples)
//    val cacheSinkOperator = new CacheSinkOpDesc(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//
//    val cachedOperatorID = filterOperator.operatorID
//
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(cachedOperatorID)
//    operatorOutputCache += ((cachedOperatorID, tuples))
//
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(sourceOperator)
//      )
//    )
//    rewriter.operatorRecord += (
//      (
//        filterOperator.operatorID,
//        rewriter.getWorkflowVertex(filterOperator)
//      )
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(2.equals(rewrittenWorkflowInfo.operators.size))
//    assert(rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(1.equals(rewrittenWorkflowInfo.links.size))
//    assert(0.equals(rewrittenWorkflowInfo.breakpoints.size))
//  }
//
//  it should "invalidate cache and replace no operator v2" in {
//    val operators = mutable.MutableList[OperatorDescriptor]()
//    val links = mutable.MutableList[OperatorLink]()
//    val breakpoints = mutable.MutableList[BreakpointInfo]()
//    val sourceOperator = new CSVScanSourceOpDesc()
//    val filterOperator = new RegexOpDesc()
//    val sinkOperator = new SimpleSinkOpDesc()
//    operators += sourceOperator
//    operators += filterOperator
//    operators += sinkOperator
//
//    val origin = OperatorPort(sourceOperator.operatorID, 0)
//    val destination = OperatorPort(filterOperator.operatorID, 0)
//    links += OperatorLink(origin, destination)
//
//    val origin2 = OperatorPort(filterOperator.operatorID, 0)
//    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
//    links += OperatorLink(origin2, destination2)
//
//    val workflowInfo = WorkflowInfo(operators, links, breakpoints)
//
//    val tuples = mutable.MutableList[Tuple]()
//    val cacheSourceOperator = new CacheSourceOpDesc(tuples)
//    val cacheSinkOperator = new CacheSinkOpDesc(tuples)
//    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
//    cacheSinkOperator.schema = new Schema()
//
//    val cachedOperatorID = filterOperator.operatorID
//
//    workflowInfo.cachedOperatorIDs = mutable.MutableList(cachedOperatorID)
//    operatorOutputCache += ((cachedOperatorID, tuples))
//
//    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
//    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
//    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
//    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
//    val cacheSinkOperators = mutable.HashMap[String, CacheSinkOpDesc]()
//    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))
//
//    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
//    breakpoints += breakpointInfo
//    rewriter = new WorkflowRewriter(
//      workflowInfo,
//      operatorOutputCache,
//      cachedOperators,
//      cacheSourceOperators,
//      cacheSinkOperators,
//      mutable.HashMap[String, WorkflowVertex]()
//    )
//
//    val modifiedSourceOperator = new CSVScanSourceOpDesc()
//    modifiedSourceOperator.hasHeader = false
//    modifiedSourceOperator.operatorID = sourceOperator.operatorID
//    rewriter.operatorRecord += (
//      (
//        sourceOperator.operatorID,
//        rewriter.getWorkflowVertex(modifiedSourceOperator)
//      )
//    )
//    rewriter.operatorRecord += (
//      (
//        filterOperator.operatorID,
//        rewriter.getWorkflowVertex(filterOperator)
//      )
//    )
//
//    val rewrittenWorkflowInfo = rewriter.rewrite_v2
//    assert(4.equals(rewrittenWorkflowInfo.operators.size))
//    assert(!rewrittenWorkflowInfo.operators.contains(cacheSourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sourceOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(filterOperator))
//    assert(rewrittenWorkflowInfo.operators.contains(sinkOperator))
//    assert(3.equals(rewrittenWorkflowInfo.links.size))
//    assert(1.equals(rewrittenWorkflowInfo.breakpoints.size))
//    assert(3.equals(rewriter.operatorRecord.size))
//  }
//}
