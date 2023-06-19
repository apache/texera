package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.operators.regex.RegexOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec

import java.util.UUID
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.storage.MemoryStorage

import scala.collection.mutable

class WorkflowRewriterSpec extends AnyFlatSpec with BeforeAndAfter {

  var opResultStorage: OpResultStorage = _

  private def create00Link(source: OperatorDescriptor, dest: OperatorDescriptor): OperatorLink = {
    OperatorLink(OperatorPort(source.operatorID, 0), OperatorPort(dest.operatorID, 0))
  }

  before {
    opResultStorage = new OpResultStorage()
  }

  var rewriter: WorkflowRewriter = _

  def operatorToString(operator: OperatorDescriptor): OperatorDescriptor = {
    operator
  }

  /**
    * Source -> Sink
    */
  it should "modify no operator for a workflow with no operators cached or to cached" in {
    val sourceOperator = new CSVScanSourceOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(sinkOperator.operatorID, 0)

    val operators = List(sourceOperator, sinkOperator)
    val links = List(OperatorLink(origin, destination))
    val breakpoints = List[BreakpointInfo]()

    val logicalPlan = LogicalPlan(operators, links, breakpoints)
    logicalPlan.cachedOperatorIds = List[String]()
    rewriter = new WorkflowRewriter(
      logicalPlan,
      mutable.HashMap[String, OperatorDescriptor](),
      mutable.HashMap[String, CacheSourceOpDesc](),
      mutable.HashMap[String, ProgressiveSinkOpDesc](),
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )
    val rewrittenLogicalPlan = rewriter.rewrite
    rewrittenLogicalPlan.operatorMap.values.foreach(operator => {
      assert(operators.contains(operator))
    })
  }

  /**
    * [] for cached
    * [Source] -> Sink => CacheSource -> CacheSink
    */
  it should "replace source with cache" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    val uuid = UUID.randomUUID().toString
    operators += sourceOperator
    operators += sinkOperator

    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(sinkOperator.operatorID, 0)
    links += OperatorLink(origin, destination)

    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
    breakpoints += breakpointInfo

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)
    logicalPlan.cachedOperatorIds = List(sourceOperator.operatorID)

    val tuples = mutable.MutableList[Tuple]()
    val cacheSourceOperator = new CacheSourceOpDesc(uuid, opResultStorage)
    val cacheSinkOperator = new ProgressiveSinkOpDesc()
    cacheSinkOperator.setStorage(new MemoryStorage(new Schema()))
    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()
    operatorOutputCache += ((sourceOperator.operatorID, tuples))
    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    cachedOperators += ((sourceOperator.operatorID, operatorToString(sourceOperator)))
    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    cacheSourceOperators += ((sourceOperator.operatorID, cacheSourceOperator))
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()
    cacheSinkOperators += ((sourceOperator.operatorID, cacheSinkOperator))

    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    rewriter.operatorRecord += (
      (
        sourceOperator.operatorID,
        rewriter.getWorkflowVertex(sourceOperator)
      )
    )
    val rewrittenLogicalPlan = rewriter.rewrite
    assert(2.equals(rewrittenLogicalPlan.operatorMap.size))
    assert(rewrittenLogicalPlan.operatorMap.contains(cacheSourceOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(sinkOperator.operatorID))
    assert(1.equals(rewrittenLogicalPlan.links.size))
    assert(1.equals(rewrittenLogicalPlan.breakpoints.size))
  }

  /**
    * {} for to cache
    * {Source} -> Sink => Source -> Sink
    *                       |
    *                       -> CacheSink
    */
  it should "add a CacheSinkOpDesc" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    operators += sourceOperator
    operators += sinkOperator

    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(sinkOperator.operatorID, 0)
    links += OperatorLink(origin, destination)

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)
    logicalPlan.cachedOperatorIds = List(sourceOperator.operatorID)

    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()

    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    val rewrittenLogicalPlan = rewriter.rewrite
    assert(3.equals(rewrittenLogicalPlan.operatorMap.size))
    assert(rewrittenLogicalPlan.operatorMap.contains(sourceOperator.operatorID))
    assert(
      rewrittenLogicalPlan.operatorMap(sinkOperator.operatorID).isInstanceOf[ProgressiveSinkOpDesc]
    )
    assert(rewrittenLogicalPlan.operatorMap.contains(sinkOperator.operatorID))
    assert(2.equals(rewrittenLogicalPlan.links.size))
    assert(0.equals(rewrittenLogicalPlan.breakpoints.size))
  }

  /**
    * [] for cached
    * {} for to cache
    * {Source} -> Sink => Source -> Sink
    *   |                   |
    *   -> Sink             -> Sink
    *                       |
    *                       -> CacheSink
    */
  it should "add correct numbers of operators and links" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    val sinkOperator2 = new ProgressiveSinkOpDesc()
    operators += sourceOperator
    operators += sinkOperator
    operators += sinkOperator2

    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(sinkOperator.operatorID, 0)
    links += OperatorLink(origin, destination)

    val destination2 = OperatorPort(sinkOperator2.operatorID, 0)
    links += OperatorLink(origin, destination2)

    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
    breakpoints += breakpointInfo

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)
    logicalPlan.cachedOperatorIds = List(sourceOperator.operatorID)

    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()

    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    val rewrittenLogicalPlan = rewriter.rewrite
    assert(4.equals(rewrittenLogicalPlan.operatorMap.size))
    assert(rewrittenLogicalPlan.operatorMap.contains(sourceOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(sinkOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(sinkOperator2.operatorID))
    assert(3.equals(rewrittenLogicalPlan.links.size))
    assert(1.equals(rewrittenLogicalPlan.breakpoints.size))
    assert(cacheSinkOperators.contains(sourceOperator.operatorID))
    assert(cacheSinkOperators(sourceOperator.operatorID).isInstanceOf[ProgressiveSinkOpDesc])
  }

  /**
    * [] for cached
    * {} for to cache
    * Source -> [Filter] -> Sink => CacheSource -> Sink
    */
  it should "replace source and filter with cache" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val filterOperator = new RegexOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    operators += sourceOperator
    operators += filterOperator
    operators += sinkOperator

    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(filterOperator.operatorID, 0)
    links += OperatorLink(origin, destination)

    val origin2 = OperatorPort(filterOperator.operatorID, 0)
    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
    links += OperatorLink(origin2, destination2)

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)

    val tuples = mutable.MutableList[Tuple]()
    val uuid = UUID.randomUUID().toString
    val cacheSourceOperator = new CacheSourceOpDesc(uuid, opResultStorage)
    val cacheSinkOperator = new ProgressiveSinkOpDesc()
    cacheSinkOperator.setStorage(new MemoryStorage(new Schema()))
    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()

    val cachedOperatorID = filterOperator.operatorID

    logicalPlan.cachedOperatorIds = List(cachedOperatorID)
    operatorOutputCache += ((cachedOperatorID, tuples))

    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()
    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))

    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
    breakpoints += breakpointInfo
    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    rewriter.operatorRecord += (
      (
        sourceOperator.operatorID,
        rewriter.getWorkflowVertex(sourceOperator)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator.operatorID,
        rewriter.getWorkflowVertex(filterOperator)
      )
    )

    val rewrittenLogicalPlan = rewriter.rewrite
    assert(2.equals(rewrittenLogicalPlan.operatorMap.size))
    assert(rewrittenLogicalPlan.operatorMap.contains(cacheSourceOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(sinkOperator.operatorID))
    assert(1.equals(rewrittenLogicalPlan.links.size))
    assert(0.equals(rewrittenLogicalPlan.breakpoints.size))
  }

  /**
    * [] for cached
    * {} for to cache
    * Source -> [Filter](invalid) -> Sink => Source -> Filter -> Sink
    *                                                    |
    *                                                    -> CacheSink
    */
  it should "invalidate cache and replace no operator" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val filterOperator = new RegexOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    operators += sourceOperator
    operators += filterOperator
    operators += sinkOperator

    val origin = OperatorPort(sourceOperator.operatorID, 0)
    val destination = OperatorPort(filterOperator.operatorID, 0)
    links += OperatorLink(origin, destination)

    val origin2 = OperatorPort(filterOperator.operatorID, 0)
    val destination2 = OperatorPort(sinkOperator.operatorID, 0)
    links += OperatorLink(origin2, destination2)

    val breakpointInfo = BreakpointInfo(sourceOperator.operatorID, CountBreakpoint(0))
    breakpoints += breakpointInfo

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)

    val tuples = mutable.MutableList[Tuple]()
    val uuid = UUID.randomUUID().toString
    val cacheSourceOperator = new CacheSourceOpDesc(uuid, opResultStorage)
    val cacheSinkOperator = new ProgressiveSinkOpDesc()
    val operatorOutputCache = mutable.HashMap[String, mutable.MutableList[Tuple]]()

    val cachedOperatorID = filterOperator.operatorID

    logicalPlan.cachedOperatorIds = List(cachedOperatorID)
    operatorOutputCache += ((cachedOperatorID, tuples))

    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    cachedOperators += ((cachedOperatorID, operatorToString(filterOperator)))
    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    cacheSourceOperators += ((cachedOperatorID, cacheSourceOperator))
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()
    cacheSinkOperators += ((cachedOperatorID, cacheSinkOperator))

    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    val modifiedSourceOperator = new CSVScanSourceOpDesc()
    modifiedSourceOperator.hasHeader = false
    modifiedSourceOperator.operatorID = sourceOperator.operatorID
    rewriter.operatorRecord += (
      (
        sourceOperator.operatorID,
        rewriter.getWorkflowVertex(modifiedSourceOperator)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator.operatorID,
        rewriter.getWorkflowVertex(filterOperator)
      )
    )

    val rewrittenLogicalPlan = rewriter.rewrite
    assert(4.equals(rewrittenLogicalPlan.operatorMap.size))
    assert(!rewrittenLogicalPlan.operatorMap.contains(cacheSourceOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(sourceOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(filterOperator.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.contains(sinkOperator.operatorID))
    assert(3.equals(rewrittenLogicalPlan.links.size))
    assert(1.equals(rewrittenLogicalPlan.breakpoints.size))
    assert(3.equals(rewriter.operatorRecord.size))
  }

  /**
    * [] for cached
    * {} for to cache
    * Source -> {Filter} -> Filter  -> [Filter] -> Sink => Source -> Filter -> Filter -> Filter -> Sink
    *                                                                  |                   |
    *                                                                  -> CacheSink        -> CacheSink
    */
  it should "invalidate downstream cache" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val filterOperator = new RegexOpDesc()
    val filterOperator2 = new RegexOpDesc()
    val filterOperator3 = new RegexOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    operators += sourceOperator
    operators += filterOperator
    operators += filterOperator2
    operators += filterOperator3
    operators += sinkOperator

    links += create00Link(sourceOperator, filterOperator)
    links += create00Link(filterOperator, filterOperator2)
    links += create00Link(filterOperator2, filterOperator3)
    links += create00Link(filterOperator3, sinkOperator)

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)

    val uuidForFilter3 = UUID.randomUUID().toString
    val cacheSourceForFilter3 = new CacheSourceOpDesc(uuidForFilter3, opResultStorage)
    val cacheSinkForFilter3 = new ProgressiveSinkOpDesc()

    val cachedOperatorIDForFilter3 = filterOperator3.operatorID
    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    cachedOperators += ((cachedOperatorIDForFilter3, operatorToString(filterOperator)))

    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    cacheSourceOperators += ((cachedOperatorIDForFilter3, cacheSourceForFilter3))
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()
    cacheSinkOperators += ((cachedOperatorIDForFilter3, cacheSinkForFilter3))

    logicalPlan.cachedOperatorIds =
      List[String](cachedOperatorIDForFilter3, filterOperator.operatorID)

    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    rewriter.operatorRecord += (
      (
        sourceOperator.operatorID,
        rewriter.getWorkflowVertex(sourceOperator)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator.operatorID,
        rewriter.getWorkflowVertex(filterOperator)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator2.operatorID,
        rewriter.getWorkflowVertex(filterOperator2)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator3.operatorID,
        rewriter.getWorkflowVertex(filterOperator3)
      )
    )
    rewriter.operatorRecord += ((sinkOperator.operatorID, rewriter.getWorkflowVertex(sinkOperator)))

    val rewrittenLogicalPlan = rewriter.rewrite
    assert(!rewrittenLogicalPlan.operatorMap.contains(cacheSourceForFilter3.operatorID))
    assert(cacheSinkOperators.size == 2)
    assert(rewrittenLogicalPlan.operatorMap.size == 7)
  }

  /**
    * [] for cached
    * {} for to cache
    * Source -> [Filter] -> Filter -> {Filter} -> Sink => CacheSource -> Filter -> Filter -> Sink
    *                                                                                |
    *                                                                                -> CacheSink
    */
  it should "preserve upstream cache" in {
    val operators = mutable.MutableList[OperatorDescriptor]()
    val links = mutable.MutableList[OperatorLink]()
    val breakpoints = mutable.MutableList[BreakpointInfo]()
    val sourceOperator = new CSVScanSourceOpDesc()
    val filterOperator = new RegexOpDesc()
    val filterOperator2 = new RegexOpDesc()
    val filterOperator3 = new RegexOpDesc()
    val sinkOperator = new ProgressiveSinkOpDesc()
    operators += sourceOperator
    operators += filterOperator
    operators += filterOperator2
    operators += filterOperator3
    operators += sinkOperator

    links += create00Link(sourceOperator, filterOperator)
    links += create00Link(filterOperator, filterOperator2)
    links += create00Link(filterOperator2, filterOperator3)
    links += create00Link(filterOperator3, sinkOperator)

    val logicalPlan = LogicalPlan(operators.toList, links.toList, breakpoints.toList)

    val uuidForFilter = UUID.randomUUID().toString
    val cacheSourceForFilter = new CacheSourceOpDesc(uuidForFilter, opResultStorage)
    val cacheSinkForFilter = new ProgressiveSinkOpDesc()
    cacheSinkForFilter.setStorage(new MemoryStorage(new Schema()))
    val cachedOperatorIDForFilter = filterOperator.operatorID
    val cachedOperators = mutable.HashMap[String, OperatorDescriptor]()
    cachedOperators += ((cachedOperatorIDForFilter, operatorToString(filterOperator)))

    val cacheSourceOperators = mutable.HashMap[String, CacheSourceOpDesc]()
    cacheSourceOperators += ((cachedOperatorIDForFilter, cacheSourceForFilter))
    val cacheSinkOperators = mutable.HashMap[String, ProgressiveSinkOpDesc]()
    cacheSinkOperators += ((cachedOperatorIDForFilter, cacheSinkForFilter))

    logicalPlan.cachedOperatorIds =
      List[String](cachedOperatorIDForFilter, filterOperator3.operatorID)

    rewriter = new WorkflowRewriter(
      logicalPlan,
      cachedOperators,
      cacheSourceOperators,
      cacheSinkOperators,
      mutable.HashMap[String, WorkflowVertex](),
      opResultStorage
    )

    rewriter.operatorRecord += (
      (
        sourceOperator.operatorID,
        rewriter.getWorkflowVertex(sourceOperator)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator.operatorID,
        rewriter.getWorkflowVertex(filterOperator)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator2.operatorID,
        rewriter.getWorkflowVertex(filterOperator2)
      )
    )
    rewriter.operatorRecord += (
      (
        filterOperator3.operatorID,
        rewriter.getWorkflowVertex(filterOperator3)
      )
    )
    rewriter.operatorRecord += ((sinkOperator.operatorID, rewriter.getWorkflowVertex(sinkOperator)))

    val rewrittenLogicalPlan = rewriter.rewrite
    println(rewrittenLogicalPlan.toString)
    assert(rewrittenLogicalPlan.operatorMap.contains(cacheSourceForFilter.operatorID))
    assert(rewrittenLogicalPlan.operatorMap.size == 5)
  }

  after {
    opResultStorage.close()
  }
}
