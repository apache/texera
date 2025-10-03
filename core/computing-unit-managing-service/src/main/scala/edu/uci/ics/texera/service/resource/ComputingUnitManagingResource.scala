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

package edu.uci.ics.texera.service.resource

import edu.uci.ics.amber.config.{EnvironmentalVariable, StorageConfig}
import edu.uci.ics.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_MINUTES, jwtClaims}
import edu.uci.ics.texera.auth.{JwtAuth, SessionUser}
import edu.uci.ics.texera.config.{ComputingUnitConfig, KubernetesConfig}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{
  ComputingUnitUserAccessDao,
  WorkflowComputingUnitDao
}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import edu.uci.ics.texera.dao.jooq.generated.enums.{PrivilegeEnum, WorkflowComputingUnitTypeEnum}
import KubernetesConfig.{
  cpuLimitOptions,
  gpuLimitOptions,
  maxNumOfRunningComputingUnitsPerUser,
  memoryLimitOptions
}
import edu.uci.ics.texera.service.resource.ComputingUnitManagingResource._
import edu.uci.ics.texera.service.resource.ComputingUnitState._
import edu.uci.ics.texera.service.resource.ComputingUnitAccessResource
import edu.uci.ics.texera.service.util.{
  ComputingUnitManagingServiceException,
  InsufficientComputingUnitQuota,
  KubernetesClient
}
import io.dropwizard.auth.Auth
import io.fabric8.kubernetes.api.model.Quantity
import io.fabric8.kubernetes.client.KubernetesClientException
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core.{MediaType, Response}
import org.apache.commons.lang3.StringUtils
import org.jooq.{DSLContext, EnumType}

import java.sql.Timestamp
import play.api.libs.json._

import scala.annotation.unused
import scala.jdk.CollectionConverters.CollectionHasAsScala

object ComputingUnitManagingResource {
  private lazy val context: DSLContext = SqlServer
    .getInstance()
    .createDSLContext()

  // Environment variables passed to the created computing unit(pod)
  private lazy val computingUnitEnvironmentVariables: Map[String, Any] = Map(
    // Variables for saving results to Iceberg
    EnvironmentalVariable.ENV_ICEBERG_CATALOG_TYPE -> StorageConfig.icebergCatalogType,
    EnvironmentalVariable.ENV_ICEBERG_CATALOG_POSTGRES_URI_WITHOUT_SCHEME -> StorageConfig.icebergPostgresCatalogUriWithoutScheme,
    EnvironmentalVariable.ENV_ICEBERG_CATALOG_POSTGRES_USERNAME -> StorageConfig.icebergPostgresCatalogUsername,
    EnvironmentalVariable.ENV_ICEBERG_CATALOG_POSTGRES_PASSWORD -> StorageConfig.icebergPostgresCatalogPassword,
    // Variables for saving the metadata of the results, i.e. URIs of results/stats
    EnvironmentalVariable.ENV_JDBC_URL -> StorageConfig.jdbcUrl,
    EnvironmentalVariable.ENV_JDBC_USERNAME -> StorageConfig.jdbcUsername,
    EnvironmentalVariable.ENV_JDBC_PASSWORD -> StorageConfig.jdbcPassword,
    // Variables for reading files & exporting results
    // LakeFS endpoint is passed to CU to make CU work in dev mode(using localhost & using default LakeFS credentials)
    // LakeFS credentials should NOT be passed to CU
    EnvironmentalVariable.ENV_LAKEFS_ENDPOINT -> StorageConfig.lakefsEndpoint,
    EnvironmentalVariable.ENV_FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT -> EnvironmentalVariable
      .get(EnvironmentalVariable.ENV_FILE_SERVICE_GET_PRESIGNED_URL_ENDPOINT)
      .get,
    EnvironmentalVariable.ENV_FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT -> EnvironmentalVariable
      .get(EnvironmentalVariable.ENV_FILE_SERVICE_UPLOAD_ONE_FILE_TO_DATASET_ENDPOINT)
      .get,
    // Variables for amber setting
    // TODO: use AmberConfig for the following items. Currently AmberConfig is only accessible in workflow-executing-service
    EnvironmentalVariable.ENV_SCHEDULE_GENERATOR_ENABLE_COST_BASED_SCHEDULE_GENERATOR -> EnvironmentalVariable
      .get(EnvironmentalVariable.ENV_SCHEDULE_GENERATOR_ENABLE_COST_BASED_SCHEDULE_GENERATOR)
      .get,
    EnvironmentalVariable.ENV_USER_SYS_ENABLED -> EnvironmentalVariable
      .get(EnvironmentalVariable.ENV_USER_SYS_ENABLED)
      .get,
    EnvironmentalVariable.ENV_MAX_WORKFLOW_WEBSOCKET_REQUEST_PAYLOAD_SIZE_KB -> EnvironmentalVariable
      .get(EnvironmentalVariable.ENV_MAX_WORKFLOW_WEBSOCKET_REQUEST_PAYLOAD_SIZE_KB)
      .get
  )

  case class WorkflowComputingUnitCreationParams(
      name: String,
      unitType: String,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      jvmMemorySize: String,
      shmSize: String,
      uri: Option[String] = None
  )

  case class WorkflowComputingUnitResourceLimit(
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String
  )

  case class WorkflowComputingUnitMetrics(
      cpuUsage: String,
      memoryUsage: String
  )

  case class DashboardWorkflowComputingUnit(
      computingUnit: WorkflowComputingUnit,
      status: String,
      metrics: WorkflowComputingUnitMetrics,
      isOwner: Boolean,
      accessPrivilege: EnumType
  )

  case class ComputingUnitLimitOptionsResponse(
      cpuLimitOptions: List[String],
      memoryLimitOptions: List[String],
      gpuLimitOptions: List[String]
  )

  case class ComputingUnitTypesResponse(
      typeOptions: List[String]
  )
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit")
class ComputingUnitManagingResource {

  private def getComputingUnitByCuid(ctx: DSLContext, cuid: Int): WorkflowComputingUnit = {
    val wcDao = new WorkflowComputingUnitDao(ctx.configuration())
    val unit = wcDao.fetchOneByCuid(cuid)

    if (unit == null) {
      throw new NotFoundException(s"Computing unit with cuid=$cuid does not exist.")
    }
    unit
  }

  private def userOwnComputingUnit(ctx: DSLContext, cuid: Integer, uid: Integer): Boolean = {
    getComputingUnitByCuid(ctx, cuid).getUid == uid
  }

  private def getSupportedComputingUnitTypes: List[String] = {
    val allTypes = WorkflowComputingUnitTypeEnum.values().map(_.getLiteral).toList
    allTypes.filter {
      case "local"      => ComputingUnitConfig.localComputingUnitEnabled
      case "kubernetes" => KubernetesConfig.kubernetesComputingUnitEnabled
      case _            => false // Any unknown types are disabled by default
    }
  }

  private def getComputingUnitStatus(unit: WorkflowComputingUnit): ComputingUnitState = {
    unit.getType match {
      // ── Local CUs are always "running" ──────────────────────────────
      case WorkflowComputingUnitTypeEnum.local =>
        Running

      // ── Kubernetes CUs – only explicit "Running" counts as running ─
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val phaseOpt = KubernetesClient
          .getPodByName(KubernetesClient.generatePodName(unit.getCuid))
          .map(_.getStatus.getPhase)

        if (phaseOpt.contains("Running")) Running else Pending

      // ── Any other (unknown) type is treated as pending ──────────────
      case _ =>
        Pending
    }
  }

  private def getComputingUnitMetrics(unit: WorkflowComputingUnit): WorkflowComputingUnitMetrics = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val metrics = KubernetesClient.getPodMetrics(unit.getCuid)
        WorkflowComputingUnitMetrics(
          metrics.getOrElse("cpu", ""),
          metrics.getOrElse("memory", "")
        )
      case _ =>
        WorkflowComputingUnitMetrics("NaN", "NaN")
    }
  }

  private def getComputingUnitResourceLimit(
      unit: WorkflowComputingUnit
  ): WorkflowComputingUnitResourceLimit = {
    unit.getType match {
      case WorkflowComputingUnitTypeEnum.local =>
        WorkflowComputingUnitResourceLimit("NaN", "NaN", "NaN")
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        val podLimits: Map[String, String] = KubernetesClient.getPodLimits(unit.getCuid)

        // Get GPU value by finding the exact configured resource key
        val gpuValue = podLimits.getOrElse(KubernetesConfig.gpuResourceKey, "0")

        WorkflowComputingUnitResourceLimit(
          podLimits("cpu"),
          podLimits("memory"),
          gpuValue
        )
    }
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/limits")
  def getComputingUnitLimitOptions(
      @Auth @unused user: SessionUser
  ): ComputingUnitLimitOptionsResponse = {
    ComputingUnitLimitOptionsResponse(cpuLimitOptions, memoryLimitOptions, gpuLimitOptions)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/types")
  def getComputingUnitTypes(
      @Auth @unused user: SessionUser
  ): ComputingUnitTypesResponse = ComputingUnitTypesResponse(getSupportedComputingUnitTypes)

  /**
    * Create a new pod for the given user ID.
    *
    * @param param The parameters containing the user ID.
    * @return The created pod or an error response.
    */
  @POST
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/create")
  def createWorkflowComputingUnit(
      param: WorkflowComputingUnitCreationParams,
      @Auth user: SessionUser
  ): DashboardWorkflowComputingUnit = {
    if (param.name.trim.isEmpty) {
      throw new ForbiddenException("Computing unit name cannot be empty.")
    }

    // Validate the unit type
    val cuType: WorkflowComputingUnitTypeEnum =
      WorkflowComputingUnitTypeEnum.lookupLiteral(param.unitType)

    // Validate that the type itself is supported
    if (!getSupportedComputingUnitTypes.contains(param.unitType))
      throw new ForbiddenException(
        s"Unit type '${param.unitType}' is not allowed. Valid options: " +
          getSupportedComputingUnitTypes.mkString(", ")
      )

    // For Kubernetes computing units, validate resource limits
    cuType match {

      // Kubernetes-specific checks
      case WorkflowComputingUnitTypeEnum.kubernetes =>
        if (!cpuLimitOptions.contains(param.cpuLimit))
          throw new ForbiddenException(
            s"CPU quantity '${param.cpuLimit}' is not allowed. " +
              s"Valid options: ${cpuLimitOptions.mkString(", ")}"
          )
        if (!memoryLimitOptions.contains(param.memoryLimit))
          throw new ForbiddenException(
            s"Memory quantity '${param.memoryLimit}' is not allowed. " +
              s"Valid options: ${memoryLimitOptions.mkString(", ")}"
          )
        if (!gpuLimitOptions.contains(param.gpuLimit))
          throw new ForbiddenException(
            s"GPU quantity '${param.gpuLimit}' is not allowed. " +
              s"Valid options: ${gpuLimitOptions.mkString(", ")}"
          )

        // Check if the shared-memory size is the valid size representation
        val shmQuantity =
          try {
            Quantity.parse(param.shmSize)
          } catch {
            case _: IllegalArgumentException =>
              throw new ForbiddenException(
                s"Shared-memory size '${param.shmSize}' is not a valid Kubernetes quantity " +
                  s"(examples: 64Mi, 2Gi)."
              )
          }

        val memQuantity = Quantity.parse(param.memoryLimit)

        // ensure /dev/shm upper bound ≤ container memory limit
        if (shmQuantity.compareTo(memQuantity) > 0)
          throw new ForbiddenException(
            s"Shared-memory size (${param.shmSize}) cannot exceed the total memory limit " +
              s"(${param.memoryLimit})."
          )

        // JVM heap ≤ total memory
        val jvmGB = param.jvmMemorySize.replaceAll("[^0-9]", "").toInt
        val memGB =
          if (param.memoryLimit.endsWith("Gi")) param.memoryLimit.replaceAll("[^0-9]", "").toInt
          else if (param.memoryLimit.endsWith("Mi"))
            param.memoryLimit.replaceAll("[^0-9]", "").toInt / 1024
          else param.memoryLimit.replaceAll("[^0-9]", "").toInt

        if (jvmGB > memGB)
          throw new ForbiddenException(
            s"JVM memory size (${param.jvmMemorySize}) cannot exceed the " +
              s"total memory limit (${param.memoryLimit})."
          )

      // Local-specific checks
      case WorkflowComputingUnitTypeEnum.local =>
        if (param.uri.forall(_.trim.isEmpty))
          throw new ForbiddenException("URI is required for local computing units")

      // Anything else (shouldn't happen if you keep supported types in sync)
      case _ =>
        throw new ForbiddenException(s"Unsupported computing-unit type: ${param.unitType}")
    }

    withTransaction(context) { ctx =>
      val wcDao = new WorkflowComputingUnitDao(ctx.configuration())

      val units = wcDao
        .fetchByUid(user.getUid)
        .asScala
        .filter(_.getTerminateTime == null) // Filter out terminated units

      if (
        units.size >= maxNumOfRunningComputingUnitsPerUser && cuType == WorkflowComputingUnitTypeEnum.kubernetes
      ) {
        throw InsufficientComputingUnitQuota(maxNumOfRunningComputingUnitsPerUser)
      }

      val resourceJson: String = cuType match {
        // ── Kubernetes CU ───────────────────────────────────────
        case WorkflowComputingUnitTypeEnum.kubernetes =>
          Json.stringify(
            Json.obj(
              "cpuLimit" -> param.cpuLimit,
              "memoryLimit" -> param.memoryLimit,
              "gpuLimit" -> param.gpuLimit,
              "jvmMemorySize" -> param.jvmMemorySize,
              "shmSize" -> param.shmSize,
              "nodeAddresses" -> Json.arr() // filled in later
            )
          )

        // ── Local CU ─────────────────────────────────────────────
        case WorkflowComputingUnitTypeEnum.local =>
          Json.stringify(
            Json.obj(
              "cpuLimit" -> "NaN",
              "memoryLimit" -> "NaN",
              "gpuLimit" -> "NaN",
              "jvmMemorySize" -> "NaN",
              "shmSize" -> "NaN",
              // user-supplied URI goes straight in
              "nodeAddresses" -> Json.arr(param.uri.get)
            )
          )
        case _ => "{}"
      }

      val computingUnit = new WorkflowComputingUnit()
      val userToken = JwtAuth.jwtToken(jwtClaims(user.user, TOKEN_EXPIRE_TIME_IN_MINUTES))
      computingUnit.setUid(user.getUid)
      computingUnit.setName(param.name)
      computingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))
      computingUnit.setType(WorkflowComputingUnitTypeEnum.lookupLiteral(param.unitType))
      computingUnit.setResource(resourceJson)

      // Set URI during initial insert for local only
      if (cuType == WorkflowComputingUnitTypeEnum.local) {
        computingUnit.setUri(param.uri.get)
      } else {
        computingUnit.setUri("") // placeholder for kubernetes
      }

      wcDao.insert(computingUnit)

      // Retrieve generated cuid
      val cuid = ctx.lastID().intValue()
      val insertedUnit = wcDao.fetchOneByCuid(cuid)

      if (cuType == WorkflowComputingUnitTypeEnum.kubernetes && insertedUnit != null) {
        // 1. Update the DB with the URI
        insertedUnit.setUri(KubernetesClient.generatePodURI(cuid))

        val updatedResource: JsObject =
          Json
            .parse(insertedUnit.getResource)
            .as[JsObject] ++
            Json.obj("nodeAddresses" -> Json.arr(insertedUnit.getUri))

        insertedUnit.setResource(Json.stringify(updatedResource))
        wcDao.update(insertedUnit)

        // 2. Launch the pod as CU
        try {
          KubernetesClient.createPod(
            cuid,
            param.cpuLimit,
            param.memoryLimit,
            param.gpuLimit,
            computingUnitEnvironmentVariables ++ Map(
              EnvironmentalVariable.ENV_USER_JWT_TOKEN -> userToken,
              EnvironmentalVariable.ENV_JAVA_OPTS -> s"-Xmx${param.jvmMemorySize}"
            ),
            Some(param.shmSize)
          )

        } catch {
          case e: KubernetesClientException =>
            throw ComputingUnitManagingServiceException.fromKubernetes(e)

          case t: Throwable =>
            throw t
        }
      }

      DashboardWorkflowComputingUnit(
        insertedUnit,
        getComputingUnitStatus(insertedUnit).toString,
        getComputingUnitMetrics(insertedUnit),
        isOwner = true,
        accessPrivilege = PrivilegeEnum.WRITE
      )
    }
  }

  /**
    * List all computing units created by the current user.
    *
    * @return A list of computing units that are not terminated.
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("")
  def listComputingUnits(
      @Auth user: SessionUser
  ): List[DashboardWorkflowComputingUnit] = {
    withTransaction(context) { ctx =>
      val computingUnitDao = new WorkflowComputingUnitDao(ctx.configuration())
      val uid = user.getUid

      // Always fetch units owned by the user
      val ownedUnits = computingUnitDao.fetchByUid(uid).asScala.toList

      // Conditionally fetch shared units based on the config flag
      val (sharedUnits, sharedUnitInfo) =
        if (ComputingUnitConfig.sharingComputingUnitEnabled) {
          val computingUnitUserAccessDao = new ComputingUnitUserAccessDao(ctx.configuration())
          val info = computingUnitUserAccessDao
            .fetchByUid(uid)
            .asScala
            .map(access => access.getCuid -> access.getPrivilege)
            .toMap
          val sharedCuids = info.keys.toList.map(Integer.valueOf(_))

          val units = if (sharedCuids.isEmpty) {
            List()
          } else {
            computingUnitDao.fetchByCuid(sharedCuids: _*).asScala.toList
          }
          (units, info)
        } else {
          // If sharing is disabled, return empty collections
          (List.empty[WorkflowComputingUnit], Map.empty[Integer, PrivilegeEnum])
        }

      val allUnits = ownedUnits ++ sharedUnits

      // If a Kubernetes pod has already disappeared (e.g., manually deleted or TTL
      // GC-ed by the cluster), we treat the corresponding computing unit as
      // terminated from the system's point of view. Here we eagerly update its
      // terminateTime in the database **before** we build the response list so
      // that subsequent API calls will no longer return this unit.
      allUnits.foreach { unit =>
        if (
          unit.getType == WorkflowComputingUnitTypeEnum.kubernetes &&
          !KubernetesClient.podExists(unit.getCuid)
        ) {
          unit.setTerminateTime(new Timestamp(System.currentTimeMillis()))
          computingUnitDao.update(unit)
        }
      }

      // For shared units, we need to check the access privilege which are saved in different table
      // to streamline the process, we combine owned units with default WRITE privilege and use sharedUnitInfo
      // to get the privilege for shared units.
      (ownedUnits.map(u => (u, PrivilegeEnum.WRITE)) ++ sharedUnits.map(u =>
        (u, sharedUnitInfo(u.getCuid))
      ))
        .distinctBy { case (unit, _) => unit.getCuid }
        .filter { case (unit, _) => unit.getTerminateTime == null }
        .filter {
          case (unit, _) =>
            unit.getType match {
              case WorkflowComputingUnitTypeEnum.kubernetes =>
                KubernetesClient.podExists(unit.getCuid)
              case _ => true
            }
        }
        .map {
          case (unit, privilege) =>
            DashboardWorkflowComputingUnit(
              computingUnit = unit,
              isOwner = unit.getUid.equals(uid),
              accessPrivilege = privilege,
              status = getComputingUnitStatus(unit).toString,
              metrics = getComputingUnitMetrics(unit)
            )
        }
    }
  }

  /**
    * Return a fully populated [[DashboardWorkflowComputingUnit]] for the
    * specified `cuid`, identical to one row produced by /list.
    *
    * @param cuid the ID of the computing-unit to fetch
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}")
  def getComputingUnitInfo(
      @PathParam("cuid") cuid: Integer,
      @Auth user: SessionUser
  ): DashboardWorkflowComputingUnit = {

    val unit = getComputingUnitByCuid(context, cuid)

    DashboardWorkflowComputingUnit(
      computingUnit = unit,
      status = getComputingUnitStatus(unit).toString,
      metrics = getComputingUnitMetrics(unit),
      isOwner = unit.getUid.equals(user.getUid),
      accessPrivilege = {
        val cuAccessDao = new ComputingUnitUserAccessDao(context.configuration())
        val access = cuAccessDao
          .fetchByUid(user.getUid)
          .asScala
          .find(access => access.getCuid.equals(cuid))

        if (access.isDefined) {
          access.get.getPrivilege
        } else if (unit.getUid.equals(user.getUid)) {
          PrivilegeEnum.WRITE
        } else {
          // Default privilege for non-owners without explicit access
          PrivilegeEnum.NONE
        }
      }
    )
  }

  /**
    * Terminate the computing unit's pod based on the pod URI.
    *
    * @return A response indicating success or failure.
    */
  @DELETE
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}/terminate")
  def terminateComputingUnit(
      @PathParam("cuid") cuid: Integer,
      @Auth user: SessionUser
  ): Response = {
    if (!userOwnComputingUnit(context, cuid, user.getUid)) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity(s"User has no access to the computing unit")
        .build()
    }

    // If successful, update the database
    withTransaction(context) { ctx =>
      val cuDao = new WorkflowComputingUnitDao(ctx.configuration())
      val unit = getComputingUnitByCuid(ctx, cuid)

      // if the computing unit is kubernetes pod, then kill the pod
      if (unit.getType == WorkflowComputingUnitTypeEnum.kubernetes) {
        KubernetesClient.deletePod(cuid)
      }

      unit.setTerminateTime(new Timestamp(System.currentTimeMillis()))
      cuDao.update(unit)
    }
    Response.ok().build()
  }

  /**
    * Rename a computing unit.
    *
    * @param cuid The computing unit ID.
    * @param name The new name for the computing unit.
    * @param user The authenticated user.
    * @return A response indicating success or failure.
    */
  @PUT
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Consumes(Array(MediaType.APPLICATION_JSON))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}/rename/{name}")
  def renameComputingUnit(
      @PathParam("cuid") cuid: Integer,
      @PathParam("name") name: String,
      @Auth user: SessionUser
  ): Response = {
    // Verify ownership or write access
    if (
      !userOwnComputingUnit(context, cuid, user.getUid) &&
      !ComputingUnitAccessResource.hasWriteAccess(cuid, user.getUid)
    ) {
      return Response
        .status(Response.Status.FORBIDDEN)
        .entity("User does not have permission to rename this computing unit")
        .build()
    }

    // Validate name
    if (StringUtils.isBlank(name)) {
      return Response
        .status(Response.Status.BAD_REQUEST)
        .entity("Computing unit name cannot be empty or blank")
        .build()
    }

    withTransaction(context) { ctx =>
      val cuDao = new WorkflowComputingUnitDao(ctx.configuration())
      val unit = getComputingUnitByCuid(ctx, cuid)

      try {
        unit.setName(name)
        cuDao.update(unit)
      } catch {
        case e: Exception =>
          return Response
            .status(Response.Status.INTERNAL_SERVER_ERROR)
            .entity(e.getMessage)
            .build()
      }
    }

    Response.ok().build()
  }

  /**
    * Retrieves the CPU and memory metrics for a computing unit identified by its `cuid`.
    *
    * @param cuid The computing unit ID.
    * @return A `WorkflowComputingUnitMetrics` object with CPU and memory usage data.
    */
  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}/metrics")
  def getComputingUnitMetricsEndpoint(
      @PathParam("cuid") cuid: String,
      @Auth user: SessionUser
  ): WorkflowComputingUnitMetrics = {
    if (!userOwnComputingUnit(context, cuid.toInt, user.getUid)) {
      throw new BadRequestException("User has no access to the computing unit")
    }
    val computingUnit = getComputingUnitByCuid(context, cuid.toInt)
    getComputingUnitMetrics(computingUnit)
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/{cuid}/limits")
  def getComputingUnitResourceLimit(
      @PathParam("cuid") cuid: String,
      @Auth user: SessionUser
  ): WorkflowComputingUnitResourceLimit = {
    if (!userOwnComputingUnit(context, cuid.toInt, user.getUid)) {
      throw new BadRequestException("User has no access to the computing unit")
    }
    val computingUnit = getComputingUnitByCuid(context, cuid.toInt)
    getComputingUnitResourceLimit(computingUnit)
  }
}
