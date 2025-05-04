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

import edu.uci.ics.amber.core.storage.{EnvironmentalVariable, StorageConfig}
import edu.uci.ics.texera.auth.JwtAuth.{TOKEN_EXPIRE_TIME_IN_DAYS, dayToMin, jwtClaims}
import edu.uci.ics.texera.auth.{JwtAuth, SessionUser}
import edu.uci.ics.texera.dao.SqlServer
import edu.uci.ics.texera.dao.SqlServer.withTransaction
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.WorkflowComputingUnitDao
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.WorkflowComputingUnit
import edu.uci.ics.texera.service.KubernetesConfig
import edu.uci.ics.texera.service.KubernetesConfig.{
  cpuLimitOptions,
  gpuLimitOptions,
  maxNumOfRunningComputingUnitsPerUser,
  memoryLimitOptions
}
import edu.uci.ics.texera.service.resource.ComputingUnitManagingResource._
import edu.uci.ics.texera.service.util.KubernetesClient
import io.dropwizard.auth.Auth
import jakarta.annotation.security.RolesAllowed
import jakarta.ws.rs._
import jakarta.ws.rs.core.{MediaType, Response}
import org.jooq.DSLContext

import java.sql.Timestamp
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

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
      .get
  )

  def userOwnComputingUnit(ctx: DSLContext, cuid: Integer, uid: Integer): Boolean = {
    val computingUnitDao = new WorkflowComputingUnitDao(ctx.configuration())

    Option(computingUnitDao.fetchOneByCuid(cuid))
      .exists(_.getUid == uid)
  }

  case class WorkflowComputingUnitCreationParams(
      name: String,
      unitType: String,
      cpuLimit: String,
      memoryLimit: String,
      gpuLimit: String,
      jvmMemorySize: String
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
      uri: String,
      status: String,
      metrics: WorkflowComputingUnitMetrics,
      resourceLimits: WorkflowComputingUnitResourceLimit
  )

  case class ComputingUnitLimitOptionsResponse(
      cpuLimitOptions: List[String],
      memoryLimitOptions: List[String],
      gpuLimitOptions: List[String]
  )
}

@Produces(Array(MediaType.APPLICATION_JSON))
@Path("/computing-unit")
class ComputingUnitManagingResource {

  private def getComputingUnitMetrics(cuid: Int): WorkflowComputingUnitMetrics = {
    val metrics: Map[String, String] = KubernetesClient.getPodMetrics(cuid)

    WorkflowComputingUnitMetrics(
      metrics.getOrElse("cpu", ""),
      metrics.getOrElse("memory", "")
    )
  }

  private def getComputingUnitResourceLimit(cuid: Int): WorkflowComputingUnitResourceLimit = {
    val podLimits: Map[String, String] = KubernetesClient.getPodLimits(cuid)

    // Get GPU value by finding the exact configured resource key
    val gpuValue = podLimits.getOrElse(KubernetesConfig.gpuResourceKey, "0")

    WorkflowComputingUnitResourceLimit(
      podLimits.getOrElse("cpu", ""),
      podLimits.getOrElse("memory", ""),
      gpuValue
    )
  }

  @GET
  @RolesAllowed(Array("REGULAR", "ADMIN"))
  @Produces(Array(MediaType.APPLICATION_JSON))
  @Path("/limits")
  def getComputingUnitLimitOptions(
      @Auth user: SessionUser
  ): ComputingUnitLimitOptionsResponse = {
    ComputingUnitLimitOptionsResponse(cpuLimitOptions, memoryLimitOptions, gpuLimitOptions)
  }

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
    if (!cpuLimitOptions.contains(param.cpuLimit)) {
      throw new ForbiddenException(
        s"CPU quantity '${param.cpuLimit}' is not allowed. Valid options: ${cpuLimitOptions.mkString(", ")}"
      )
    }
    if (!memoryLimitOptions.contains(param.memoryLimit)) {
      throw new ForbiddenException(
        s"Memory quantity '${param.memoryLimit}' is not allowed. Valid options: ${memoryLimitOptions
          .mkString(", ")}"
      )
    }
    if (!gpuLimitOptions.contains(param.gpuLimit)) {
      throw new ForbiddenException(
        s"GPU quantity '${param.gpuLimit}' is not allowed. Valid options: ${gpuLimitOptions
          .mkString(", ")}"
      )
    }

    // Validate JVM memory size against the selected memory limit
    val jvmMemorySizeValue = param.jvmMemorySize.replaceAll("[^0-9]", "").toInt
    val memoryLimitValue = {
      val memValue = param.memoryLimit
      if (memValue.endsWith("Gi")) {
        memValue.replaceAll("[^0-9]", "").toInt
      } else if (memValue.endsWith("Mi")) {
        memValue.replaceAll("[^0-9]", "").toInt / 1024
      } else {
        // Default case, assume value is in GB
        memValue.replaceAll("[^0-9]", "").toInt
      }
    }

    if (jvmMemorySizeValue > memoryLimitValue) {
      throw new ForbiddenException(
        s"JVM memory size (${param.jvmMemorySize}) cannot exceed the total memory limit (${param.memoryLimit})"
      )
    }

    try {
      withTransaction(context) { ctx =>
        val wcDao = new WorkflowComputingUnitDao(ctx.configuration())

        val units = wcDao
          .fetchByUid(user.getUid)
          .filter(_.getTerminateTime == null) // Filter out terminated units

        if (units.size >= maxNumOfRunningComputingUnitsPerUser) {
          throw new BadRequestException(
            s"You can only have at most ${maxNumOfRunningComputingUnitsPerUser} running at the same time"
          )
        }

        val computingUnit = new WorkflowComputingUnit()
        val userToken = JwtAuth.jwtToken(jwtClaims(user.user, dayToMin(TOKEN_EXPIRE_TIME_IN_DAYS)))
        computingUnit.setUid(user.getUid)
        computingUnit.setName(param.name)
        computingUnit.setCreationTime(new Timestamp(System.currentTimeMillis()))

        // Insert using the DAO
        wcDao.insert(computingUnit)

        // Retrieve the generated CUID
        val cuid = ctx.lastID().intValue()
        val insertedUnit = wcDao.fetchOneByCuid(cuid)

        // Create the pod with the generated CUID
        val pod = KubernetesClient.createPod(
          cuid,
          param.cpuLimit,
          param.memoryLimit,
          param.gpuLimit,
          computingUnitEnvironmentVariables ++ Map(
            EnvironmentalVariable.ENV_USER_JWT_TOKEN -> userToken,
            EnvironmentalVariable.ENV_JAVA_OPTS -> s"-Xmx${param.jvmMemorySize}"
          )
        )

        // Return the dashboard response
        DashboardWorkflowComputingUnit(
          insertedUnit,
          KubernetesClient.generatePodURI(cuid),
          pod.getStatus.getPhase,
          getComputingUnitMetrics(cuid),
          WorkflowComputingUnitResourceLimit(param.cpuLimit, param.memoryLimit, param.gpuLimit)
        )
      }
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

      val units = computingUnitDao
        .fetchByUid(user.getUid)
        .filter(_.getTerminateTime == null) // Filter out terminated units

      units.map { unit =>
        val cuid = unit.getCuid.intValue()
        val podName = KubernetesClient.generatePodName(cuid)
        val pod = KubernetesClient.getPodByName(podName)

        DashboardWorkflowComputingUnit(
          computingUnit = unit,
          uri = KubernetesClient.generatePodURI(cuid),
          status = pod.map(_.getStatus.getPhase).getOrElse("Unknown"),
          metrics = getComputingUnitMetrics(cuid),
          resourceLimits = getComputingUnitResourceLimit(cuid)
        )
      }.toList
    }
  }

  /**
    * Terminate the computing unit's pod based on the pod URI.
    *
    * @param param The parameters containing the pod URI.
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

    KubernetesClient.deletePod(cuid)

    // If successful, update the database
    withTransaction(context) { ctx =>
      val cuDao = new WorkflowComputingUnitDao(ctx.configuration())
      val units = cuDao.fetchByCuid(cuid)

      units.forEach(unit => unit.setTerminateTime(new Timestamp(System.currentTimeMillis())))
      cuDao.update(units)
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
  def getComputingUnitMetrics(
      @PathParam("cuid") cuid: String,
      @Auth user: SessionUser
  ): WorkflowComputingUnitMetrics = {
    if (!userOwnComputingUnit(context, cuid.toInt, user.getUid)) {
      throw new BadRequestException("User has no access to the computing unit")
    }
    getComputingUnitMetrics(cuid.toInt)
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
    getComputingUnitResourceLimit(cuid.toInt)
  }
}
