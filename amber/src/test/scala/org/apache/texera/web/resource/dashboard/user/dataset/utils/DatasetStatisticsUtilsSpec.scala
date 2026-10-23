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

package org.apache.texera.web.resource.dashboard.user.dataset.utils

import org.apache.texera.dao.MockTexeraDB
import org.apache.texera.dao.jooq.generated.Tables.{DATASET, USER}
import org.apache.texera.dao.jooq.generated.tables.daos.{DatasetDao, UserDao}
import org.apache.texera.dao.jooq.generated.tables.pojos.{Dataset, User}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.sql.Timestamp
import java.util.UUID

/**
  * Unit tests for [[DatasetStatisticsUtils]].
  *
  * `DatasetStatisticsUtils.context` resolves through `SqlServer.getInstance()`, and
  * `MockTexeraDB.initializeDBAndReplaceDSLContext()` is exactly what re-points that
  * singleton at this suite's isolated embedded-Postgres database, so no seam is
  * needed - the object is exercised as production calls it.
  *
  * Every test seeds two distinct owners, because a fixture with a single owner
  * cannot tell an owner-filtered query apart from an unfiltered one, and the
  * owner's own datasets differ in every visibility flag, because a fixture whose
  * rows are uniformly private (or uniformly downloadable) cannot tell the owner
  * filter apart from an owner filter plus a spurious visibility predicate.
  */
class DatasetStatisticsUtilsSpec extends AnyFlatSpec with BeforeAndAfterAll with MockTexeraDB {

  private val ownerUid = 7000 + scala.util.Random.nextInt(1000)
  private val otherUid = ownerUid + 1
  private val strangerUid = ownerUid + 2

  private var userDao: UserDao = _
  private var datasetDao: DatasetDao = _

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()
    userDao = new UserDao(getDSLContext.configuration())
    datasetDao = new DatasetDao(getDSLContext.configuration())
  }

  override protected def afterAll(): Unit = shutdownDB()

  private def resetFixtures(): Unit = {
    Seq(ownerUid, otherUid, strangerUid).foreach { uid =>
      getDSLContext.deleteFrom(DATASET).where(DATASET.OWNER_UID.eq(uid)).execute()
      getDSLContext.deleteFrom(USER).where(USER.UID.eq(uid)).execute()
    }
    Seq(ownerUid, otherUid, strangerUid).foreach(insertUser)
  }

  private def insertUser(uid: Int): Unit = {
    val user = new User
    user.setUid(uid)
    user.setName(s"stats_user_$uid")
    user.setEmail(s"stats_user_$uid@example.com")
    userDao.insert(user)
  }

  private def insertDataset(
      uid: Int,
      creationTime: Timestamp,
      isPublic: Boolean = false,
      isDownloadable: Boolean = true
  ): Dataset = {
    val dataset = new Dataset
    dataset.setOwnerUid(uid)
    dataset.setName("stats_ds_" + UUID.randomUUID().toString.substring(0, 8))
    dataset.setRepositoryName("repo-" + UUID.randomUUID().toString.substring(0, 8))
    dataset.setIsPublic(isPublic)
    dataset.setIsDownloadable(isDownloadable)
    dataset.setDescription("")
    dataset.setCreationTime(creationTime)
    datasetDao.insert(dataset)
    dataset
  }

  private def now(): Timestamp = new Timestamp(System.currentTimeMillis())

  private def didOf(dataset: Dataset): Integer =
    datasetDao.fetchByName(dataset.getName).get(0).getDid

  /**
    * Two datasets for `ownerUid` whose visibility flags disagree on both axes, one
    * for `otherUid`, none for `strangerUid`. Both queries under test filter on
    * OWNER_UID only, so any visibility predicate added to either of them would drop
    * one of the owner's two rows.
    */
  private def seedTwoOwners(): Unit = {
    resetFixtures()
    insertDataset(ownerUid, now(), isPublic = true, isDownloadable = false)
    insertDataset(ownerUid, now(), isPublic = false, isDownloadable = true)
    insertDataset(otherUid, now())
  }

  // ---------------------------------------------------------------------------
  // getUserCreatedDatasetCount
  // ---------------------------------------------------------------------------

  "getUserCreatedDatasetCount" should
    "aggregate only the datasets owned by the given uid, and report 0 for a uid that owns none" in {
    seedTwoOwners()

    // The zero case shares this fixture on purpose: on its own it cannot
    // distinguish a count from a null projection, and on its own the two-dataset
    // case cannot distinguish an owner-filtered count from an unfiltered one.
    assert(DatasetStatisticsUtils.getUserCreatedDatasetCount(ownerUid) == 2)
    assert(DatasetStatisticsUtils.getUserCreatedDatasetCount(otherUid) == 1)
    assert(DatasetStatisticsUtils.getUserCreatedDatasetCount(strangerUid) == 0)
  }

  it should "agree with the length of the list getUserCreatedDatasets returns" in {
    seedTwoOwners()

    Seq(ownerUid, otherUid, strangerUid).foreach { uid =>
      assert(
        DatasetStatisticsUtils.getUserCreatedDatasetCount(uid) ==
          DatasetStatisticsUtils.getUserCreatedDatasets(uid).size
      )
    }
  }

  // ---------------------------------------------------------------------------
  // getUserCreatedDatasets
  // ---------------------------------------------------------------------------

  "getUserCreatedDatasets" should "return an empty list for a uid that owns no datasets" in {
    resetFixtures()
    insertDataset(otherUid, now())

    assert(DatasetStatisticsUtils.getUserCreatedDatasets(strangerUid).isEmpty)
  }

  it should "project did, name and creation time of the owner's datasets, with size forced to 0" in {
    resetFixtures()
    // Two owned rows with distinct names AND distinct creation times, so a mapping
    // that reads any field off one fixed record instead of the record it is
    // projecting (the classic row-collapse regression) cannot pass. They also
    // disagree on both visibility flags, so neither query may filter on those.
    val firstTime = new Timestamp(1_700_000_000_000L)
    val secondTime = new Timestamp(1_500_000_000_000L)
    val owned = insertDataset(ownerUid, firstTime, isPublic = true, isDownloadable = false)
    val alsoOwned = insertDataset(ownerUid, secondTime)
    insertDataset(otherUid, now())

    val quotas = DatasetStatisticsUtils.getUserCreatedDatasets(ownerUid)

    assert(quotas.size == 2)
    // Size reporting is deliberately disabled; a non-zero value would mean the
    // dataset-size computation was silently re-enabled.
    assert(
      quotas.map(q => (q.did, q.name, q.creationTime, q.size)).toSet ==
        Set(
          (didOf(owned), owned.getName, firstTime.getTime, 0L),
          (didOf(alsoOwned), alsoOwned.getName, secondTime.getTime, 0L)
        )
    )
  }
}
