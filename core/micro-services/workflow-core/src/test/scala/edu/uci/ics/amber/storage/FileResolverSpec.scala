package edu.uci.ics.amber.storage

import edu.uci.ics.amber.core.storage.FileResolver
import edu.uci.ics.texera.dao.MockTexeraDB
import edu.uci.ics.texera.dao.jooq.generated.enums.UserRole
import edu.uci.ics.texera.dao.jooq.generated.tables.daos.{DatasetDao, DatasetVersionDao, UserDao}
import edu.uci.ics.texera.dao.jooq.generated.tables.pojos.{Dataset, DatasetVersion, User}
import org.apache.commons.vfs2.FileNotFoundException
import org.jooq.types.UInteger
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Paths

class FileResolverSpec
    extends AnyFlatSpec
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with MockTexeraDB {

  private val testUser: User = {
    val user = new User
    user.setUid(UInteger.valueOf(1))
    user.setName("test_user")
    user.setRole(UserRole.ADMIN)
    user.setPassword("123")
    user.setEmail("test_user@test.com")
    user
  }

  private val testDataset: Dataset = {
    val dataset = new Dataset
    dataset.setDid(UInteger.valueOf(1))
    dataset.setName("test_dataset")
    dataset.setDescription("dataset for test")
    dataset.setIsPublic(1.toByte)
    dataset.setOwnerUid(UInteger.valueOf(1))
    dataset
  }

  private val testDatasetVersion1: DatasetVersion = {
    val datasetVersion = new DatasetVersion
    datasetVersion.setDid(UInteger.valueOf(1))
    datasetVersion.setName("v1")
    datasetVersion.setDvid(UInteger.valueOf(1))
    datasetVersion.setCreatorUid(UInteger.valueOf(1))
    datasetVersion.setVersionHash("97fd4c2a755b69b7c66d322eab40b7e5c2ad5d10")
    datasetVersion
  }

  private val testDatasetVersion2: DatasetVersion = {
    val datasetVersion = new DatasetVersion
    datasetVersion.setDid(UInteger.valueOf(1))
    datasetVersion.setName("v2")
    datasetVersion.setDvid(UInteger.valueOf(2))
    datasetVersion.setCreatorUid(UInteger.valueOf(1))
    datasetVersion.setVersionHash("37966c92cb3a8bee1f9d8e21937aa8faa5e48513")
    datasetVersion
  }

  private val localCsvFilePath = "workflow-core/src/test/resources/country_sales_small.csv"

  private val datasetACsvFilePath = "/test_user@test.com/test_dataset/v2/directory/a.csv"

  private val dataset1TxtFilePath = "/test_user@test.com/test_dataset/v1/1.txt"

  override protected def beforeAll(): Unit = {
    initializeDBAndReplaceDSLContext()

    // add test user
    val userDao = new UserDao(getDSLContext.configuration())
    userDao.insert(testUser)

    // add test dataset
    val datasetDao = new DatasetDao(getDSLContext.configuration())
    datasetDao.insert(testDataset)

    // add test dataset versions
    val datasetVersionDao = new DatasetVersionDao(getDSLContext.configuration())
    datasetVersionDao.insert(testDatasetVersion1)
    datasetVersionDao.insert(testDatasetVersion2)
  }

  "FileResolver" should "resolve local file correctly" in {
    val localUri = FileResolver.resolve(localCsvFilePath)

    assert(localUri == Paths.get(localCsvFilePath).toUri)
  }

  "FileResolver" should "resolve dataset file correctly" in {
    val datasetACsvUri = FileResolver.resolve(datasetACsvFilePath)
    val dataset1TxtUri = FileResolver.resolve(dataset1TxtFilePath)

    assert(
      datasetACsvUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getDid}/${testDatasetVersion2.getVersionHash}/directory/a.csv"
    )
    assert(
      dataset1TxtUri.toString == f"${FileResolver.DATASET_FILE_URI_SCHEME}:///${testDataset.getDid}/${testDatasetVersion1.getVersionHash}/1.txt"
    )
  }

  "FileResolver" should "throw not found exception" in {
    assertThrows[FileNotFoundException] {
      FileResolver.resolve("some/random/path")
    }
  }

  override protected def afterAll(): Unit = {
    shutdownDB()
  }

}
