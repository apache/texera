package edu.uci.ics.texera.web.resource.dashboard.user.dataset;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorageService;
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.type.FileNode;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GitVersionControlLocalFileStorageServiceSpec {

  private Path testRepoPath;

  private String testRepoMainBranchId;

  private List<String> testRepoMasterCommitHashes;
  private final String testFile1Name = "testFile1.txt";

  private final String testFile2Name = "testFile2.txt";
  private final String testDirectoryName = "testDir";

  private final String testFile1ContentV1 = "This is a test file1 v1";
  private final String testFile1ContentV2 = "This is a test file1 v2";
  private final String testFile1ContentV3 = "This is a test file1 v3";

  private final String testFile2Content = "This is a test file2 in the testDir";

  private void writeFileToRepo(Path filePath, String fileContent) throws IOException {
    try (ByteArrayInputStream input = new ByteArrayInputStream(fileContent.getBytes())) {
      GitVersionControlLocalFileStorageService.writeFile(filePath, input);
    }
  }

  @Before
  public void setUp() throws IOException, GitAPIException {
    // Create a temporary directory for the repository
    testRepoPath = Files.createTempDirectory("testRepo");
    testRepoMainBranchId = GitVersionControlLocalFileStorageService.initRepo(testRepoPath);

    // Version 1
    Path file1Path = testRepoPath.resolve(testFile1Name);
    writeFileToRepo(file1Path, testFile1ContentV1);
    String v1Commit = GitVersionControlLocalFileStorageService.createVersion(testRepoPath, "v1");

    // Version 2
    writeFileToRepo(file1Path, testFile1ContentV2);
    String v2Commit = GitVersionControlLocalFileStorageService.createVersion(testRepoPath, "v2");

    // Version 3
    writeFileToRepo(file1Path, testFile1ContentV3);
    String v3Commit = GitVersionControlLocalFileStorageService.createVersion(testRepoPath, "v3");

    // Retrieve all commit hashes after creating versions
    testRepoMasterCommitHashes = new ArrayList<>() {{
      add(v1Commit);
      add(v2Commit);
      add(v3Commit);
    }};
  }

  @After
  public void tearDown() throws IOException {
    // Clean up the test repository directory
    GitVersionControlLocalFileStorageService.deleteDirectory(testRepoPath);
  }

  @Test
  public void testFileContentAcrossVersions() throws IOException, GitAPIException {
    // File path for the test file
    Path filePath = testRepoPath.resolve(testFile1Name);

    // testRepoMasterCommitHashes is populated in chronological order: v1, v2, v3
    // Retrieve and compare file content for version 1
    ByteArrayOutputStream outputV1 = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorageService.retrieveFileContentOfVersion(testRepoPath, testRepoMasterCommitHashes.get(0), filePath, outputV1);
    String retrievedContentV1 = outputV1.toString();
    Assert.assertEquals(
        "Content for version 1 does not match",
        testFile1ContentV1,
        retrievedContentV1);

    // Retrieve and compare file content for version 2
    ByteArrayOutputStream outputV2 = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorageService.retrieveFileContentOfVersion(testRepoPath, testRepoMasterCommitHashes.get(1), filePath, outputV2);
    String retrievedContentV2 = outputV2.toString();
    Assert.assertEquals(
        "Content for version 2 does not match",
        testFile1ContentV2,
        retrievedContentV2);

    // Retrieve and compare file content for version 3
    ByteArrayOutputStream outputV3 = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorageService.retrieveFileContentOfVersion(testRepoPath, testRepoMasterCommitHashes.get(2), filePath, outputV3);
    String retrievedContentV3 = outputV3.toString();
    Assert.assertEquals(
        "Content for version 3 does not match",
        testFile1ContentV3,
        retrievedContentV3);
  }

  @Test
  public void testFileTreeRetrieval() throws Exception {
    // File path for the test file
    Path file1Path = testRepoPath.resolve(testFile1Name);

    Set<FileNode> fileNodes = new HashSet<>() {{
      add(new FileNode(file1Path));
    }};

    // first retrieve the latest version's file tree
    Assert.assertEquals("File Tree should match",
        fileNodes,
        GitVersionControlLocalFileStorageService.retrieveFileTreeOfVersion(testRepoPath, testRepoMasterCommitHashes.get(testRepoMasterCommitHashes.size() - 1)));

    // now we add a new file testDir/testFile2.txt
    Path testDirPath = testRepoPath.resolve(testDirectoryName);
    Path file2Path = testDirPath.resolve(testFile2Name);
    writeFileToRepo(file2Path, testFile2Content);

    String v4Hash = GitVersionControlLocalFileStorageService.createVersion(testRepoPath, "v4");
    testRepoMasterCommitHashes.add(v4Hash);

    FileNode dirNode = new FileNode(testDirPath);
    dirNode.addChildNode(new FileNode(file2Path));
    // update the expected fileNodes
    fileNodes.add(dirNode);

    // check the file tree
    Assert.assertEquals(
        "File Tree should match",
        fileNodes,
        GitVersionControlLocalFileStorageService.retrieveFileTreeOfVersion(testRepoPath, v4Hash));
  }

  @Test
  public void testUncommittedCheckAndRecoverToLatest() throws Exception {
    Path tempFilePath = testRepoPath.resolve("tempFile");
    String content = "some random content";
    writeFileToRepo(tempFilePath, content);

    Assert.assertTrue(
        "There should be some uncommitted changes",
        GitVersionControlLocalFileStorageService.hasUncommittedChanges(testRepoPath));

    GitVersionControlLocalFileStorageService.discardUncommittedChanges(testRepoPath);

    Assert.assertFalse("There should be no uncommitted changes",
        GitVersionControlLocalFileStorageService.hasUncommittedChanges(testRepoPath));
  }
}
