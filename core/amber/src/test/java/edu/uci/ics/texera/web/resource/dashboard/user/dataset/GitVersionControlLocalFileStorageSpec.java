package edu.uci.ics.texera.web.resource.dashboard.user.dataset;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorage;
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.type.PhysicalFileNode;
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

public class GitVersionControlLocalFileStorageSpec {

  private Path testRepoPath;

  private List<String> testRepoMasterCommitHashes;
  private final String testFile1Name = "testFile1.txt";

  private final String testFile2Name = "testFile2.txt";
  private final String testDirectoryName = "testDir";

  private final String testFile1ContentV1 = "This is a test file1 v1";
  private final String testFile1ContentV2 = "This is a test file1 v2";
  private final String testFile1ContentV3 = "This is a test file1 v3";

  private final String testFile2Content = "This is a test file2 in the testDir";

  private void writeFileToRepo(Path filePath, String fileContent) throws IOException, GitAPIException {
    try (ByteArrayInputStream input = new ByteArrayInputStream(fileContent.getBytes())) {
      GitVersionControlLocalFileStorage.writeFileToRepo(testRepoPath, filePath, input);
    }
  }

  @Before
  public void setUp() throws IOException, GitAPIException {
    // Create a temporary directory for the repository
    testRepoPath = Files.createTempDirectory("testRepo");
    GitVersionControlLocalFileStorage.initRepo(testRepoPath);

    Path file1Path = testRepoPath.resolve(testFile1Name);
    // Version 1
    String v1Hash = GitVersionControlLocalFileStorage.withCreateVersion(
        testRepoPath,
        "v1",
        () -> {
          try {
            writeFileToRepo(file1Path, testFile1ContentV1);
          } catch (IOException | GitAPIException e) {
            throw new RuntimeException(e);
          }
        });

    String v2Hash = GitVersionControlLocalFileStorage.withCreateVersion(
        testRepoPath,
        "v2",
        () -> {
          try {
            writeFileToRepo(file1Path, testFile1ContentV2);
          } catch (IOException | GitAPIException e) {
            throw new RuntimeException(e);
          }});

    // Version 3
    String v3Hash = GitVersionControlLocalFileStorage.withCreateVersion(
        testRepoPath,
        "v3",
        () -> {
          try {
            writeFileToRepo(file1Path, testFile1ContentV3);
          } catch (IOException | GitAPIException e) {
            throw new RuntimeException(e);
          }});

    testRepoMasterCommitHashes = new ArrayList<String>() {{
      add(v1Hash);
      add(v2Hash);
      add(v3Hash);
    }};
  }

  @After
  public void tearDown() throws IOException {
    // Clean up the test repository directory
    GitVersionControlLocalFileStorage.deleteRepo(testRepoPath);
  }

  @Test
  public void testFileContentAcrossVersions() throws IOException, GitAPIException {
    // File path for the test file
    Path filePath = testRepoPath.resolve(testFile1Name);

    // testRepoMasterCommitHashes is populated in chronological order: v1, v2, v3
    // Retrieve and compare file content for version 1
    ByteArrayOutputStream outputV1 = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorage.retrieveFileContentOfVersion(testRepoPath, testRepoMasterCommitHashes.get(0), filePath, outputV1);
    String retrievedContentV1 = outputV1.toString();
    Assert.assertEquals(
        "Content for version 1 does not match",
        testFile1ContentV1,
        retrievedContentV1);

    // Retrieve and compare file content for version 2
    ByteArrayOutputStream outputV2 = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorage.retrieveFileContentOfVersion(testRepoPath, testRepoMasterCommitHashes.get(1), filePath, outputV2);
    String retrievedContentV2 = outputV2.toString();
    Assert.assertEquals(
        "Content for version 2 does not match",
        testFile1ContentV2,
        retrievedContentV2);

    // Retrieve and compare file content for version 3
    ByteArrayOutputStream outputV3 = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorage.retrieveFileContentOfVersion(testRepoPath, testRepoMasterCommitHashes.get(2), filePath, outputV3);
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
    PhysicalFileNode file1Node = new PhysicalFileNode(testRepoPath, file1Path, Files.size(file1Path));
    Set<PhysicalFileNode> physicalFileNodes = new HashSet<PhysicalFileNode>() {{
      add(file1Node);
    }};

    // first retrieve the latest version's file tree
    Assert.assertEquals("File Tree should match",
        physicalFileNodes,
        GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(testRepoPath, testRepoMasterCommitHashes.get(testRepoMasterCommitHashes.size() - 1)));

    // now we add a new file testDir/testFile2.txt
    Path testDirPath = testRepoPath.resolve(testDirectoryName);
    Path file2Path = testDirPath.resolve(testFile2Name);

    String v4Hash = GitVersionControlLocalFileStorage.withCreateVersion(testRepoPath, "v4", () -> {
      try {
        writeFileToRepo(file2Path, testFile2Content);
      } catch (IOException | GitAPIException e) {
        throw new RuntimeException(e);
      }
    });
    testRepoMasterCommitHashes.add(v4Hash);

    PhysicalFileNode dirNode = new PhysicalFileNode(testRepoPath, testDirPath, 0); // Directories typically have size 0
    dirNode.addChildNode(new PhysicalFileNode(testRepoPath, file2Path, Files.size(file2Path)));
    // update the expected fileNodes
    physicalFileNodes.add(dirNode);

    // check the file tree
    Assert.assertEquals(
        "File Tree should match",
        physicalFileNodes,
        GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(testRepoPath, v4Hash));

    // now we delete the file1, check the filetree
    String v5Hash = GitVersionControlLocalFileStorage.withCreateVersion(testRepoPath, "v5", () -> {
      try {
        GitVersionControlLocalFileStorage.removeFileFromRepo(testRepoPath, file1Path);
      } catch (IOException | GitAPIException e) {
        throw new RuntimeException(e);
      }
    });

    physicalFileNodes.remove(file1Node);
    Assert.assertEquals(
        "File1 should be gone",
        physicalFileNodes,
        GitVersionControlLocalFileStorage.retrieveRootFileNodesOfVersion(testRepoPath, v5Hash)
    );

  }

  @Test
  public void testUncommittedCheckAndRecoverToLatest() throws Exception {
    Path tempFilePath = testRepoPath.resolve("tempFile");
    String content = "some random content";
    writeFileToRepo(tempFilePath, content);

    Assert.assertTrue(
        "There should be some uncommitted changes",
        GitVersionControlLocalFileStorage.hasUncommittedChanges(testRepoPath));

    GitVersionControlLocalFileStorage.discardUncommittedChanges(testRepoPath);

    Assert.assertFalse("There should be no uncommitted changes",
        GitVersionControlLocalFileStorage.hasUncommittedChanges(testRepoPath));
  }
}
