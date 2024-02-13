package edu.uci.ics.texera.web.resource.dashboard.user.dataset;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.service.GitVersionControlLocalFileStorageService;
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
import java.nio.file.Paths;

public class GitVersionControlLocalFileStorageServiceSpec {

  private Path testRepoPath;
  private final String testFileName = "testFile.txt";
  private final String testFileContent = "This is a test file";

  @Before
  public void setUp() throws IOException {
    // Create a temporary directory for the repository
    testRepoPath = Files.createTempDirectory("testRepo");
  }

  @After
  public void tearDown() throws IOException {
    // Clean up the test repository directory
    GitVersionControlLocalFileStorageService.deleteDirectory(testRepoPath);
  }

  @Test
  public void testInitRepo() throws IOException, GitAPIException {
    GitVersionControlLocalFileStorageService.initRepo(testRepoPath);
    Assert.assertTrue(Files.exists(testRepoPath.resolve(".git")));
  }

  @Test
  public void testWriteAndDeleteFile() throws IOException {
    Path filePath = testRepoPath.resolve(testFileName);
    try (ByteArrayInputStream input = new ByteArrayInputStream(testFileContent.getBytes())) {
      GitVersionControlLocalFileStorageService.writeFile(filePath, input);
    }
    Assert.assertTrue(Files.exists(filePath));

    GitVersionControlLocalFileStorageService.deleteFile(filePath);
    Assert.assertFalse(Files.exists(filePath));
  }

  @Test
  public void testRetrieveFileContentOfVersion() throws IOException, GitAPIException {
    // Initialize the repository and write a test file
    GitVersionControlLocalFileStorageService.initRepo(testRepoPath);
    Path filePath = testRepoPath.resolve(testFileName);
    try (ByteArrayInputStream input = new ByteArrayInputStream(testFileContent.getBytes())) {
      GitVersionControlLocalFileStorageService.writeFile(filePath, input);
    }

    // Commit the test file to the repository
    String commitHash = GitVersionControlLocalFileStorageService.createVersion(testRepoPath, "Initial commit");

    // Retrieve the file content of the committed version
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    GitVersionControlLocalFileStorageService.retrieveFileContentOfVersion(testRepoPath, commitHash, filePath, output);
    String retrievedContent = output.toString();

    Assert.assertEquals(testFileContent, retrievedContent);
  }
}
