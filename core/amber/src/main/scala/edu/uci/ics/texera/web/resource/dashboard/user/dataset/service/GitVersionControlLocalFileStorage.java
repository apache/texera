package edu.uci.ics.texera.web.resource.dashboard.user.dataset.service;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.type.FileNode;
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.JGitVersionControl;
import org.eclipse.jgit.api.errors.GitAPIException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.Set;

/**
 * Git-based implementation of the VersionControlFileStorageService, using local file storage.
 */
public class GitVersionControlLocalFileStorage {

  /**
   * Writes content from the InputStream to a file at the given path. And stage the file addition/modification to git
   * This function WILL create the missing parent directory along the path

   * This method is NOT THREAD SAFE, as it did the file I/O and the git add operation
   * @param filePath The path within the repository to write the file.
   * @param inputStream The InputStream from which to read the content.
   * @throws IOException If an I/O error occurs.
   */
  public static void writeFileToRepo(Path repoPath, Path filePath, InputStream inputStream) throws IOException, GitAPIException {
    Files.createDirectories(filePath.getParent());
    Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
    // stage the addition/modification
    JGitVersionControl.add(repoPath, filePath);
  }

  /**
   * Deletes a file at the given path.
   * If the file path is pointing to a directory, or if file does not exist, error will be thrown

   * This method is NOT THREAD SAFE, as it did the file I/O and the git rm operation
   * @param filePath The path of the file to delete.
   * @throws IOException If an I/O error occurs.
   */
  public static void removeFileFromRepo(Path repoPath, Path filePath) throws IOException, GitAPIException {
    if (Files.isDirectory(filePath)) {
      throw new IllegalArgumentException("Provided path is a directory, not a file: " + filePath);
    }

    Files.delete(filePath);
    // stage the deletion
    JGitVersionControl.rm(repoPath, filePath);
  }

  /**
   * Deletes the entire repository specified by the path.

   * This method is NOT THREAD SAFE, as it did the file I/O recursively
   * @param directoryPath The path of the directory to delete.
   * @throws IOException If an I/O error occurs.
   */
  public static void deleteRepo(Path directoryPath) throws IOException {
    Files.walk(directoryPath)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }

  /**
   * Initializes a new repository for version control at the specified path.

   * This method is NOT THREAD SAFE
   * @param baseRepoPath Path to initialize the repository at.
   * @return The branch identifier
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the JGit operation is interrupted.
   */
  public static String initRepo(Path baseRepoPath) throws IOException, GitAPIException {
    // Check if the directory exists, if not, create it
    if (Files.notExists(baseRepoPath)) {
      Files.createDirectories(baseRepoPath);
    }

    return JGitVersionControl.initRepo(baseRepoPath);
  }

  /**
   * Executes a group of file operations as a single versioned transaction. The version is bumped after the operations finish.

   * This method is NOT THREAD SAFE as it potentially does lots of file I/O along with git operations
   * @param baseRepoPath The repository path.
   * @param versionName The name or message associated with the version.
   * @param operations The file operations to be executed within this versioned transaction.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If a Git operation fails.
   */
  public static String withCreateVersion(Path baseRepoPath, String versionName, Runnable operations) throws IOException, GitAPIException {
    // Execute the provided file operations
    operations.run();
    // After successful execution, create a new version with the specified name
    return JGitVersionControl.commit(baseRepoPath, versionName);
  }

  /**
   * Retrieves the set of file nodes at the root level, identified by its commit hash.

   * This method is THREAD SAFE
   * @param baseRepoPath The repository path.
   * @param versionCommitHashVal The commit hash of the version.
   * @return A set of file nodes at the root level of the given repo at given version
   */
  public static Set<FileNode> retrieveRootFileNodesOfVersion(Path baseRepoPath, String versionCommitHashVal) throws Exception {
    return JGitVersionControl.getRootFileNodeOfCommit(baseRepoPath, versionCommitHashVal);
  }

  /**
   * Retrieves the content of a specific file from a specific version identified by its commit hash.
   * Writes the file content to the provided OutputStream.

   * This method is THREAD SAFE
   * @param baseRepoPath The repository path.
   * @param commitHash The commit hash of the version from which the file content is retrieved.
   * @param filePath The path of the file within the repository.
   * @param outputStream The OutputStream to which the file content is written.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the operation is interrupted.
   */
  public static void retrieveFileContentOfVersion(Path baseRepoPath, String commitHash, Path filePath, OutputStream outputStream) throws IOException, GitAPIException {
    JGitVersionControl.readFileContentOfCommit(baseRepoPath, commitHash, filePath, outputStream);
  }

  /**
   * Check if there is any uncommitted change in the given repo

   * This method is THREAD SAFE
   * @param repoPath The repository path
   * @return True if there are uncommitted changes.
   * @throws GitAPIException
   * @throws IOException
   */
  public static boolean hasUncommittedChanges(Path repoPath) throws GitAPIException, IOException {
    return JGitVersionControl.hasUncommittedChanges(repoPath);
  }

  /**
   * Recovers the repository to its latest version, discarding any uncommitted changes if any.

   * This method is NOT THREAD SAFE
   * @param baseRepoPath The repository path.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the operation is interrupted.
   */
  public static void discardUncommittedChanges(Path baseRepoPath) throws IOException, GitAPIException {
    JGitVersionControl.discardUncommittedChanges(baseRepoPath);
  }
}
