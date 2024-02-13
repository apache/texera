package edu.uci.ics.texera.web.resource.dashboard.user.dataset.service;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.type.FileNode;
import edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils.JGitVersionControl;
import org.eclipse.jgit.api.errors.GitAPIException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Git-based implementation of the VersionControlFileStorageService, using local file storage.
 */
public class GitVersionControlLocalFileStorageService {

  /**
   * Writes content from the InputStream to a file at the given path.
   *
   * @param filePath The path within the repository to write the file.
   * @param inputStream The InputStream from which to read the content.
   * @throws IOException If an I/O error occurs.
   */
  public static void writeFile(Path filePath, InputStream inputStream) throws IOException {
    Files.createDirectories(filePath.getParent());
    Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Deletes a file at the given path.
   *
   * @param filePath The path of the file to delete.
   * @throws IOException If an I/O error occurs.
   */
  public static void deleteFile(Path filePath) throws IOException {
    if (Files.isDirectory(filePath)) {
      throw new IllegalArgumentException("Provided path is a directory, not a file: " + filePath);
    }
    Files.deleteIfExists(filePath);
  }

  /**
   * Deletes the entire directory specified by the path.
   *
   * @param directoryPath The path of the directory to delete.
   * @throws IOException If an I/O error occurs.
   */
  public static void deleteDirectory(Path directoryPath) throws IOException {
    Files.walk(directoryPath)
        .sorted(Comparator.reverseOrder())
        .map(Path::toFile)
        .forEach(File::delete);
  }

  /**
   * Initializes a new repository for version control at the specified path.
   *
   * @param baseRepoPath Path to initialize the repository at.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the JGit operation is interrupted.
   */
  public static void initRepo(Path baseRepoPath) throws IOException, GitAPIException {
    JGitVersionControl.initRepo(baseRepoPath);
  }

  /**
   * Creates a new version in the repository with the given version name.
   *
   * @param baseRepoPath The repository path.
   * @param versionName The name or message associated with the version.
   * @return The commit hash of the created version.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the JGit operation is interrupted.
   */
  public static String createVersion(Path baseRepoPath, String versionName) throws IOException, GitAPIException {
    return JGitVersionControl.addAndCommit(baseRepoPath, versionName);
  }

  /**
   * Retrieves the file tree hierarchy of a specific version identified by its commit hash.
   *
   * @param baseRepoPath The repository path.
   * @param versionCommitHashVal The commit hash of the version.
   * @return A set of file nodes at the root level of the given repo at given version
   */
  public static Set<FileNode> retrieveFileTreeOfVersion(Path baseRepoPath, String versionCommitHashVal) throws Exception {
    return JGitVersionControl.getFileTreeOfCommit(baseRepoPath, versionCommitHashVal);
  }

  /**
   * Retrieves the content of a specific file from a specific version identified by its commit hash.
   * Writes the file content to the provided OutputStream.
   *
   * @param baseRepoPath The repository path.
   * @param commitHash The commit hash of the version from which the file content is retrieved.
   * @param filePath The path of the file within the repository.
   * @param outputStream The OutputStream to which the file content is written.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the operation is interrupted.
   */
  public static void retrieveFileContentOfVersion(Path baseRepoPath, String commitHash, Path filePath, OutputStream outputStream) throws IOException, GitAPIException {
    JGitVersionControl.showFileContentOfCommit(baseRepoPath, commitHash, filePath, outputStream);
  }

  /**
   * Recovers the repository to its latest version, discarding any uncommitted changes.
   *
   * @param baseRepoPath The repository path.
   * @throws IOException If an I/O error occurs.
   * @throws GitAPIException If the operation is interrupted.
   */
  public static void recoverToLatestVersion(Path baseRepoPath) throws IOException, GitAPIException {
    JGitVersionControl.rollbackToLatestCommit(baseRepoPath);
  }
}
