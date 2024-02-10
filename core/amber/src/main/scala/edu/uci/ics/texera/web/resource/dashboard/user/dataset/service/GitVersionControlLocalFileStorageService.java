package edu.uci.ics.texera.web.resource.dashboard.user.dataset.service;

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
import java.util.StringTokenizer;

/**
 * Git-based implementation of the VersionControlFileStorageService, using local file storage.
 */
public class GitVersionControlLocalFileStorageService {

  /**
   * Writes content from the InputStream to a file at the given path within the repository.
   *
   * @param repoPath The repository base path.
   * @param relativeFilePath The relative path within the repository to write the file.
   * @param inputStream The InputStream from which to read the content.
   * @throws IOException If an I/O error occurs.
   */
  public static void writeFile(String repoPath, String relativeFilePath, InputStream inputStream) throws IOException {
    Path filePath = Paths.get(repoPath, relativeFilePath);
    Files.createDirectories(filePath.getParent());
    Files.copy(inputStream, filePath, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Deletes a file at the given path within the repository.
   *
   * @param repoPath The repository base path.
   * @param relativeFilePath The relative path within the repository of the file to delete.
   * @throws IOException If an I/O error occurs.
   */
  public static void deleteFile(String repoPath, String relativeFilePath) throws IOException {
    Path filePath = Paths.get(repoPath, relativeFilePath);
    Files.deleteIfExists(filePath);
  }

  /**
   * Deletes the entire directory specified by the repository path.
   *
   * @param repoPath The path of the directory to delete.
   * @throws IOException If an I/O error occurs.
   */
  public static void deleteDirectory(String repoPath) throws IOException {
    Path directoryPath = Paths.get(repoPath);
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
   * @throws InterruptedException If the operation is interrupted.
   */
  public static void initRepo(String baseRepoPath) throws IOException, InterruptedException {
    GitSystemCall.initRepo(baseRepoPath);
  }

  /**
   * Creates a new version in the repository with the given version name.
   *
   * @param baseRepoPath The repository path.
   * @param versionName The name or message associated with the version.
   * @return The commit hash of the created version.
   * @throws IOException If an I/O error occurs.
   * @throws InterruptedException If the operation is interrupted.
   */
  public static String createVersion(String baseRepoPath, String versionName) throws IOException, InterruptedException {
    return GitSystemCall.addAndCommit(baseRepoPath, versionName);
  }

  /**
   * Retrieves the file tree hierarchy of a specific version identified by its commit hash.
   *
   * @param baseRepoPath The repository path.
   * @param versionCommitHashVal The commit hash of the version.
   * @return A map representing the file tree hierarchy of the version.
   * @throws IOException If an I/O error occurs.
   * @throws InterruptedException If the operation is interrupted.
   */
  public static Map<String, Object> retrieveFileTreeOfVersion(String baseRepoPath, String versionCommitHashVal) throws IOException, InterruptedException {
    return GitSystemCall.getFileTreeHierarchy(baseRepoPath, versionCommitHashVal);
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
   * @throws InterruptedException If the operation is interrupted.
   */
  public static void retrieveFileContentOfVersion(String baseRepoPath, String commitHash, String filePath, OutputStream outputStream) throws IOException, InterruptedException {
    GitSystemCall.showFileContentOfCommit(baseRepoPath, commitHash, filePath, outputStream);
  }

  /**
   * Recovers the repository to its latest version, discarding any uncommitted changes.
   *
   * @param baseRepoPath The repository path.
   * @throws IOException If an I/O error occurs.
   * @throws InterruptedException If the operation is interrupted.
   */
  public static void recoverToLatestVersion(String baseRepoPath) throws IOException, InterruptedException {
    if (GitSystemCall.hasUncommittedChanges(baseRepoPath)) {
      GitSystemCall.rollbackToLatestCommit(baseRepoPath);
    }
  }

  /**
   * Utility class for executing Git commands in a system-agnostic manner.
   */
  static class GitSystemCall {

    public static Charset BYTE_2_STRING_CHARSET = StandardCharsets.UTF_8;

    // utility function to execute git-related system call
    private static byte[] executeGitCommand(String workingDirectory, String... args) throws IOException, InterruptedException {
      List<String> commands = new ArrayList<>();
      commands.add("git");
      Collections.addAll(commands, args);

      ProcessBuilder builder = new ProcessBuilder(commands);
      builder.directory(new File(workingDirectory));
      builder.redirectErrorStream(true);
      Process process = builder.start();

      try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[1024];
        int length;
        while ((length = process.getInputStream().read(buffer)) != -1) {
          outputStream.write(buffer, 0, length);
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
          throw new IOException("Failed to execute Git command: " + String.join(" ", commands));
        }

        return outputStream.toByteArray();
      }
    }

    public static void initRepo(String path) throws IOException, InterruptedException {
      executeGitCommand(path, "init");
    }

    // this function adds all changes to the stage and create a commit, it will return the hash of that commit
    public static String addAndCommit(String repoPath, String commitMessage) throws IOException, InterruptedException {
      // Adding all files and committing
      executeGitCommand(repoPath, "add", ".");
      executeGitCommand(repoPath, "commit", "-m", commitMessage);

      // Retrieving the full commit hash of the latest commit
      byte[] commitHashOutput = executeGitCommand(repoPath, "rev-parse", "HEAD");
      return new String(commitHashOutput, StandardCharsets.UTF_8).trim();
    }

    // write the file content of a certain commit into the output file stream
    public static void showFileContentOfCommit(String repoPath, String commitHash, String filePath, OutputStream outputStream) throws IOException, InterruptedException {
      byte[] fileContent = executeGitCommand(repoPath, "show", commitHash + ":" + filePath);
      outputStream.write(fileContent);
    }

    // get the file tree of a certain commit as a Map
    public static Map<String, Object> getFileTreeHierarchy(String repoPath, String commitHash) throws IOException, InterruptedException {
      String treeOutput = new String(executeGitCommand(repoPath, "ls-tree", "-r", commitHash), BYTE_2_STRING_CHARSET);
      return parseFileTree(treeOutput);
    }

    // utility function to parse the file tree as the map
    private static Map<String, Object> parseFileTree(String treeOutput) {
      Map<String, Object> fileTree = new HashMap<>();
      StringTokenizer st = new StringTokenizer(treeOutput, "\n");
      while (st.hasMoreTokens()) {
        String line = st.nextToken();
        String[] parts = line.split("\\s+");

        if (parts.length > 3) {
          String type = parts[1]; // "blob" for files, "tree" for directories
          String path = parts[3];

          if (type.equals("blob")) {
            String[] pathParts = path.split("/");
            addToFileTree(fileTree, pathParts, 0);
          }
        }
      }
      return fileTree;
    }

    private static void addToFileTree(Map<String, Object> tree, String[] pathParts, int index) {
      if (index == pathParts.length - 1) {
        // It's a file, add it to the map
        tree.put(pathParts[index], "file");
      } else {
        // It's a directory, recurse
        tree.computeIfAbsent(pathParts[index], k -> new HashMap<String, Object>());
        @SuppressWarnings("unchecked")
        Map<String, Object> subTree = (Map<String, Object>) tree.get(pathParts[index]);
        addToFileTree(subTree, pathParts, index + 1);
      }
    }

    // rollback the repo to its latest commit
    public static void rollbackToLatestCommit(String repoPath) throws IOException, InterruptedException {
      executeGitCommand(repoPath, "reset", "--hard", "HEAD");
    }

    // check if a repo has uncommitted changes.
    public static boolean hasUncommittedChanges(String repoPath) throws IOException, InterruptedException {
      String statusOutput = new String(executeGitCommand(repoPath, "status", "--porcelain"), BYTE_2_STRING_CHARSET);
      return !statusOutput.isEmpty();
    }
  }
}
