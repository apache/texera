package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.type.FileNode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;
import org.eclipse.jgit.treewalk.filter.PathFilter;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JGitVersionControl {

  public static void initRepo(Path path) throws GitAPIException, IOException {
    File gitDir = path.resolve(".git").toFile();
    if (gitDir.exists()) {
      throw new IOException("Repository already exists at " + path);
    } else {
      Git.init().setDirectory(path.toFile()).call().close();
    }
  }

  public static void showFileContentOfCommit(Path repoPath, String commitHash, Path filePath, OutputStream outputStream) throws IOException, GitAPIException {
    if (!filePath.startsWith(repoPath)) {
      throw new IllegalArgumentException("File path must be under the repository path.");
    }

    String relativePath = repoPath.relativize(filePath).toString();
    if (Files.isDirectory(filePath)) {
      throw new IllegalArgumentException("File path points to a directory, not a file.");
    }

    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         RevWalk revWalk = new RevWalk(repository)) {

      RevCommit commit = revWalk.parseCommit(repository.resolve(commitHash));
      TreeWalk treeWalk = TreeWalk.forPath(repository, relativePath, commit.getTree());

      if (treeWalk != null) {
        ObjectId objectId = treeWalk.getObjectId(0);
        ObjectLoader loader = repository.open(objectId);

        loader.copyTo(outputStream);
      } else {
        throw new IOException("File not found in commit: " + filePath);
      }
    }
  }

  public static Set<FileNode> getFileTreeOfCommit(Path repoPath, String commitHash) throws Exception {
    Map<String, FileNode> pathToFileNodeMap = new HashMap<>();
    Set<FileNode> rootNodes = new HashSet<>();

    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         Git git = new Git(repository)) {
      ObjectId commitId = repository.resolve(commitHash);

      try (RevWalk revWalk = new RevWalk(repository)) {
        RevCommit commit = revWalk.parseCommit(commitId);
        try (TreeWalk treeWalk = new TreeWalk(repository)) {
          treeWalk.addTree(commit.getTree());
          treeWalk.setRecursive(false); // Only walk this commit's root level

          while (treeWalk.next()) {
            String pathStr = treeWalk.getPathString();
            Path path = repoPath.resolve(pathStr); // Resolve against repoPath for accuracy
            FileNode node = new FileNode(path);

            // Here we're directly working with root level nodes
            if (!treeWalk.isSubtree() || treeWalk.getDepth() == 0) {
              rootNodes.add(node);
            } else {
              // For deeper levels, establish parent-child relationships
              String parentPathStr = path.getParent().toString();
              FileNode parentNode = pathToFileNodeMap.getOrDefault(parentPathStr, new FileNode(path.getParent()));
              parentNode.addChildNode(node);
              pathToFileNodeMap.putIfAbsent(parentPathStr, parentNode);
            }

            pathToFileNodeMap.put(pathStr, node);
          }
        }
      }
    }

    return rootNodes; // Return the set of FileNode objects at the root level of the repository for the specific commit
  }

  public static String addAndCommit(Path repoPath, String commitMessage) throws IOException, GitAPIException {
    FileRepositoryBuilder builder = new FileRepositoryBuilder();
    try (Repository repository = builder.setGitDir(repoPath.resolve(".git").toFile())
        .readEnvironment() // scan environment GIT_* variables
        .findGitDir() // scan up the file system tree
        .build()) {

      try (Git git = new Git(repository)) {
        // Add all files to the staging area
        git.add().addFilepattern(".").call();

        // Commit the changes
        RevCommit commit = git.commit().setMessage(commitMessage).call();

        // Return the commit hash
        return commit.getId().getName();
      }
    }
  }

  public static void rollbackToLatestCommit(Path repoPath) throws IOException, GitAPIException {
    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         Git git = new Git(repository)) {

      git.reset().setMode(ResetCommand.ResetType.HARD).call();
    }
  }

  public static boolean hasUncommittedChanges(Path repoPath) throws IOException, GitAPIException {
    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .readEnvironment()
        .findGitDir()
        .build();
         Git git = new Git(repository)) {

      Status status = git.status().call();
      return !status.isClean();
    }
  }
}
