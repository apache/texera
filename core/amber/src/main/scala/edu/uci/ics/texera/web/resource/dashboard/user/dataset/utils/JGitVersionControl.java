package edu.uci.ics.texera.web.resource.dashboard.user.dataset.utils;

import edu.uci.ics.texera.web.resource.dashboard.user.dataset.type.FileNode;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.ResetCommand;
import org.eclipse.jgit.api.Status;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.Ref;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.lib.ObjectLoader;
import org.eclipse.jgit.revwalk.RevCommit;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.eclipse.jgit.treewalk.TreeWalk;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JGitVersionControl {

  public static String initRepo(Path path) throws GitAPIException, IOException {
    File gitDir = path.resolve(".git").toFile();
    if (gitDir.exists()) {
      throw new IOException("Repository already exists at " + path);
    }
    // try-with-resource make sure the resource is released
    try (Git git = Git.init().setDirectory(path.toFile()).call()) {
      // Retrieve the default branch name
      Ref head = git.getRepository().exactRef("HEAD");
      if (head == null || head.getTarget() == null) {
        return null;
      }
      String refName = head.getTarget().getName();
      // HEAD should be in the form of 'ref: refs/heads/defaultBranchName'
      if (!refName.startsWith("refs/heads/")) {
        return null;
      }
      return refName.substring("refs/heads/".length());
    }
  }

  public static void readFileContentOfCommit(Path repoPath, String commitHash, Path filePath, OutputStream outputStream) throws IOException {
    if (!filePath.startsWith(repoPath)) {
      throw new IllegalArgumentException("File path must be under the repository path.");
    }

    if (Files.isDirectory(filePath)) {
      throw new IllegalArgumentException("File path points to a directory, not a file.");
    }

    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         RevWalk revWalk = new RevWalk(repository)) {

      RevCommit commit = revWalk.parseCommit(repository.resolve(commitHash));
      TreeWalk treeWalk =
          TreeWalk.forPath(repository, repoPath.relativize(filePath).toString(), commit.getTree());
      if (treeWalk == null) {
        throw new IOException("File not found in commit: " + filePath);
      }
      ObjectId objectId = treeWalk.getObjectId(0);
      ObjectLoader loader = repository.open(objectId);

      loader.copyTo(outputStream);
    }
  }

  public static Set<FileNode> getRootFileNodeOfCommit(Path repoPath, String commitHash) throws Exception {
    Map<String, FileNode> pathToFileNodeMap = new HashMap<>();
    Set<FileNode> rootNodes = new HashSet<>();

    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         RevWalk revWalk = new RevWalk(repository)) {
      ObjectId commitId = repository.resolve(commitHash);
      RevCommit commit = revWalk.parseCommit(commitId);

      // initialize the treeWalk to traverse the file tree
      TreeWalk treeWalk = new TreeWalk(repository);
      treeWalk.addTree(commit.getTree());
      treeWalk.setRecursive(true);

      while (treeWalk.next()) {
        Path fullPath = repoPath.resolve(treeWalk.getPathString());
        String pathStr = fullPath.toString();

        // Determine if the current path is at the root level
        if (treeWalk.getDepth() == 0) {
          FileNode rootNode = new FileNode(repoPath, fullPath);
          rootNodes.add(rootNode);
          pathToFileNodeMap.put(pathStr, rootNode);
        } else {
          // For child nodes, find or create the parent node based on the directory structure
          Path parentPath = fullPath.getParent();
          String parentPathStr = parentPath.toString();
          FileNode parentNode = pathToFileNodeMap.get(parentPathStr);

          if (parentNode == null) {
            parentNode = new FileNode(repoPath, parentPath);
            pathToFileNodeMap.put(parentPathStr, parentNode);
            // Determine if this parent should be added to rootNodes
            if (parentPath.getParent().equals(repoPath)) {
              rootNodes.add(parentNode);
            }
          }

          FileNode childNode = new FileNode(repoPath, fullPath);
          parentNode.addChildNode(childNode);
          // Map child node to its path for potential future children
          pathToFileNodeMap.put(pathStr, childNode);
        }
      }
    }
    return rootNodes;
  }

  public static void add(Path repoPath, Path filePath) throws IOException, GitAPIException {
    try (Git git = Git.open(repoPath.toFile())) {
      // Stage the file addition/modification
      git.add().addFilepattern(repoPath.relativize(filePath).toString()).call();
    }
  }

  public static void rm(Path repoPath, Path filePath) throws IOException, GitAPIException {
    try (Git git = Git.open(repoPath.toFile())) {
      git.rm().addFilepattern(repoPath.relativize(filePath).toString()).call(); // Stages the file deletion
    }
  }

  // create a commit, and return the commit hash
  public static String commit(Path repoPath, String commitMessage) throws IOException, GitAPIException {
    FileRepositoryBuilder builder = new FileRepositoryBuilder();
    try (Repository repository = builder.setGitDir(repoPath.resolve(".git").toFile())
        .readEnvironment() // scan environment GIT_* variables
        .findGitDir() // scan up the file system tree
        .build()) {

      try (Git git = new Git(repository)) {
        // Commit the changes that have been staged
        RevCommit commit = git.commit().setMessage(commitMessage).call();

        // Return the commit hash
        return commit.getId().getName();
      }
    }
  }

  public static void discardUncommittedChanges(Path repoPath) throws IOException, GitAPIException {
    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         Git git = new Git(repository)) {

      // Reset hard to discard changes in tracked files
      git.reset().setMode(ResetCommand.ResetType.HARD).call();

      // Clean the working directory to remove untracked files
      git.clean().setCleanDirectories(true).call();
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
