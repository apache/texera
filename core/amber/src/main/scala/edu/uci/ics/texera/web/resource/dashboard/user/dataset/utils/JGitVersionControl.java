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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JGitVersionControl {

  public static String initRepo(Path path) throws GitAPIException, IOException {
    File gitDir = path.resolve(".git").toFile();
    if (gitDir.exists()) {
      throw new IOException("Repository already exists at " + path);
    } else {
      try (Git git = Git.init().setDirectory(path.toFile()).call()) {
        // Repository initialization logic remains the same

        // Retrieve the default branch name
        Ref head = git.getRepository().exactRef("HEAD");
        if (head != null && head.getTarget() != null) {
          String refName = head.getTarget().getName();
          // HEAD will be in the form of 'ref: refs/heads/defaultBranchName'
          if (refName.startsWith("refs/heads/")) {
            return refName.substring("refs/heads/".length());
          }
        }
      }
    }
    return null; // or throw an exception if preferred
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
         RevWalk revWalk = new RevWalk(repository)) {
      ObjectId commitId = repository.resolve(commitHash);
      RevCommit commit = revWalk.parseCommit(commitId);

      try (TreeWalk treeWalk = new TreeWalk(repository)) {
        treeWalk.addTree(commit.getTree());
        treeWalk.setRecursive(true);

        while (treeWalk.next()) {
          Path fullPath = repoPath.resolve(treeWalk.getPathString());
          String pathStr = fullPath.toString();

          // Determine if the current path is at the root level
          if (treeWalk.getDepth() == 0) {
            FileNode rootNode = new FileNode(fullPath);
            rootNodes.add(rootNode);
            pathToFileNodeMap.put(pathStr, rootNode);
          } else {
            // For child nodes, find or create the parent node based on the directory structure
            Path parentPath = fullPath.getParent();
            String parentPathStr = parentPath.toString();
            FileNode parentNode = pathToFileNodeMap.get(parentPathStr);

            if (parentNode == null) {
              parentNode = new FileNode(parentPath);
              pathToFileNodeMap.put(parentPathStr, parentNode);
              // Determine if this parent should be added to rootNodes
              if (parentPath.getParent().equals(repoPath)) {
                rootNodes.add(parentNode);
              }
            }

            FileNode childNode = new FileNode(fullPath);
            parentNode.addChildNode(childNode);
            // Map child node to its path for potential future children
            pathToFileNodeMap.put(pathStr, childNode);
          }
        }
      }
    }

    return rootNodes;
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

  public static List<String> getAllCommitHashes(Path repoPath, String branchName) throws IOException, GitAPIException {
    List<String> commitHashes = new ArrayList<>();

    try (Repository repository = new FileRepositoryBuilder()
        .setGitDir(repoPath.resolve(".git").toFile())
        .build();
         RevWalk revWalk = new RevWalk(repository)) {

      Ref branchRef = repository.exactRef("refs/heads/" + branchName);
      if (branchRef == null) {
        throw new IllegalArgumentException("Branch not found: " + branchName);
      }
      RevCommit startCommit = revWalk.parseCommit(branchRef.getObjectId());
      revWalk.markStart(startCommit);

      for (RevCommit commit : revWalk) {
        commitHashes.add(commit.getId().getName());
      }
    }

    return commitHashes;
  }

  public JGitVersionControl() {
  }
}
