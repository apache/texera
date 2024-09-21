package edu.uci.ics.texera.web.resource.dashboard.user.dataset.type;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class PhysicalFileNode {
  private final Path absoluteFilePath;

  private final Path relativeFilePath;
  private final Set<PhysicalFileNode> children;

  public PhysicalFileNode(Path repoPath, Path path) {
    this.absoluteFilePath = path;
    this.relativeFilePath = repoPath.relativize(this.absoluteFilePath);
    this.children = new HashSet<>();
  }

  public boolean isFile() {
    return Files.isRegularFile(absoluteFilePath);
  }

  public boolean isDirectory() {
    return Files.isDirectory(absoluteFilePath);
  }

  public Path getAbsolutePath() {
    return absoluteFilePath;
  }

  public Path getRelativePath() {
    return relativeFilePath;
  }

  public void addChildNode(PhysicalFileNode child) {
    if (!child.getAbsolutePath().getParent().equals(this.absoluteFilePath)) {
      throw new IllegalArgumentException("Child node is not a direct subpath of the parent node");
    }
    this.children.add(child);
  }

  public Set<PhysicalFileNode> getChildren() {
    return children;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PhysicalFileNode physicalFileNode = (PhysicalFileNode) o;
    return Objects.equals(absoluteFilePath, physicalFileNode.absoluteFilePath) &&
        Objects.equals(children, physicalFileNode.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(absoluteFilePath, children);
  }

  /**
   * Collects the relative paths of all file nodes from a given set of FileNode.
   * @param nodes The set of FileNode to collect file paths from.
   * @return A list of strings representing the relative paths of all file nodes.
   */
  public static List<String> getAllFileRelativePaths(Set<PhysicalFileNode> nodes) {
    List<String> filePaths = new ArrayList<>();
    getAllFileRelativePathsHelper(nodes, filePaths);
    return filePaths;
  }

  /**
   * Helper method to recursively collect the relative paths of all file nodes.
   * @param nodes The current set of FileNode to collect file paths from.
   * @param filePaths The list to add the relative paths of the file nodes to.
   */
  private static void getAllFileRelativePathsHelper(Set<PhysicalFileNode> nodes, List<String> filePaths) {
    for (PhysicalFileNode node : nodes) {
      if (node.isFile()) {
        filePaths.add(node.getRelativePath().toString());
      } else if (node.isDirectory()) {
        getAllFileRelativePathsHelper(node.getChildren(), filePaths);
      }
    }
  }
}
