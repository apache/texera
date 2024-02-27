package edu.uci.ics.texera.web.resource.dashboard.user.dataset.type;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FileNode {
  private final Path absoluteFilePath;

  private final Path relativeFilePath;
  private final Set<FileNode> children;

  public FileNode(Path repoPath, Path path) {
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

  public void addChildNode(FileNode child) {
    if (!child.getAbsolutePath().getParent().equals(this.absoluteFilePath)) {
      throw new IllegalArgumentException("Child node is not a direct subpath of the parent node");
    }
    this.children.add(child);
  }

  public Set<FileNode> getChildren() {
    return children;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    FileNode fileNode = (FileNode) o;
    return Objects.equals(absoluteFilePath, fileNode.absoluteFilePath) &&
        Objects.equals(children, fileNode.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(absoluteFilePath, children);
  }
}
