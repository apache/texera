package edu.uci.ics.texera.web.resource.dashboard.user.dataset.type;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class FileNode {
  private final Path path;
  private final Set<FileNode> children;

  public FileNode(Path path) {
    this.path = path;
    this.children = new HashSet<>();
  }

  public boolean isFile() {
    return Files.isRegularFile(path);
  }

  public boolean isDirectory() {
    return Files.isDirectory(path);
  }

  public Path getPath() {
    return path;
  }

  public void addChildNode(FileNode child) {
    if (!child.getPath().getParent().equals(this.path)) {
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
    return Objects.equals(path, fileNode.path) &&
        Objects.equals(children, fileNode.children);
  }

  @Override
  public int hashCode() {
    return Objects.hash(path, children);
  }
}
