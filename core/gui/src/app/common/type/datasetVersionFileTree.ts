import { FileUploadItem } from "../../dashboard/type/dashboard-file.interface";
import { FileNode } from "./fileNode";
import { DashboardDataset } from "../../dashboard/user/type/dashboard-dataset.interface";

export interface DatasetVersionFileTree {
  [key: string]: DatasetVersionFileTree | string;
}

export interface DatasetVersionFileTreeNode {
  name: string;
  type: "file" | "directory";
  children?: DatasetVersionFileTreeNode[]; // Only populated if 'type' is 'directory'
  parentDir: string;
  did?: number;
  dvid?: number;
}

export function pruneFilePath(path: string): string {
  // Split the path by '/'
  const segments = path.split('/');

  // Remove the first segment
  segments.shift();

  // Join the remaining segments back into a path
  return '/' + segments.join('/');
}

export function getFullPathFromFileTreeNode(node: DatasetVersionFileTreeNode, embedIDs: boolean = false): string {
  const parts = node.parentDir.split('/').filter(Boolean);
  let embeddedPath = parts.join('/');

  if (embedIDs) {
    const didPart = node.did !== undefined ? `did:${node.did}` : '';
    const dvidPart = node.dvid !== undefined ? `_dvid:${node.dvid}` : '';

    if (didPart || dvidPart) {
      embeddedPath = `${didPart}${dvidPart}/${embeddedPath}`;
    }
  }

  if (embeddedPath === "") {
    // This means the original parentDir was "/"
    return `/${node.name}`;
  } else {
    return `/${embeddedPath}/${node.name}`;
  }
}



export function getPathsFromTreeNode(node: DatasetVersionFileTreeNode): string[] {
  // Helper function to recursively gather paths
  const gatherPaths = (node: DatasetVersionFileTreeNode, currentPath: string): string[] => {
    // Base case: if the node is a file, return its path
    if (node.type === "file") {
      return [currentPath];
    }

    // Recursive case: if the node is a directory, explore its children
    let paths = node.children ? node.children.flatMap(child => gatherPaths(child, currentPath + "/" + child.name)) : [];

    // Include the directory's own path if it's not the root
    if (node.name !== "/") {
      paths.unshift(currentPath);
    }

    return paths;
  };

  return gatherPaths(node, node.parentDir === "/" ? "/" + node.name : node.parentDir + "/" + node.name);
}

// This class convert a list of DatasetVersionTreeNode into a hash map, recursively containing all the paths
export class DatasetVersionFileTreeManager {
  private root: DatasetVersionFileTreeNode = { name: "/", type: "directory", children: [], parentDir: "" };
  private treeNodesMap: Map<string, DatasetVersionFileTreeNode> = new Map<string, DatasetVersionFileTreeNode>();

  constructor(nodes: DatasetVersionFileTreeNode[] = []) {
    this.treeNodesMap.set("/", this.root);
    if (nodes.length > 0) this.initializeWithRootNodes(nodes);
  }

  private updateTreeMapWithPath(path: string): DatasetVersionFileTreeNode {
    const pathParts = path.startsWith("/") ? path.slice(1).split("/") : path.split("/");
    let currentPath = "/";
    let currentNode = this.root;

    pathParts.forEach((part, index) => {
      const previousPath = currentPath;
      currentPath += part + (index < pathParts.length - 1 ? "/" : ""); // Don't add trailing slash for last part

      if (!this.treeNodesMap.has(currentPath)) {
        const isLastPart = index === pathParts.length - 1;
        const newNode: DatasetVersionFileTreeNode = {
          name: part,
          type: isLastPart ? "file" : "directory",
          parentDir: previousPath.endsWith("/") ? previousPath.slice(0, -1) : previousPath, // Store the full path for parentDir
          ...(isLastPart ? {} : { children: [] }), // Only add 'children' for directories
        };
        this.treeNodesMap.set(currentPath, newNode);
        currentNode.children = currentNode.children ?? []; // Ensure 'children' is initialized
        currentNode.children.push(newNode);
      }
      currentNode = this.treeNodesMap.get(currentPath)!; // Get the node for the next iteration
    });

    return currentNode;
  }

  private removeNodeAndDescendants(node: DatasetVersionFileTreeNode): void {
    if (node.type === "directory" && node.children) {
      node.children.forEach(child => {
        const childPath =
          node.parentDir === "/" ? `/${node.name}/${child.name}` : `${node.parentDir}/${node.name}/${child.name}`;
        this.removeNodeAndDescendants(child);
        this.treeNodesMap.delete(childPath); // Remove the child from the map
      });
    }
    // Now that all children are removed, clear the current node's children array
    node.children = [];
  }

  addNodeWithPath(path: string): DatasetVersionFileTreeNode {
    return this.updateTreeMapWithPath(path);
  }

  initializeWithRootNodes(rootNodes: DatasetVersionFileTreeNode[]) {
    // Clear existing nodes in map except the root
    this.treeNodesMap.clear();
    this.treeNodesMap.set("/", this.root);

    // Helper function to add nodes recursively
    const addNodeRecursively = (node: DatasetVersionFileTreeNode, parentDir: string) => {
      const nodePath = parentDir === "/" ? `/${node.name}` : `${parentDir}/${node.name}`;
      this.treeNodesMap.set(nodePath, node);

      // If the node is a directory, recursively add its children
      if (node.type === "directory" && node.children) {
        node.children.forEach(child => addNodeRecursively(child, nodePath));
      }
    };

    // Add each root node and their children to the tree and map
    rootNodes.forEach(node => {
      if (!this.root.children) {
        this.root.children = [];
      }
      this.root.children.push(node);
      addNodeRecursively(node, "/");
    });
  }

  removeNode(targetNode: DatasetVersionFileTreeNode): void {
    if (targetNode.parentDir === "" && targetNode.name === "/") {
      // Can't remove root
      return;
    }

    // Queue for BFS
    const queue: DatasetVersionFileTreeNode[] = [this.root];

    while (queue.length > 0) {
      const node = queue.shift()!;

      // Check if the current node is the parent of the target node
      if (node.children && node.children.some(child => child === targetNode)) {
        // Remove the target node and its descendants
        this.removeNodeAndDescendants(targetNode);

        // Remove the target node from the current node's children
        node.children = node.children.filter(child => child !== targetNode);

        // Construct the full path of the target node to remove it from the map
        const pathToRemove = getFullPathFromFileTreeNode(targetNode);
        this.treeNodesMap.delete(pathToRemove);

        return; // Node found and removed, exit the function
      }

      // If not found, add the children of the current node to the queue
      if (node.children) {
        queue.push(...node.children);
      }
    }
  }

  removeNodeWithPath(path: string): void {
    const nodeToRemove = this.treeNodesMap.get(path);
    if (nodeToRemove) {
      // First, recursively remove all descendants of the node
      this.removeNodeAndDescendants(nodeToRemove);

      // Then, remove the node from its parent's children array
      const parentNode = this.treeNodesMap.get(nodeToRemove.parentDir);
      if (parentNode && parentNode.children) {
        parentNode.children = parentNode.children.filter(child => child.name !== nodeToRemove.name);
      }

      // Finally, remove the node from the map
      this.treeNodesMap.delete(path);
    }
  }

  getRootNodes(): DatasetVersionFileTreeNode[] {
    return this.root.children ?? [];
  }
}
export function parseFileUploadItemToTreeNodes(fileUploadItems: FileUploadItem[]): DatasetVersionFileTreeNode[] {
  const root: DatasetVersionFileTreeNode = { name: "/", type: "directory", children: [], parentDir: "" };
  const treeNodesMap = new Map<string, DatasetVersionFileTreeNode>();
  treeNodesMap.set("/", root);

  fileUploadItems.forEach(item => {
    const pathParts = item.name.startsWith("/") ? item.name.slice(1).split("/") : item.name.split("/");
    let currentPath = "/";
    let currentNode = root;

    pathParts.forEach((part, index) => {
      currentPath += part + (index < pathParts.length - 1 ? "/" : ""); // Don't add trailing slash for last part
      if (!treeNodesMap.has(currentPath)) {
        const isLastPart = index === pathParts.length - 1;
        const newNode: DatasetVersionFileTreeNode = {
          name: part,
          type: isLastPart ? "file" : "directory",
          parentDir: currentNode.name,
          ...(isLastPart ? {} : { children: [] }), // Only add 'children' for directories
        };
        treeNodesMap.set(currentPath, newNode);
        currentNode.children = currentNode.children ?? []; // Ensure 'children' is initialized
        currentNode.children.push(newNode);
      }
      currentNode = treeNodesMap.get(currentPath)!; // Get the node for the next iteration
    });
  });

  return root.children ?? []; // Return the top-level nodes (excluding the root)
}

export function parseDatasetRootNode(dataset: DashboardDataset): DatasetVersionFileTreeNode {
  const did = dataset.dataset.did;
  const datasetNode: DatasetVersionFileTreeNode = {
    name: dataset.dataset.name,
    type: "directory",
    parentDir: "/",
    children: [],
    did: did, // Assuming the dataset has an id property
  };

  dataset.versions.forEach(version => {
    const dvid = version.datasetVersion.dvid;
    const versionNode: DatasetVersionFileTreeNode = {
      name: version.datasetVersion.name,
      type: "directory",
      parentDir: `/${dataset.dataset.name}`,
      children: parseFileNodesToTreeNodes(
        version.fileNodes,
        dataset.dataset.name,
        did,
        version.datasetVersion.name,
        dvid
      ),
      dvid: dvid, // Assuming the datasetVersion has an id property
    };
    datasetNode.children!.push(versionNode);
  });

  return datasetNode;
}

// parse the file nodes passed by the backend to tree nodes that are displayable in frontend
// datasetName is an optional parameter, when given, the datasetName should be the prefix of every parentDir
export function parseFileNodesToTreeNodes(
  fileNodes: FileNode[],
  datasetName: string = "",
  did: number = 0,
  versionName: string = "",
  dvid: number = 0
): DatasetVersionFileTreeNode[] {
  // Ensure datasetName is formatted correctly as a path prefix
  let prefix = datasetName ? `/${datasetName}` : "";
  // Append versionName to the prefix if provided
  if (versionName) {
    prefix += `/${versionName}`;
  }

  return fileNodes.map(fileNode => {
    // Split the path to work with its segments
    const splitPath = fileNode.path.split("/");
    const name = splitPath.pop() || ""; // Get the last segment as the name

    // Construct the parentDir
    // If there are remaining segments, join them as the path, prefixed by the combined prefix
    // Otherwise, use the combined prefix directly (or just "/" if both datasetName and versionName are empty)
    const parentDir = splitPath.length > 0 ? `${prefix}/${splitPath.join("/")}` : prefix || "/";

    // Define the new tree node
    const treeNode: DatasetVersionFileTreeNode = {
      name,
      type: fileNode.isFile ? "file" : "directory",
      parentDir,
      did,
      dvid,
    };

    // Recursively process children if it's a directory
    if (!fileNode.isFile && fileNode.children) {
      treeNode.children = parseFileNodesToTreeNodes(fileNode.children, datasetName, did, versionName, dvid);
    }

    return treeNode;
  });
}
