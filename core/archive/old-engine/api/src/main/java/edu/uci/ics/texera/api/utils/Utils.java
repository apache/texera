package edu.uci.ics.texera.api.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import edu.uci.ics.texera.api.constants.DataConstants;
import edu.uci.ics.texera.api.constants.DataConstants.TexeraProject;
import edu.uci.ics.texera.api.exception.StorageException;

public class Utils {
    
    // cache the texera home path once it's found
    private static Path TEXERA_HOME_PATH = null;
	
	public static Path getDefaultIndexDirectory() throws StorageException {
		return getTexeraHomePath().resolve("index");
	}
    
    /**
     * Gets the path of resource files under the a subproject's resource folder (in src/main/resources)
     * 
     * @param resourcePath, the path to a resource relative to subproject/src/main/resources
     * @param subProject, the sub project where the resource is located
     * @return the path to the resource
     * @throws StorageException if finding fails
     */
    public static Path getResourcePath(String resourcePath, TexeraProject subProject) throws StorageException {
        resourcePath = resourcePath.trim();
        if (resourcePath.startsWith("/")) {
            resourcePath = resourcePath.substring(1);
        }
        return getTexeraHomePath()
                .resolve(subProject.getProjectName())
                .resolve("src/main/resources")
                .resolve(resourcePath);
    }
    
    /**
     * Gets the real path of the texera home directory by:
     *   1): check if the current directory is texera/core (where TEXERA_HOME should be), 
     *   if it's not then:
     *   2): search the parents all the way up to find the texera home path
     *   if it's not then:
     *   3): search the siblings and children to find the texera home path
     *   
     *   Finding texera home directory will fail
     * 
     * @return the real absolute path to texera home directory
     * @throws StorageException if can not find texera home
     */
    public static Path getTexeraHomePath() throws StorageException {
        if (TEXERA_HOME_PATH != null) {
            return TEXERA_HOME_PATH;
        }
        try {
            Path currentWorkingDirectory = Paths.get(".").toRealPath();
            
            // check if the current directory is the texera home path
            if (isTexeraHomePath(currentWorkingDirectory)) {
                TEXERA_HOME_PATH = currentWorkingDirectory;
                return currentWorkingDirectory;
            }
            
            // search parents all the way up to find texera home path
            Path searchParents = currentWorkingDirectory.getParent();
            while (searchParents.getParent() != null) {
                if (isTexeraHomePath(searchParents)) {
                    TEXERA_HOME_PATH = searchParents;
                    return searchParents;
                }
                searchParents = searchParents.getParent();
            }
            
            // from current path's parent directory, search itschildren to find texera home path
            // current max depth is set to 2 (current path's siblings and direct children)
            Optional<Path> searchChildren = Files.walk(currentWorkingDirectory.getParent(), 2).filter(path -> isTexeraHomePath(path)).findAny();
            if (searchChildren.isPresent()) {
                TEXERA_HOME_PATH = searchChildren.get();
                return searchChildren.get();
            }
                 
            throw new StorageException(
                    "Finding texera home path failed. Current working directory is " + currentWorkingDirectory);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }
    
    private static boolean isTexeraHomePath(Path path) {
        try {
            Path realPath = path.toRealPath();
            if (realPath.endsWith(DataConstants.HOME_FOLDER_NAME)) {
                // make sure all sub projects exist in this folder
                List<String> allSubProjectNames = DataConstants.TexeraProject.getAllProjectNames();
                List<String> actualSubFiles = Files.list(realPath).map(p -> p.getFileName().toString()).collect(Collectors.toList());
                boolean allSubProjectsPresent = allSubProjectNames.stream().allMatch(subProject -> actualSubFiles.contains(subProject));
                
                if (allSubProjectsPresent) {
                    return true;
                }
            }
            return false;
        } catch (IOException | NullPointerException e) {
            return false;
        }
    }

}
