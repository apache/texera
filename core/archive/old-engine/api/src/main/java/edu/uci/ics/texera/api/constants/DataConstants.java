/**
 * 
 */
package edu.uci.ics.texera.api.constants;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Zuozhi Wang (zuozhi)
 */
public class DataConstants {
    
    public static final ObjectMapper defaultObjectMapper = new ObjectMapper();
    
    public static final String HOME_FOLDER_NAME = "core";
    
    public enum TexeraProject {
        TEXERA_API("api"),
        TEXERA_DATAFLOW("dataflow"),
        TEXERA_PERFTEST("perftest"),
        TEXERA_SANDBOX("sandbox"),
        TEXERA_STORAGE("storage"),
        TEXERA_TEXTQL("textql"),
        TEXERA_WEB("web");
        
        private String projectName;
        
        TexeraProject(String projectName) {
            this.projectName = projectName;
        }
        
        public String getProjectName() {
            return this.projectName;
        }
        
        public static List<String> getAllProjectNames() {
            return Arrays.asList(values()).stream()
                    .map(enumValue -> enumValue.getProjectName())
                    .collect(Collectors.toList());
        }
    }

}
