package edu.uci.ics.texera.web;
import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

public class OPversion {
    private static Git git = null;
    private static String currentPath = System.getProperty("user.dir");
    private static Map<String, String> opMap = new HashMap<>();
    static {
        try {
            Path path = Paths.get(currentPath);
            Path amberPath = Paths.get("core/amber/");
            if(path.endsWith(amberPath)){
                path = Paths.get(currentPath).getParent().getParent();
            }
            git = Git.open(new File(path+"/.git"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getVersion(String operatorName, String operatorPath) {
        if(!opMap.containsKey(operatorName)) {
            try {
                String version = git.log().addPath(operatorPath).setMaxCount(1).call().iterator().next().getName();
                opMap.put(operatorName, version);
            } catch (GitAPIException e) {
                e.printStackTrace();
            }
        }
        return opMap.get(operatorName);
    }

}
