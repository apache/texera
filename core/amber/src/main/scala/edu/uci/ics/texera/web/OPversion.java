package edu.uci.ics.texera.web;
import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

public class OPversion {
    private static final Git git;
    private static String currentPath = System.getProperty("user.dir");
    private static Map<String, String> opMap = new HashMap<>();
    static {
        try {
            System.out.println();
            git = Git.open(new File(Paths.get(currentPath).getParent().getParent()+"/.git"));
        } catch (IOException e) {
            throw new RuntimeException(e);
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
