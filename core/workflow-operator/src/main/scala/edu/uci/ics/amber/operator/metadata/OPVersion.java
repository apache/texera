package edu.uci.ics.amber.operator.metadata;

import edu.uci.ics.amber.util.PathUtils;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class OPVersion {
    private static Git git = null;
    private static Map<String, String> opMap = new HashMap<>();
    static {
        try {
            git = Git.open(new File(PathUtils.gitDirectoryPath().toString()));
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
            } catch (NullPointerException e) {
                opMap.put(operatorName, "N/A");
            }
        }
        return opMap.get(operatorName);
    }

}
