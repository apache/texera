package edu.uci.ics.texera.web;
import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;

public class OPversion {
    private static final Git git;
    private static String currentPath = System.getProperty("user.dir");
    static {
        try {
            System.out.println();
            git = Git.open(new File(Paths.get(currentPath).getParent().getParent()+"/.git"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static Properties prop = new Properties();
    // saved path for operator version properties file
    private static String versionPath = currentPath+"/operator_version.properties";

    public static void refreshVersion(String operatorName, String operatorPath) throws GitAPIException, IOException{
        try {
            String version = git.log().addPath(operatorPath).setMaxCount(1).call().iterator().next().getName();
            File versionConfig = new File(versionPath);
            if (!versionConfig.exists()){
                // initialize the latest commit hash code to version if operator version hasn't been introduced
                storeVersion(operatorName, version);
            } else {
                updateVersion(operatorName, version);
            }
        } catch (GitAPIException | IOException e) {
            e.printStackTrace();
        }
    };

    // store the operator version into properties file
    private static void storeVersion(String operatorName, String version) throws FileNotFoundException{
        try (OutputStream output = new FileOutputStream(versionPath)) {
            prop.setProperty(operatorName, version);
            prop.store(output, null);
        } catch (IOException io) {
            io.printStackTrace();
        }
    };

    // update the operator version if current one is outdated
    private static void updateVersion(String operatorName, String version) {
        try {
            String preVersion = "";
            InputStream input = new FileInputStream(versionPath);
            try {
                prop.load(input);
                preVersion = prop.getProperty(operatorName);
            } catch (IOException io) {
                io.printStackTrace();
            } finally {
                input.close();
            }
            if (!version.equals(preVersion)) {
                storeVersion(operatorName, version);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
