package edu.uci.ics.texera.web;
import java.io.*;
import java.nio.file.Paths;
import java.util.Properties;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.lib.Repository;

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
    private static Repository repository = git.getRepository();
    private static Properties prop = new Properties();
    // The folder that is tracked whether is changed or not
    private static String operatorPath = "core/amber/src/main/scala/edu/uci/ics/texera/workflow/operators";
    // saved path for operator version properties file
    private static String versionPath = currentPath+"/operator_version.properties";

    public static void refreshVersion() throws GitAPIException, IOException{
        String version;
        File versionConfig = new File(versionPath);
        if (!versionConfig.exists()){
            // initialize latest commit hash code to version if operator version hasn't been introduced
            version = git.log().addPath(operatorPath).setMaxCount(1).call().iterator().next().getName();
            storeVersion(version);
        } else {
            updateVersion();
        }
    };

    // store the operator version into properties file
    private static void storeVersion(String version) throws FileNotFoundException{
        try (OutputStream output = new FileOutputStream(versionPath)) {
            prop.setProperty("OperatorVersion", version);
            prop.store(output, null);
        } catch (IOException io) {
            io.printStackTrace();
        }
    };

    // update the operator version if current one is outdated
    private static void updateVersion() throws GitAPIException, IOException{
        String version = "";
        try (InputStream input = new FileInputStream(versionPath)) {
            prop.load(input);
            version = prop.getProperty("OperatorVersion");
        } catch (IOException io) {
            io.printStackTrace();
        }
        String latestVersion = git.log().addPath(operatorPath).setMaxCount(1).call().iterator().next().getName();
        if (!latestVersion.equals(version)) {
            version = latestVersion;
            storeVersion(version);
        }
    }
}
