package octo47.yarn;

/**
 * @author Andrey Stepachev
 */
public class Constant {
  // application type
  public static final String APP_TYPE = "YARN";

  public static final String APP_JAR_NAME = "yarnApp.jar";

  public static final String APP_STAGING_DIR = ".yarn/staging";

  public static final String APP_DIST_CACHE = "YARN_DIST_CACHE";

  public static final String APP_OUTPUT = "YARN_OUTPUT";

  // application master command line arguments
  public static final int APP_JAR_PERMISSIONS = 00660;
  public static final short APP_RESOURCE_DEFAULT_REPLICATION = 10;
  public static final String APP_JOB_SPEC_NAME = "jobspec.json";
}
