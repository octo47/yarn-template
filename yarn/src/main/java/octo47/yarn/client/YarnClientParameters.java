package octo47.yarn.client;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.beust.jcommander.validators.PositiveInteger;
import octo47.yarn.master.ApplicationMasterMain;
import org.apache.hadoop.conf.Configuration;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * @author Andrey Stepachev
 */
@Parameters
public class YarnClientParameters {
  private final Configuration configuration;

  @Parameter(names = {"--name", "-n"}, description = "Application name")
  private String applicationName = "YarnClient";
  @Parameter(names = {"--queue", "-q"}, description = "Application queue")
  private String applicationQueue = "default";
  @Parameter(names = {"--timeout-ms", "-t"}, description = "Submission timeout",
          validateWith = PositiveInteger.class)
  private long clientTimeoutMillis;
  @Parameter(names = {"--master-priority"}, description = "Application priority",
          validateWith = PositiveInteger.class)
  private long masterPriority = 0;
  @Parameter(names = "--master-memory",
          description = "Master application memory allocation",
          validateWith = PositiveInteger.class)
  private int masterMemory = 1024;
  @Parameter(names = "--master-vcores", description = "Master application vcores allocation",
          validateWith = PositiveInteger.class)
  private int masterVCores = 1;

  @Parameter(names = "--job-spec", description = "Job specification", required = true)
  private File jobSpec;
  @Parameter(names = "--output", description = "Job output directory, each container writes" +
          "its output into subdirectory, iff job-spec describes outputs", required = true)
  private String outputPath;

  @Parameter(names = {"--jar", "-j"}, description = "Master application jar", required = true)
  private String masterJar;
  @Parameter(names = {"--class", "-c"}, description = "Master application class")
  private String masterClass = ApplicationMasterMain.class.getName();
  @DynamicParameter(names = {"--env", "-e"}, description = "Environment key value pairs")
  private Map<String, String> environment = Maps.newHashMap();
  @Parameter(names = {"--file", "-f"}, variableArity = true, description = "Resources to pass with containers")
  private List<String> files = Lists.newArrayList();

  @Parameter(names = {"--help", "-h"}, help = true)
  private boolean help;
  @Parameter(names = "--debug", description = "Is debug enabled")
  private boolean debug = false;

  public YarnClientParameters(Configuration configuration) {
    this.configuration = configuration;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  public String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getApplicationQueue() {
    return applicationQueue;
  }

  public void setApplicationQueue(String applicationQueue) {
    this.applicationQueue = applicationQueue;
  }

  public long getClientTimeoutMillis() {
    return clientTimeoutMillis;
  }

  public void setClientTimeoutMillis(long clientTimeoutMillis) {
    this.clientTimeoutMillis = clientTimeoutMillis;
  }

  public long getMasterPriority() {
    return masterPriority;
  }

  public void setMasterPriority(long masterPriority) {
    this.masterPriority = masterPriority;
  }

  public int getMasterMemory() {
    return masterMemory;
  }

  public void setMasterMemory(int masterMemory) {
    this.masterMemory = masterMemory;
  }

  public int getMasterVCores() {
    return masterVCores;
  }

  public void setMasterVCores(int masterVCores) {
    this.masterVCores = masterVCores;
  }

  public String getMasterJar() {
    return masterJar;
  }

  public void setMasterJar(String masterJar) {
    this.masterJar = masterJar;
  }

  public String getMasterClass() {
    return masterClass;
  }

  public void setMasterClass(String masterClass) {
    this.masterClass = masterClass;
  }

  public boolean isDebug() {
    return debug;
  }

  public void setDebug(boolean debug) {
    this.debug = debug;
  }

  public boolean isHelp() {
    return help;
  }

  public void setHelp(boolean help) {
    this.help = help;
  }

  public Map<String, String> getEnvironment() {
    return environment;
  }

  public void setEnvironment(Map<String, String> environment) {
    this.environment = environment;
  }

  public List<String> getFiles() {
    return files;
  }

  public void setFiles(List<String> files) {
    this.files = files;
  }

  public File getJobSpec() {
    return jobSpec;
  }

  public void setJobSpec(File jobSpec) {
    this.jobSpec = jobSpec;
  }

  public String getOutputPath() {
    return outputPath;
  }

  public void setOutputPath(String outputPath) {
    this.outputPath = outputPath;
  }
}
