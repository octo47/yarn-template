package octo47.yarn.client;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import octo47.yarn.Constant;
import octo47.yarn.util.LocalizerHelper;

import javax.inject.Provider;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class YarnClientService extends AbstractScheduledService {

  private static final Set<YarnApplicationState> DONE =
          EnumSet.of(
                  YarnApplicationState.FAILED,
                  YarnApplicationState.FINISHED,
                  YarnApplicationState.KILLED);

  private static final Log LOG = LogFactory.getLog(YarnClientService.class);

  private final YarnClientParameters parameters;
  private final Provider<YarnClient> yarnClientFactory;
  private final Stopwatch stopwatch;

  private YarnClient yarnClient;
  private ApplicationId applicationId;
  private ApplicationReport finalReport;
  private boolean timeout = false;

  public YarnClientService(YarnClientParameters params) {
    this(params, new YarnClientProvider(params.getConfiguration()),
            new Stopwatch());
  }

  public YarnClientService(YarnClientParameters parameters,
                           Provider<YarnClient> yarnClientFactory,
                           Stopwatch stopwatch) {
    Preconditions.checkArgument(
            parameters.getJobSpec() != null &&
                    parameters.getJobSpec().canRead() &&
                    parameters.getJobSpec().isFile(),
            "Need valid jobspec " + parameters.getJobSpec()
    );
    this.parameters = Preconditions.checkNotNull(parameters);
    this.yarnClientFactory = yarnClientFactory;
    this.stopwatch = stopwatch;

  }

  @Override
  protected void startUp() {
    this.yarnClient = yarnClientFactory.get();
    reportYarnClusterStatus();

    YarnClientApplication clientApp = getNewApplication();
    GetNewApplicationResponse newApp = clientApp.getNewApplicationResponse();

    final Resource maxResources = newApp.getMaximumResourceCapability();
    LOG.info(String.format("Max resource capability %s", maxResources));

    ApplicationSubmissionContext appContext = clientApp.getApplicationSubmissionContext();
    this.applicationId = appContext.getApplicationId();
    ContainerLaunchContext amContainer = getContainerLaunchContext();

    appContext.setApplicationType(Constant.APP_TYPE);
    appContext.setApplicationName(parameters.getApplicationName());

    // Set up resource requirements for ApplicationMaster
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(parameters.getMasterMemory());
    capability.setVirtualCores(parameters.getMasterVCores());
    amContainer.setCommands(Collections.singletonList(this.getCommand()));

    // put everything together.
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(capability);
    appContext.setQueue(parameters.getApplicationQueue());

    // Submit application
    submitApplication(appContext);

    // Make sure we stop the application in the case that it isn't done already.
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        if (YarnClientService.this.isRunning()) {
          YarnClientService.this.stop();
        }
      }
    });

    stopwatch.start();
  }

  private ContainerLaunchContext getContainerLaunchContext() {
    try {
      ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

      final LocalizerHelper localizerHelper =
              new LocalizerHelper(getConf(), getAppStagingDir());

      localizerHelper.addLocalFileDistCache(Constant.APP_JAR_NAME, parameters.getMasterJar());
      localizerHelper.addLocalFileMasterOnly(Constant.APP_JOB_SPEC_NAME, parameters.getJobSpec().getPath());
      for (String file : parameters.getFiles()) {
        final Path path = new Path(file);
        localizerHelper.addLocalFileDistCache(path.getName(), file);
      }
      amContainer.setLocalResources(localizerHelper.getResources());

      // Set up CLASSPATH for ApplicationMaster
      Map<String, String> appMasterEnv = new HashMap<String, String>();
      if (parameters.getOutputPath() != null)
        appMasterEnv.put(Constant.APP_OUTPUT, parameters.getOutputPath());

      setupAppMasterEnv(appMasterEnv);
      localizerHelper.serializeDistCace(appMasterEnv);
      appMasterEnv.putAll(parameters.getEnvironment());
      amContainer.setEnvironment(appMasterEnv);
      return amContainer;
    } catch (Exception e) {
      LOG.error("IOException thrown submitting application", e);
      stop();
    }
    return null;
  }

  private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
    StringBuilder classPathEnv = new StringBuilder();
    classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
    classPathEnv.append("./*");

    for (String c : getConf().getStrings(
            YarnConfiguration.YARN_APPLICATION_CLASSPATH,
            YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
      classPathEnv.append(File.pathSeparatorChar);
      classPathEnv.append(c.trim());
    }

    String envStr = classPathEnv.toString();
    LOG.info("env: " + envStr);
    appMasterEnv.put(ApplicationConstants.Environment.CLASSPATH.name(), envStr);
  }

  private String getCommand() {
    String r = ApplicationConstants.Environment.JAVA_HOME.$() +
            "/bin/java"
            + " " + "-Xmx" + parameters.getMasterMemory() + "M"
            + " " + parameters.getMasterClass()
            + " " + "1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
            + " " + "2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
    LOG.info("command : " + r);
    return r;
  }

  private String getAppStagingDir() {
    return Constant.APP_STAGING_DIR + Path.SEPARATOR
            + applicationId.toString() + Path.SEPARATOR;
  }

  private Configuration getConf() {
    return parameters.getConfiguration();
  }

  private void reportYarnClusterStatus() {
    try {
      YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
      LOG.info("Got Cluster metric info from ASM" + ", numNodeManagers=" + clusterMetrics.getNumNodeManagers());

      if (parameters.isDebug()) {
        List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
        LOG.info("Got Cluster node info from ASM");
        for (NodeReport node : clusterNodeReports) {
          LOG.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId() + ", nodeAddress"
                  + node.getHttpAddress() + ", nodeRackName" + node.getRackName() + ", nodeNumContainers"
                  + node.getNumContainers());
        }
      }
      QueueInfo queueInfo = yarnClient.getQueueInfo(this.parameters.getApplicationQueue());
      LOG.info("Queue info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity="
              + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
              + ", queueApplicationCount=" + queueInfo.getApplications().size() + ", queueChildQueueCount="
              + queueInfo.getChildQueues().size());

      List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
      for (QueueUserACLInfo aclInfo : listAclInfo) {
        for (QueueACL userAcl : aclInfo.getUserAcls()) {
          LOG.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl="
                  + userAcl.name());
        }
      }
    } catch (YarnException e) {
      LOG.error("Exception thrown submitting application", e);
      stop();
    } catch (Exception e) {
      LOG.error("IOException thrown submitting application", e);
      stop();
    }

  }

  private void submitApplication(ApplicationSubmissionContext appContext) {
    LOG.info("Submitting application to the applications manager");
    try {
      yarnClient.submitApplication(appContext);
    } catch (YarnException e) {
      LOG.error("Exception thrown submitting application", e);
      stop();
    } catch (Exception e) {
      LOG.error("IOException thrown submitting application", e);
      stop();
    }
  }

  private YarnClientApplication getNewApplication() {
    try {
      return yarnClient.createApplication();
    } catch (YarnException e) {
      LOG.error("Exception thrown getting new application", e);
      stop();
      return null;
    } catch (Exception e) {
      stop();
      return null;
    }
  }

  @Override
  protected void shutDown() {
    if (finalReport != null) {
      YarnApplicationState state = finalReport.getYarnApplicationState();
      FinalApplicationStatus status = finalReport.getFinalApplicationStatus();
      String diagnostics = finalReport.getDiagnostics();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == status) {
          LOG.info("Application completed successfully.");
        } else {
          LOG.info("Application finished unsuccessfully."
                  + " State = " + state.toString() + ", FinalStatus = " + status.toString());
        }
      } else if (YarnApplicationState.KILLED == state
              || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not complete successfully."
                + " State = " + state.toString() + ", FinalStatus = " + status.toString());
        if (diagnostics != null) {
          LOG.info("Diagnostics = " + diagnostics);
        }
      }
    } else {
      // Otherwise, we need to kill the application, if it was created.
      if (applicationId != null) {
        LOG.info("Killing application id = " + applicationId);
        try {
          yarnClient.killApplication(applicationId);
        } catch (YarnException e) {
          LOG.error("Exception thrown killing application", e);
        } catch (IOException e) {
          LOG.error("IOException thrown killing application", e);
        }
        LOG.info("Application was killed.");
      }
    }
    if (!parameters.isDebug()) {
      try {
        final LocalizerHelper localizerHelper =
                new LocalizerHelper(getConf(), getAppStagingDir());
        LOG.info("Cleaning stage directory " + localizerHelper.getAppStagingPath());
        localizerHelper.cleanup();
      } catch (IOException e) {
        LOG.error("Can't cleanup staging dir " + getAppStagingDir());
      }
    }

  }

  public boolean isApplicationFinished() {
    return timeout || finalReport != null;
  }

  public ApplicationReport getFinalReport() {
    if (!timeout && finalReport == null) {
      finalReport = getApplicationReport();
    }
    return finalReport;
  }

  public ApplicationReport getApplicationReport() {
    try {
      return yarnClient.getApplicationReport(applicationId);
    } catch (YarnException e) {
      LOG.error("Exception occurred requesting application report", e);
      return null;
    } catch (Exception e) {
      LOG.error("IOException occurred requesting application report", e);
      return null;
    }
  }

  @Override
  protected void runOneIteration() throws Exception {
    if (isApplicationFinished()) {
      LOG.info("Nothing to do, application is finished");
      return;
    }

    ApplicationReport report = getApplicationReport();
    if (report == null) {
      LOG.error("No application report received");
    } else if (DONE.contains(report.getYarnApplicationState()) ||
            report.getFinalApplicationStatus() != FinalApplicationStatus.UNDEFINED) {
      finalReport = report;
      stop();
    }

    // Ensure that we haven't been running for all that long.
    if (parameters.getClientTimeoutMillis() > 0 &&
            stopwatch.elapsedMillis() > parameters.getClientTimeoutMillis()) {
      LOG.warn("Stopping application due to timeout.");
      timeout = true;
      stop();
    }
  }

  @Override
  protected Scheduler scheduler() {
    // TODO: make this configurable
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }

}
