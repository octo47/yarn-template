package octo47.yarn.master;

import com.google.common.util.concurrent.AbstractScheduledService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.webapp.WebApp;
import org.apache.hadoop.yarn.webapp.WebApps;
import octo47.yarn.Constant;
import octo47.yarn.master.spec.OutputCommiter;
import octo47.yarn.master.spec.TaskSpecAllocator;
import octo47.yarn.master.webapp.AMWebApp;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Andrey Stepachev
 */
public class ApplicationMasterImpl extends AbstractScheduledService {

  private static final Log LOG = LogFactory.getLog(ApplicationMasterImpl.class);
  private static final int MAX_FAILED_CONAINERS = 10; // TODO: make configurable

  private final ApplicationMasterParameters parameters;
  private final YarnConfiguration conf;

  private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
  private TaskSpecAllocator allocator;

  private boolean registered = false;
  private Throwable throwable;
  private volatile boolean done = false;
  private RunningAppContext appContext;
  private WebApp webApp;

  public ApplicationMasterImpl(ApplicationMasterParameters parameters, Configuration conf) {
    this.parameters = parameters;
    this.conf = new YarnConfiguration(conf);
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {

    LOG.info("Initiating allocator");
    allocator = new TaskSpecAllocator(
            new File(Constant.APP_JOB_SPEC_NAME), conf,
            new OutputCommiter(
                    conf,
                    parameters.getOutput(),
                    parameters.getApplicationAttemptId().getApplicationId())
    );

    LOG.info("Initiating RM connection");
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);
    amRMClient.init(conf);
    amRMClient.start();

    try {
      RegisterApplicationMasterResponse registration =
              amRMClient.registerApplicationMaster(
                      parameters.getNmHost(),
                      parameters.getNmPort(),
                      parameters.getTrackingUrl());
      registered = true;
      LOG.info("Registered at queue: " + registration.getQueue());
    } catch (Exception e) {
      LOG.error("Exception thrown registering application master", e);
      stop();
    }

    appContext = new RunningAppContext();

    try {
      // TODO: Explicitly disable SSL
      webApp =
              WebApps.$for("yarn-template", AppContext.class, appContext, "ws")
                      .start(new AMWebApp());
    } catch (Exception e) {
      LOG.error("Webapps failed to start. Ignoring for now:", e);
      webApp = null;
    }
  }

  /**
   * Stop the service. This is guaranteed not to run concurrently with {@link #runOneIteration}.
   */
  @Override
  protected void shutDown() throws Exception {

    allocator.close();

    try {
      if (registered) {
        if (webApp != null)
          webApp.stop();
        FinalApplicationStatus status;
        String message = null;
        if (state() == State.FAILED
                || throwable != null
                || allocator.isFailed()) {
          //TODO: diagnostics
          status = FinalApplicationStatus.FAILED;
          if (throwable != null) {
            message = throwable.getLocalizedMessage();
          }
        } else {
          status = FinalApplicationStatus.SUCCEEDED;
        }
        LOG.info("Sending finish request with status = " + status
                + " and message = "
                + message + " (service state = " + state());
        try {
          amRMClient.unregisterApplicationMaster(status, message, null);
        } catch (Exception e) {
          LOG.error("Error finishing application master", e);
        }
      }
    } finally {
      amRMClient.stop();
    }
  }

  @Override
  protected Scheduler scheduler() {
    return Scheduler.newFixedRateSchedule(0, 1, TimeUnit.SECONDS);
  }

  @Override
  protected void runOneIteration() throws Exception {
    try {
      List<AMRMClient.ContainerRequest> requests = allocator.request();
      for (AMRMClient.ContainerRequest request : requests) {
        LOG.info("Requesting " + request);
        amRMClient.addContainerRequest(request);
      }
      if (allocator.getFailedContainers() > MAX_FAILED_CONAINERS) {
        throw new IOException("Maximum failed containers reached");
      }
      if (done || allocator.isEmpty())
        stop();
    } catch (Exception e) {
      throwable = e;
      LOG.error("Iteration failed", e);
      throw e;
    }

  }

  private class RunningAppContext implements AppContext {

    private long startTime = System.currentTimeMillis();

    @Override
    public ApplicationId getApplicationId() {
      return parameters.getApplicationAttemptId().getApplicationId();
    }

    @Override
    public ApplicationAttemptId getAttemptId() {
      return parameters.getApplicationAttemptId();
    }

    @Override
    public String getUser() {
      return System.getProperty("user.name");
    }

    @Override
    public long getStartTime() {
      return startTime;
    }
  }

  private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {
      LOG.info("Got response from RM for container ask, allocatedCnt="
              + allocatedContainers.size());
      allocator.allocate(allocatedContainers);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {
      LOG.info("Got response from RM for container ask, completedCnt="
              + completedContainers.size());
      allocator.complete(completedContainers);
    }

    @Override
    public void onShutdownRequest() {
      done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
      allocator.updateHosts(updatedNodes);
    }

    @Override
    public float getProgress() {
      return allocator.getProgress();
    }

    @Override
    public void onError(Throwable e) {
      done = true;
      throwable = e;
    }
  }
}
