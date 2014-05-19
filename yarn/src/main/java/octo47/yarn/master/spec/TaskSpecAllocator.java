package octo47.yarn.master.spec;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerExitStatus;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jackson.map.ObjectMapper;
import octo47.yarn.master.TaskRunner;
import octo47.yarn.util.LocalizerHelper;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Andrey Stepachev
 */
public class TaskSpecAllocator
        implements NMClientAsync.CallbackHandler, Closeable {
  private static final Log LOG = LogFactory.getLog(TaskSpecAllocator.class);

  private static final int N_THREADS = 6;
  private static final int RELAXED_PRIORITY = 0;
  public static final int MAX_SPEC_FAILS = 3;

  private final NMClientAsyncImpl nmClientAsync;
  private final Map<ContainerId, TaskSpecAssignment> containers =
          new ConcurrentHashMap<ContainerId, TaskSpecAssignment>();

  private final TaskSpecQueue specQueue;
  private final Scheduled scheduled = new Scheduled();
  private final OutputCommiter outputCommiter;
  private final Map<String, Integer> failedTasks =
          new ConcurrentHashMap<String, Integer>();
  private final YarnConfiguration conf;

  private ExecutorService pool = Executors.newFixedThreadPool(N_THREADS);
  private AtomicInteger numFailedContainers = new AtomicInteger();
  private Set<String> hosts = Sets.newHashSet();

  private ObjectMapper om = new ObjectMapper();
  private int runnerMemory = 64;

  public TaskSpecAllocator(File jobSpec, YarnConfiguration conf,
                           OutputCommiter commiter) throws IOException {
    this.conf = conf;
    this.nmClientAsync = new NMClientAsyncImpl(this);
    this.nmClientAsync.init(conf);
    this.nmClientAsync.start();

    this.specQueue = new TaskSpecQueue(jobSpec);

    this.outputCommiter = commiter;
  }

  /**
   * Start containers, returns unused containers
   *
   * @param allocatedContainers allocated by RM containers
   * @return unused
   */
  public synchronized List<Container> allocate(List<Container> allocatedContainers) {
    List<Container> unused = Lists.newArrayList();
    for (Container allocatedContainer : allocatedContainers) {
      final String host = allocatedContainer.getNodeId().getHost();

      final String containerDescr = ", containerId=" + allocatedContainer.getId()
              + ", containerNode=" + host
              + ":" + allocatedContainer.getNodeId().getPort()
              + ", containerNodeURI=" + allocatedContainer.getNodeHttpAddress()
              + ", containerResourceMemory"
              + allocatedContainer.getResource().getMemory()
              + ", containerResourceVirtualCores"
              + allocatedContainer.getResource().getVirtualCores();
      TaskSpec spec = scheduled.remove(host);
      if (spec == null) {
        LOG.info("Host not found in scheduled" + scheduled.toString());
        spec = specQueue.getForHost(host, true);
      }
      if (spec == null) {
        LOG.info("No spec found for container, releasing."
                + containerDescr);
        unused.add(allocatedContainer);
      } else {
        LOG.info("Launching, launching " + spec
                + " " + containerDescr);
        addContainer(
                allocatedContainer, spec);
        LaunchContainerRunnable runnableLaunchContainer =
                new LaunchContainerRunnable(allocatedContainer, spec);
        pool.submit(runnableLaunchContainer);
      }
    }
    return unused;
  }

  public synchronized void complete(List<ContainerStatus> completedContainers) {
    for (ContainerStatus containerStatus : completedContainers) {
      // non complete containers should not be here
      assert (containerStatus.getState() == ContainerState.COMPLETE);

      // increment counters for completed/failed containers
      int exitStatus = containerStatus.getExitStatus();
      if (0 != exitStatus) {
        // container failed
        LOG.info("Container failed, containerId="
                + containerStatus.getContainerId()
                + ":" + containerStatus.getDiagnostics());
        rescheduleContainer(containerStatus, exitStatus, null);
      } else {
        try {
          outputCommiter.commitContainer(containerStatus.getContainerId());
          removeContainer(containerStatus);
          // nothing to do
          // container completed successfully
          LOG.info("Container completed successfully, containerId="
                  + containerStatus.getContainerId());
        } catch (IOException e) {
          LOG.info("Failed to commit container output, containerId="
                  + containerStatus.getContainerId()
                  + ":" + containerStatus.getDiagnostics(), e);
          rescheduleContainer(containerStatus, exitStatus, e);
        }
      }
    }
  }

  public synchronized void updateHosts(List<NodeReport> updatedNodes) {
    Set<String> newHosts = Sets.newHashSet();
    for (NodeReport node : updatedNodes) {
      newHosts.add(node.getNodeId().getHost());
    }
    hosts = Sets.intersection(hosts, newHosts);
  }

  public float getProgress() {
    return 0;
  }

  public int getFailedContainers() {
    return numFailedContainers.get();
  }

  public synchronized List<AMRMClient.ContainerRequest> request() throws IOException {
    List<AMRMClient.ContainerRequest> requests = Lists.newArrayList();
    final Map<String, TaskSpec> forSchedule = specQueue.getHead(false);
    for (Map.Entry<String, TaskSpec> entry : forSchedule.entrySet()) {
      final String host = entry.getKey();
      final TaskSpec spec = entry.getValue();
      requests.add(requestForSpec(host, spec));
      scheduled.add(host, spec);
    }
    for (Map.Entry<String, LinkedList<TaskSpec>> entry : scheduled.specs.entrySet()) {
      for (TaskSpec taskSpec : entry.getValue()) {
        requests.add(requestForSpec(entry.getKey(), taskSpec));
      }
    }
    return requests;
  }

  public synchronized boolean isEmpty() {
    return scheduled.isEmpty()
            && containers.isEmpty()
            && specQueue.isEmpty();
  }

  public synchronized boolean isFailed() {
    LOG.info("Failed due of failed tasks: " + failedTasks);
    return failedTasks.size() > 0;
  }

  /**
   * Thread to connect to the {@link org.apache.hadoop.yarn.api.ContainerManagementProtocol} and launch the container
   * that will execute the shell command.
   */
  private class LaunchContainerRunnable implements Callable<TaskSpec> {

    // container spec
    private final TaskSpec spec;
    // Allocated container
    private final Container container;
    private final Path workPath;

    /**
     * @param lcontainer        Allocated container
     * @param containerListener Callback handler of the container
     */
    public LaunchContainerRunnable(
            Container lcontainer,
            TaskSpec spec) {
      this.container = lcontainer;
      this.spec = spec;
      this.workPath =  outputCommiter.pathForContainer(container.getId());
    }

    @Override
    public TaskSpec call() throws Exception {
      LOG.info("Setting up container launch container for containerid="
              + container.getId());
      ContainerLaunchContext ctx = Records
              .newRecord(ContainerLaunchContext.class);

      // Set the environment
      final Map<String, String> env = Maps.newHashMap();
      setupContainerEnv(env);
      ctx.setEnvironment(env);

      // Set the local resources
      Map<String, LocalResource> localResources =
              LocalizerHelper.deserializeDistCache(System.getenv());
      LOG.info("Resources: " + localResources);
      ctx.setLocalResources(localResources);
      ctx.setCommands(getCommand());

      addContainer(container, spec);

      nmClientAsync.startContainerAsync(container, ctx);
      return spec;
    }

    private void setupContainerEnv(Map<String, String> containerEnv) {
      StringBuilder classPathEnv = new StringBuilder();
      classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$())
              .append(File.pathSeparatorChar);
      classPathEnv.append("./*");

      for (String c : getConf().getStrings(
              YarnConfiguration.YARN_APPLICATION_CLASSPATH,
              YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
        classPathEnv.append(File.pathSeparatorChar);
        classPathEnv.append(c.trim());
      }

      String envStr = classPathEnv.toString();
      LOG.info("container env: " + envStr);
      containerEnv.put(ApplicationConstants.Environment.CLASSPATH.name(), envStr);
    }

    private List<String> getCommand() throws IOException {
      String r = ApplicationConstants.Environment.JAVA_HOME.$() +
              "/bin/java"
              + " " + "-Xmx" + runnerMemory + "M"
              + " " + TaskRunner.class.getName()
              + " " + Base64.encodeBase64URLSafeString(om.writeValueAsBytes(spec))
              + " " + workPath.toUri().toString()
              + " " + "1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout"
              + " " + "2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr";
      LOG.info("command : " + r);
      return Lists.newArrayList(r);
    }
  }


  private static class Scheduled {
    Map<String, LinkedList<TaskSpec>> specs =
            new ConcurrentHashMap<String, LinkedList<TaskSpec>>();

    void add(String host, TaskSpec spec) {
      LinkedList<TaskSpec> taskSpecs = specs.get(host);
      if (taskSpecs == null) {
        taskSpecs = Lists.newLinkedList();
        specs.put(host, taskSpecs);
      }
      taskSpecs.add(spec);
    }

    TaskSpec remove(String host) {
      final LinkedList<TaskSpec> taskSpecs = specs.get(host);
      if (taskSpecs == null || taskSpecs.isEmpty())
        return null;
      final TaskSpec r = taskSpecs.remove();
      if (taskSpecs.isEmpty())
        specs.remove(host);
      return r;
    }

    @Override
    public String toString() {
      return "Scheduled{" +
              "specs=" + specs +
              '}';
    }

    public boolean isEmpty() {
      return specs.isEmpty();
    }
  }

  private void addContainer(Container container, TaskSpec spec) {
    containers.put(container.getId(), new TaskSpecAssignment(spec, container));
  }

  private void removeContainer(ContainerStatus containerStatus) {
    final TaskSpecAssignment assignment = containers.remove(containerStatus.getContainerId());
    if (assignment != null)
      failedTasks.remove(assignment.getSpec().getId());
  }

  private synchronized void rescheduleContainer(ContainerStatus status,
                                                int exitStatus,
                                                Throwable t) {
    final TaskSpecAssignment assignment =  containers.remove(status.getContainerId());
    final boolean preempted = ContainerExitStatus.ABORTED == exitStatus;
    if (assignment == null) {
      if (!preempted) {
        numFailedContainers.incrementAndGet();
      }
      return;
    }

    if (!preempted) {
      Integer prevFails = failedTasks.get(assignment.getSpec().getId());
      if (prevFails == null)
        prevFails = 1;
      prevFails += 1;
      failedTasks.put(assignment.getSpec().getId(), prevFails);
      if (prevFails >= MAX_SPEC_FAILS) {
        LOG.error("Container " + assignment.getSpec() + " reached maximum number of failures + " + MAX_SPEC_FAILS, t);
        numFailedContainers.incrementAndGet();
        return;
      }
    }
    LOG.error("Container rescheduled: " + assignment.getSpec(), t);
    specQueue.add(assignment.getSpec());
  }

  private AMRMClient.ContainerRequest requestForSpec(String host, TaskSpec spec) {
    final boolean isAny = host.equals(ResourceRequest.ANY);
    final String[] hosts = isAny ? null : new String[]{host};

    Priority pri = Records.newRecord(Priority.class);
    // assign zero priority for 'any host' specs
    pri.setPriority(isAny ? RELAXED_PRIORITY : spec.getPriority());

    // Set up resource type requirements
    // For now, memory and CPU are supported so we set memory and cpu requirements
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(spec.getMemoryMB() + runnerMemory);
    capability.setVirtualCores(spec.getVcores());

    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability,
            hosts, null,
            pri, false);
    LOG.info("Requested container: " + request.toString() + " hosts: "
            + request.getNodes() + " racks:" + request.getRacks());
    return request;
  }


  @Override
  public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
    LOG.info(String.format("Container %s started", containerId));
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
    LOG.info(String.format("Container %s status received %s", containerId, containerStatus));
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    LOG.info(String.format("Container %s stopped", containerId));
  }

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.info(String.format("Container %s error", containerId), t);
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    LOG.info(String.format("Container %s status error", containerId), t);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    LOG.info(String.format("Container %s stop error", containerId), t);
  }

  public YarnConfiguration getConf() {
    return conf;
  }

  @Override
  public void close() throws IOException {
    nmClientAsync.close();
  }
}
