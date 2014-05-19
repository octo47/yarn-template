package octo47.yarn.master.spec;

import com.beust.jcommander.internal.Maps;
import com.beust.jcommander.internal.Sets;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * @author Andrey Stepachev
 */
public class TaskSpecQueue {

  private final static Logger LOG = LoggerFactory.getLogger(TaskSpecQueue.class);
  public static final String ANY = "*";

  private Map<String, PriorityQueue<TaskSpec>> withHosts = Maps.newHashMap();
  private PriorityQueue<TaskSpec> anyHosts = mkPq();
  private Set<String> availableHosts = Sets.newHashSet();

  private static TypeReference<List<TaskSpec>> wpType =
          new TypeReference<List<TaskSpec>>() {
          };

  public TaskSpecQueue(File specFile) throws IOException {
    this(new ObjectMapper()
            .reader(wpType).<List<TaskSpec>>readValue(specFile));
  }

  public TaskSpecQueue(@Nonnull Iterable<TaskSpec> workParts) {
    int wpCount = 0;
    for (TaskSpec taskSpec : workParts) {
      add(taskSpec);
      wpCount++;
    }
    LOG.info("Initializing allocator: " + wpCount + " work parts, " +
            +withHosts.keySet().size() + " unique hosts, " +
            +anyHosts.size() + " any host parts ");
    if (LOG.isDebugEnabled()) {
      LOG.debug("   hosts involved " + withHosts.keySet());
    }
  }

  public void add(@Nonnull TaskSpec taskSpec) {
    final List<String> wpHosts = taskSpec.getHosts();
    if (wpHosts.isEmpty()) {
      anyHosts.add(taskSpec);
    } else {
      for (String wpHost : wpHosts) {
        if (!availableHosts.contains(wpHost)) {
          availableHosts.add(wpHost);
        }
        final String host = wpHost.trim();
        if (host.isEmpty())
          throw new IllegalArgumentException("Host must not be empty");
        if (host.equals(ANY))
          anyHosts.add(taskSpec);
        else
          pq(wpHost).add(taskSpec);
      }
    }
  }

  public boolean isEmpty() {
    return withHosts.isEmpty() && anyHosts.isEmpty();
  }

  public Map<String, TaskSpec> getHead(boolean withAnyHosts) {
    final Map<String, TaskSpec> tasks = Maps.newHashMap();
    final Set<String> hosts = Sets.newHashSet();
    hosts.addAll(withHosts.keySet());
    for (String host : hosts) {
      final TaskSpec forHost = getForHost(host);
      if (forHost != null) {
        tasks.put(host, forHost);
      }
    }
    if (withAnyHosts && !anyHosts.isEmpty())
      tasks.put(ANY, anyHosts.poll());
    return tasks;
  }

  /**
   * Return next workpart for host
   */
  @Nullable
  public TaskSpec getForHost(@Nonnull String hostName) {
    return getForHost(hostName, false);
  }

  /**
   * Return next workpart for host
   */
  @Nullable
  public TaskSpec getForHost(@Nonnull String hostName, boolean exactMatch) {
    final PriorityQueue<TaskSpec> queue = withHosts.get(hostName);
    final TaskSpec next;
    if (queue != null && !queue.isEmpty()) {
      next = queue.poll();
      if (queue.isEmpty())
        withHosts.remove(hostName);
      for (String host : next.getHosts()) {
        if (host.equals(hostName))
          continue;
        // remove from other queues too
        removeFromQueue(next, host);
      }
    } else if (!exactMatch && !anyHosts.isEmpty()) {
      next = anyHosts.poll();
    } else {
      next = null;
    }
    return next;
  }

  private void removeFromQueue(TaskSpec next, String host) {
    final PriorityQueue<TaskSpec> queueForRemove = withHosts.get(host);
    if (queueForRemove != null) {
      queueForRemove.remove(next);
      if (queueForRemove.isEmpty())
        withHosts.remove(host);
    }
  }

  private PriorityQueue<TaskSpec> pq(String hostName) {
    PriorityQueue<TaskSpec> taskSpecs = withHosts.get(hostName);
    if (taskSpecs == null) {
      taskSpecs = mkPq();
      withHosts.put(hostName, taskSpecs);
    }
    taskSpecs.iterator();
    return taskSpecs;
  }

  private PriorityQueue<TaskSpec> mkPq() {
    return new PriorityQueue<TaskSpec>(3, TaskSpec.PRIORITY_COMPARATOR);
  }

  @Override
  public String toString() {
    return "TaskSpecQueue{" +
            "withHosts=" + withHosts +
            ", anyHosts=" + anyHosts +
            '}';
  }

}
