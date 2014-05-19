package octo47.yarn.master.spec;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * @author Andrey Stepachev
 */
@JsonAutoDetect
public class TaskSpec {
  private final String id;
  private final List<String> commands;
  private final List<String> hosts;
  private Map<String, String> env = Maps.newHashMap();
  private List<String> outputs = Lists.newArrayList();
  private int priority = 1;
  private int memoryMB = 350;
  private int vcores = 1;

  public TaskSpec(@JsonProperty("id") String id,
                  @JsonProperty("commands") List<String> commands,
                  @JsonProperty("hosts") List<String> hosts) {
    this.id = id;
    this.commands = commands;
    this.hosts = hosts;
  }

  public TaskSpec(String id, List<String> commands, List<String> hosts,
                  Map<String, String> env, int memoryMB, int vcores) {
    this.id = id;
    this.commands = commands;
    this.hosts = hosts;
    this.env = env;
    this.memoryMB = memoryMB;
    this.vcores = vcores;
  }

  public String getId() {
    return id;
  }

  public List<String> getCommands() {
    return commands;
  }

  public List<String> getHosts() {
    return hosts;
  }

  @Nullable
  @JsonIgnore
  public String[] getHostsArray() {
    if (hosts.size() == 0)
      return null;
    else
      return hosts.toArray(new String[hosts.size()]);
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public void setEnv(Map<String, String> env) {
    this.env = env;
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    if (priority < 1) {
      throw new IllegalArgumentException("Priority should be >= 1");
    }
    this.priority = priority;
  }

  public List<String> getOutputs() {
    return outputs;
  }

  public void setOutputs(List<String> outputs) {
    this.outputs = outputs;
  }

  public int getMemoryMB() {
    return memoryMB;
  }

  public void setMemoryMB(int memoryMB) {
    this.memoryMB = memoryMB;
  }

  public int getVcores() {
    return vcores;
  }

  public void setVcores(int vcores) {
    this.vcores = vcores;
  }

  /**
   * Compare by asc priority, desc memory and desc vcores
   */
  public static final Comparator<? super TaskSpec> PRIORITY_COMPARATOR = new Comparator<TaskSpec>() {
    @Override
    public int compare(TaskSpec o1, TaskSpec o2) {
      int c;
      c = Integer.compare(o1.priority, o2.priority); // ascending
      if (c != 0)
        return c;

      c = Integer.compare(o2.memoryMB, o1.memoryMB); // descending
      if (c != 0)
        return c;

      c = Integer.compare(o2.vcores, o1.vcores);  // descending
      if (c != 0)
        return c;

      return 0;
    }
  };

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    TaskSpec taskSpec = (TaskSpec) o;

    if (memoryMB != taskSpec.memoryMB) return false;
    if (priority != taskSpec.priority) return false;
    if (vcores != taskSpec.vcores) return false;
    if (commands != null ? !commands.equals(taskSpec.commands) : taskSpec.commands != null)
      return false;
    if (id != null ? !id.equals(taskSpec.id) : taskSpec.id != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (commands != null ? commands.hashCode() : 0);
    result = 31 * result + priority;
    result = 31 * result + memoryMB;
    result = 31 * result + vcores;
    return result;
  }

  @Override
  public String toString() {
    return "WorkPart{" +
            "name='" + id + '\'' +
            ", commands=" + commands +
            ", priority=" + priority +
            ", memoryMB=" + memoryMB +
            ", vcores=" + vcores +
            '}';
  }
}
