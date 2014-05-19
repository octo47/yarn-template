package octo47.yarn.master.spec;

import org.apache.hadoop.yarn.api.records.Container;

/**
* @author Andrey Stepachev
*/
public class TaskSpecAssignment {
  private final TaskSpec spec;
  private final Container container;

  public TaskSpecAssignment(TaskSpec spec, Container container) {
    this.spec = spec;
    this.container = container;
  }

  public TaskSpec getSpec() {
    return spec;
  }

  public Container getContainer() {
    return container;
  }
}
