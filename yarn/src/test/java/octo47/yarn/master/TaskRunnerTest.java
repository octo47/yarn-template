package octo47.yarn.master;

import com.beust.jcommander.internal.Lists;
import org.junit.Test;
import octo47.yarn.master.spec.TaskSpec;

import java.io.File;
import java.io.IOException;

public class TaskRunnerTest {
  @Test
  public void simpleTest() throws IOException {
    final TaskSpec spec = new TaskSpec("test1",
            Lists.newArrayList("ls \".\" | grep yarn > target/out.txt"), Lists.<String>newArrayList());
    new TaskRunner(new File("target"), spec);
  }
}
