package octo47.yarn.master.spec;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

public class TaskSpecQueueTest {

  private Random r = new Random(1234);

  @Test
  public void testAllocations() throws IOException {
    final List<TaskSpec> parts = Arrays.asList(
            wp("c1:host1,host2:123:1:cmd1"),
            wp("c2:host2,host1:123:1:cmd1"),
            wp("c31:host1:123:1:cmd1"),
            wp("c32:host1:256:1:cmd1"),
            wp("c33:host1:256:2:cmd1"),
            wp("c4:host3,host2:123:1:cmd1"),
            wp("c5:host3:123:1:cmd1"),
            wp("c61:host1,host2,host3:123:2:cmd1"),
            wp("c62:host1,host2,host3:123:2:cmd1"),
            wp("c63:host1,host2,host3:256:1:cmd1"),
            wp("c71:*:123:1:cmd1"),
            wp("c72:*:123:1:cmd1")
    );
    Collections.shuffle(parts, r);
    final TaskSpecQueue allocator =
            new TaskSpecQueue(parts);
    StringBuilder sb = new StringBuilder();
    Set<String> names = Sets.newHashSet();
    Queue<String> hosts = Queues.newConcurrentLinkedQueue();
    hosts.addAll(Lists.newArrayList(
            "host1",
            "host1",
            "host2",
            "host2",
            "host3",
            "host2",
            "host1",
            "host1",
            "host1",
            "host1",
            "host1",
            "host1",
            "host1",
            "host2",
            "host3"
    ));
    while (!allocator.isEmpty()) {
      final String host = hosts.poll();
      if (host == null) {
        Assert.fail("Queue still not empty, " + allocator.toString());
      }
      final TaskSpec wp = allocator.getForHost(host);
      if (wp != null) {
        if (names.contains(wp.getId()))
          Assert.fail("Already returned " + wp);
        else
          names.add(wp.getId());
        sb.append(host).append(" = ")
                .append(wp.getId()).append(", ");
      } else {
        hosts.remove(host);
      }
    }
    Assert.assertEquals("host1 = c33" +
            ", host1 = c63" +
            ", host2 = c61" +
            ", host2 = c62" +
            ", host3 = c4" +
            ", host2 = c2" +
            ", host1 = c32" +
            ", host1 = c1" +
            ", host1 = c31" +
            ", host1 = c72" +
            ", host1 = c71" +
            ", host3 = c5" +
            ", "
            , sb.toString());

  }

  @Test
  public void testReadFromFile() throws IOException {
    final String path = this.getClass().getResource("sample.json").getPath();
    final TaskSpecQueue allocator = new TaskSpecQueue(new File(path));
    Assert.assertFalse(allocator.isEmpty());
  }

  @Test
  public void testGetHead() {
    final List<TaskSpec> parts = Arrays.asList(
            wp("c1:host1,host2:123:1:cmd1"),
            wp("c2:host2,host1:123:1:cmd1"),
            wp("c4:host3,host2:123:1:cmd1"),
            wp("c5:host1,host2:123:1:cmd1"),
            wp("c6:host2,host1:123:1:cmd1"),
            wp("c7:host3,host2:123:1:cmd1"),
            wp("c8:host3:123:1:cmd1"),
            wp("c71:*:123:1:cmd1"),
            wp("c72:*:123:1:cmd1")
    );
    final TaskSpecQueue allocator =
            new TaskSpecQueue(parts);
    final Map<String, TaskSpec> head = allocator.getHead(false); // no '*' hosts
    Assert.assertEquals(3, head.size());
    Assert.assertNull(head.get("*"));
    Assert.assertEquals("c6", head.get("host1").getId());
    Assert.assertEquals("c1", head.get("host2").getId());

    final Map<String, TaskSpec> head2 = allocator.getHead(true);
    Assert.assertEquals(4, head2.size());
    Assert.assertEquals("c71", head2.get("*").getId());
    Assert.assertEquals("c2", head2.get("host1").getId());
    Assert.assertEquals("c5", head2.get("host2").getId());
  }

  /**
   * Create workpart using syntax: name:host,host:mem:vcores:cmd,cmd
   *
   * @return
   */
  private TaskSpec wp(String spec) {
    final String[] parts = spec.split(":");
    final String[] hosts = parts[1].split(",");
    final String[] cmds = parts[4].split(",");
    final String name = parts[0];
    final int mem = Integer.parseInt(parts[2]);
    final int vcores = Integer.parseInt(parts[3]);
    return new TaskSpec(name,
            Arrays.asList(cmds),
            Arrays.asList(hosts),
            Maps.<String, String>newHashMap(),
            mem, vcores);
  }
}
