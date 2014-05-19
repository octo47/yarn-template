package octo47.yarn.master.spec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Andrey Stepachev
 */
public class OutputCommiter implements Closeable {

  private final static Logger LOG = LoggerFactory.getLogger(OutputCommiter.class);

  private final Path outputDir;
  private final Path tmpOutputDir;
  private final Configuration conf;
  private final ApplicationId appId;
  private final FileSystem fs;

  private final Map<ContainerId, Path> containers =
          new ConcurrentHashMap<ContainerId, Path>();

  public OutputCommiter(Configuration conf, Path outputDir, ApplicationId appId) throws IOException {
    this.conf = conf;
    this.appId = appId;
    this.fs = FileSystem.get(conf);
    this.outputDir = fs.makeQualified(outputDir);
    this.tmpOutputDir = fs.makeQualified(new Path(outputDir, ".tmp-" + appId));
    // it is safe to try to create that dirs,
    // we don't restrict to nonexistent directory
    fs.mkdirs(outputDir);
    if (!fs.mkdirs(tmpOutputDir))
      throw new IOException("Unable to create tmpOutputDirs or it exists");
    LOG.info("OutputCommiter: tmp=" + tmpOutputDir);
  }

  public Path pathForContainer(ContainerId container) {
    final Path containerPath = new Path(tmpOutputDir, container.toString());
    containers.put(container, containerPath);
    LOG.info(container + " working dir is: " + containerPath);
    return containerPath;
  }

  public void commitContainer(ContainerId container) throws IOException {
    final Path toRemove = containers.remove(container);
    if (toRemove != null) {
      Path targetName = new Path(outputDir, toRemove.getName());
      if (fs.exists(toRemove)) {
        LOG.info(container + " commiting, moving " + toRemove + " to " + targetName);
        if (!fs.rename(toRemove, targetName)) {
          throw new IOException("Can't move " + toRemove + " to " + targetName);
        }
      } else {
        LOG.info(container + " generated no output");
      }
    }
    LOG.info(container + " commited");
  }

  @Override
  public void close() throws IOException {
    if (conf != null) {
      final FileSystem fs = FileSystem.get(conf);
      fs.delete(tmpOutputDir, true);
      fs.close();
    }
  }
}
