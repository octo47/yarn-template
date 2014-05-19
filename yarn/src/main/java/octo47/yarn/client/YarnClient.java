package octo47.yarn.client;

import com.beust.jcommander.JCommander;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * @author Andrey Stepachev
 */
public class YarnClient extends Configured implements Tool {

  private static final Log LOG = LogFactory.getLog(YarnClient.class);

  @Override
  public int run(String[] args) throws Exception {

    Configuration conf = getConf();
    final YarnClientParameters parameters = new YarnClientParameters(conf);
    final JCommander jc = new JCommander(parameters, args);
    if (parameters.isHelp()) {
      jc.usage();
      return 1;
    }
    YarnClientService service = new YarnClientService(parameters);
    return handle(service);
  }

  public int handle(YarnClientService service) throws Exception {
    service.startAndWait();
    if (!service.isRunning()) {
      LOG.error("Service failed to startup, exiting...");
      return 1;
    }

    String trackingUrl = null;
    while (service.isRunning()) {
      if (trackingUrl == null) {
        Thread.sleep(1000);
        ApplicationReport report = service.getApplicationReport();
        YarnApplicationState yarnAppState = report.getYarnApplicationState();
        if (yarnAppState == YarnApplicationState.RUNNING) {
          trackingUrl = report.getTrackingUrl();
          if (trackingUrl == null || trackingUrl.isEmpty()) {
            LOG.info("Application is running, but did not specify a tracking URL");
            trackingUrl = "";
          } else {
            LOG.info("Master Tracking URL = " + trackingUrl);
          }
        }
      }
    }

    LOG.info("Checking final app report");
    ApplicationReport report = service.getFinalReport();
    if (report == null || report.getFinalApplicationStatus() != FinalApplicationStatus.SUCCEEDED) {
      return 1;
    }
    LOG.info("Yarn client finishing...");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new YarnClient(), args);
    System.exit(rc);
  }
}
