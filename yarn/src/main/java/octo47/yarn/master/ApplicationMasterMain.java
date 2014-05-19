package octo47.yarn.master;

import com.google.common.util.concurrent.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import octo47.yarn.Constant;

import java.io.IOException;

public class ApplicationMasterMain extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(ApplicationMasterMain.class);

  @Override
  public int run(String[] args) throws Exception {

    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());

    ApplicationMasterParameters params = getApplicationMasterParameters();
    final ApplicationMasterImpl service = new ApplicationMasterImpl(params, getConf());
    service.startAndWait();
    while (service.state().equals(Service.State.RUNNING)) {
      Thread.sleep(1000);
    }
    service.stopAndWait();
    return 0;
  }

  private ApplicationMasterParameters getApplicationMasterParameters() throws IOException {
    String containerIdStr =
            System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
    String nodeHostString = System.getenv(ApplicationConstants.Environment.NM_HOST.name());
    String nodePortString = System.getenv(ApplicationConstants.Environment.NM_PORT.name());
    String nodeHttpPortString =
            System.getenv(ApplicationConstants.Environment.NM_HTTP_PORT.name());
    String appSubmitTimeStr =
            System.getenv(ApplicationConstants.APP_SUBMIT_TIME_ENV);
    String maxAppAttempts =
            System.getenv(ApplicationConstants.MAX_APP_ATTEMPTS_ENV);
    Path outputPath =
            new Path(System.getenv(Constant.APP_OUTPUT));

    validateInputParam(containerIdStr,
            ApplicationConstants.Environment.CONTAINER_ID.name());
    validateInputParam(nodeHostString, ApplicationConstants.Environment.NM_HOST.name());
    validateInputParam(nodePortString, ApplicationConstants.Environment.NM_PORT.name());
    validateInputParam(nodeHttpPortString,
            ApplicationConstants.Environment.NM_HTTP_PORT.name());
    validateInputParam(appSubmitTimeStr,
            ApplicationConstants.APP_SUBMIT_TIME_ENV);
    validateInputParam(maxAppAttempts,
            ApplicationConstants.MAX_APP_ATTEMPTS_ENV);

    ContainerId containerId = ConverterUtils.toContainerId(containerIdStr);
    ApplicationAttemptId applicationAttemptId =
            containerId.getApplicationAttemptId();
    long appSubmitTime = Long.parseLong(appSubmitTimeStr);

    final ApplicationMasterParameters parameters = new ApplicationMasterParameters(
            applicationAttemptId, containerId,
            outputPath, nodeHostString, Integer.parseInt(nodePortString),
            Integer.parseInt(nodeHttpPortString),
            appSubmitTime,
            Integer.parseInt(maxAppAttempts));
    LOG.info("Master parameters: " + parameters);
    return parameters;
  }

  private static void validateInputParam(String value, String param)
          throws IOException {
    if (value == null) {
      String msg = param + " is null";
      LOG.error(msg);
      throw new IOException(msg);
    }
  }


  public static void main(String[] args) throws Exception {
    try {
      int rc = ToolRunner.run(new YarnConfiguration(), new ApplicationMasterMain(), args);
      System.exit(rc);
    } catch (Exception e) {
      System.err.println(e);
      System.exit(1);
    }
  }
}
