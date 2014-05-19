package octo47.yarn.master;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;

import javax.annotation.Nullable;

/**
 * @author Andrey Stepachev
 */
public class ApplicationMasterParameters {

  private final ApplicationAttemptId applicationAttemptId;
  private final ContainerId containerId;
  private final Path output;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;
  private final long appSubmitTime;
  private final int maxAppAttempts;

  public ApplicationMasterParameters(ApplicationAttemptId applicationAttemptId,
                                     ContainerId containerId,
                                     @Nullable Path output, String nmHost,
                                     int nmPort, int nmHttpPort,
                                     long appSubmitTime, int maxAppAttempts) {
    this.applicationAttemptId = applicationAttemptId;
    this.containerId = containerId;
    this.output = output;
    this.nmHost = nmHost;
    this.nmPort = nmPort;
    this.nmHttpPort = nmHttpPort;
    this.appSubmitTime = appSubmitTime;
    this.maxAppAttempts = maxAppAttempts;
  }

  public ApplicationAttemptId getApplicationAttemptId() {
    return applicationAttemptId;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public String getNmHost() {
    return nmHost;
  }

  public int getNmPort() {
    return nmPort;
  }

  public int getNmHttpPort() {
    return nmHttpPort;
  }

  public long getAppSubmitTime() {
    return appSubmitTime;
  }

  public int getMaxAppAttempts() {
    return maxAppAttempts;
  }

  public boolean hasOutput() {
    return output != null;
  }

  @Nullable
  public Path getOutput() {
    return output;
  }

  public String getTrackingUrl() {
    return "http://" + getNmHost() + ":" + getNmHttpPort();
  }

  @Override
  public String toString() {
    return "ApplicationMasterParameters{" +
            "applicationAttemptId=" + applicationAttemptId +
            ", containerId=" + containerId +
            ", output=" + output +
            ", nmHost='" + nmHost + '\'' +
            ", nmPort=" + nmPort +
            ", nmHttpPort=" + nmHttpPort +
            ", appSubmitTime=" + appSubmitTime +
            ", maxAppAttempts=" + maxAppAttempts +
            '}';
  }
}

