package octo47.yarn.master.webapp;

import org.apache.hadoop.yarn.MockApps;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import octo47.yarn.master.AppContext;

/**
 * @author Andrey Stepachev
 */
public class MockAppContext implements AppContext {
  private final ApplicationId applicationId;
  private final ApplicationAttemptId attemptId;
  private final long startTime = System.currentTimeMillis();

  public MockAppContext(int appid) {
    applicationId = MockApps.newAppID(appid);
    attemptId = ApplicationAttemptId.newInstance(applicationId, 0);
  }

  @Override
  public ApplicationId getApplicationId() {
    return applicationId;
  }

  @Override
  public ApplicationAttemptId getAttemptId() {
    return attemptId;
  }

  @Override
  public String getUser() {
    return System.getProperty("user.name");
  }

  public long getStartTime() {
    return startTime;
  }
}
