package octo47.yarn.master;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

/**
 * @author Andrey Stepachev
 */
public interface AppContext {
  public ApplicationId getApplicationId();

  public ApplicationAttemptId getAttemptId();

  public String getUser();

  public long getStartTime();
}
