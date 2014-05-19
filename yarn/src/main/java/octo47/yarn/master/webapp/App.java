package octo47.yarn.master.webapp;

import com.google.inject.Inject;
import com.google.inject.servlet.RequestScoped;
import octo47.yarn.master.AppContext;

/**
 * @author Andrey Stepachev
 */
@RequestScoped
public class App {

  private final AppContext context;

  @Inject
  public App(AppContext context) {
    this.context = context;
  }

  public AppContext getContext() {
    return context;
  }
}
