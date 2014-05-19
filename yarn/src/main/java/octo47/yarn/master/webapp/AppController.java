package octo47.yarn.master.webapp;

import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.util.Times;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import octo47.yarn.master.AppContext;

import static org.apache.hadoop.yarn.util.StringHelper.join;

/**
 * @author Andrey Stepachev
 */
public class AppController extends Controller implements AMParams {

  private static final Log LOG = LogFactory.getLog(AppController.class);
  private final App app;

  @Inject
  public AppController(App app, Configuration conf, RequestContext ctx) {
    super(ctx);
    this.app = app;
    set(APP_ID, app.getContext().getApplicationId().toString());
    set(RM_WEB, WebAppUtils.getResolvedRMWebAppURLWithScheme(conf));
  }

  @Override
  public void index() {
    setTitle(join("Yarn Template Application ", $(APP_ID)));
  }

  /**
   * Render the /info page with an overview of current application.
   */
  public void info() {
    final AppContext context = app.getContext();
    info("Application Master Overview").
            _("Application ID:", context.getApplicationId()).
            _("User:", context.getUser()).
            _("Started on:", Times.format(context.getStartTime())).
            _("Elasped: ", org.apache.hadoop.util.StringUtils.formatTime(
                    Times.elapsed(context.getStartTime(), 0)));
    render(InfoPage.class);
  }

}
