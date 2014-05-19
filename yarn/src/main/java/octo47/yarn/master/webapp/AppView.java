package octo47.yarn.master.webapp;

import org.apache.hadoop.yarn.webapp.SubView;
import org.apache.hadoop.yarn.webapp.view.TwoColumnLayout;
import octo47.yarn.master.AppContext;

import javax.inject.Inject;

/**
 * @author Andrey Stepachev
 */
public class AppView extends TwoColumnLayout {

  private AppContext context;

  @Inject
  public AppView(AppContext context) {
    this.context = context;
  }

  protected void commonPreHead(Page.HTML<_> html) {
  }

  @Override
  protected Class<? extends SubView> nav() {
    return NavBlock.class;
  }
}
