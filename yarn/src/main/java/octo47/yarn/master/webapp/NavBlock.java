package octo47.yarn.master.webapp;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import javax.inject.Inject;

/**
 * @author Andrey Stepachev
 */
public class NavBlock extends HtmlBlock implements AMParams {
  final App app;

  @Inject
  NavBlock(App app) {
    this.app = app;
  }

  @Override
  protected void render(Block html) {
    String rmweb = $(RM_WEB);
    Hamlet.DIV<Hamlet> nav = html.
            div("#nav").
            h3("Cluster").
            ul().
            li().a(url(rmweb, "cluster", "cluster"), "About")._().
            li().a(url(rmweb, "cluster", "apps"), "Applications")._().
            li().a(url(rmweb, "cluster", "scheduler"), "Scheduler")._()._().
            h3("Application").
            ul().
            li().a(url("app/info"), "About")._().
            li().a(url("app"), "Tasks")._()._();
    nav.
            h3("Tools").
            ul().
            li().a("/conf", "Configuration")._().
            li().a("/logs", "Local logs")._().
            li().a("/stacks", "Server stacks")._().
            li().a("/metrics", "Server metrics")._()._()._();
  }
}

