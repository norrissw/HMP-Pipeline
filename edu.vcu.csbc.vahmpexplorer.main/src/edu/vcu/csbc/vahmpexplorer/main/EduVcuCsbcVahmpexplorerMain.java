/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.vcu.csbc.vahmpexplorer.main;

/**
 *
 * @author snorris
 */
import com.vaadin.Application;
import com.vaadin.data.Property.ValueChangeEvent;
import com.vaadin.data.Property.ValueChangeListener;
import com.vaadin.event.ItemClickEvent;
import com.vaadin.event.ItemClickEvent.ItemClickListener;
import com.vaadin.terminal.ExternalResource;
import com.vaadin.terminal.ThemeResource;
import com.vaadin.terminal.gwt.server.WebApplicationContext;
import com.vaadin.ui.*;
import com.vaadin.ui.Button.ClickEvent;
import com.vaadin.ui.TabSheet.SelectedTabChangeEvent;
import com.vaadin.ui.TabSheet.Tab;
import com.vaadin.ui.Window.CloseEvent;
import com.vaadin.ui.themes.BaseTheme;
import edu.vcu.csbc.vahmpexplorer.auth.ChangePasswordWindow;
import edu.vcu.csbc.vahmpexplorer.auth.LoginInfo;
import edu.vcu.csbc.vahmpexplorer.data.Sample;
import edu.vcu.csbc.vahmpexplorer.db.DBPool;
import edu.vcu.csbc.vahmpexplorer.ui.DataTabSheet;
import edu.vcu.csbc.vahmpexplorer.ui.ListView;
import edu.vcu.csbc.vahmpexplorer.ui.table.RdpTable;
import edu.vcu.csbc.vahmpexplorer.ui.table.RunTable;
import edu.vcu.csbc.vahmpexplorer.ui.table.SampleTable;
import edu.vcu.csbc.vahmpexplorer.auth.LoginWindow;
import edu.vcu.csbc.vahmpexplorer.auth.User;
import edu.vcu.csbc.vahmpexplorer.db.DBHelper;
import edu.vcu.csbc.vahmpexplorer.ui.window.RunViewer;
import edu.vcu.csbc.vahmpexplorer.ui.window.SampleViewer;
import edu.vcu.csbc.vahmpexplorer.ui.window.TabViewer;
import edu.vcu.csbc.vahmpexplorer.util.HelpMessages;
import edu.vcu.csbc.vahmpexplorer.util.MainHelpPopup;
import edu.vcu.csbc.vahmpexplorer.util.TaxaMap;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Set;

public class VaHMPExplorer extends Application implements ItemClickListener,
        TabSheet.SelectedTabChangeListener, Table.ValueChangeListener,
        Button.ClickListener, Window.CloseListener {

    private HorizontalSplitPanel horiztonalSplit = new HorizontalSplitPanel();
    private RunTable runTable = null;
    private ListView listView = null;
    private SampleTable sampleTable = null;
    private DataTabSheet dataTabSheet = null;
    private TaxaMap taxaMap = null;
    private RdpTable rdpTable = null;
    private SampleViewer sampleViewer;
    private TabViewer tabViewer;
    private RunViewer runViewer;
    private LoginWindow login;
    private User user;
    private Button changePassword;
    private Button logout;
    private Window main;
    private boolean showLogin = true;

    @Override
    public void init() {
//        showLogin = false;
        setTheme("vahmpexplorer");
        if (user == null && showLogin) {
            buildLoginLayout();
        } else {
            buildMainLayout();
        }
    }

    private void buildLoginLayout() {
        login = new LoginWindow(this);
        setMainWindow(login);
    }

    public void authenticate(String login, String password) throws Exception {
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        Date date = new Date();
        String loginTime = dateFormat.format(date);
        WebApplicationContext context = (WebApplicationContext) getContext();
        LoginInfo info = new LoginInfo(loginTime, context.getBrowser());

        try {
            user = DBHelper.getInstance().getUser(login, password);
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception(e.getMessage());
        }
        DBHelper.getInstance().recordLogin(user, info);
        buildMainLayout();
    }

    private void buildMainLayout() {
        main = new Window("VaHMP Explorer");
        setMainWindow(main);
        setMainComponent(getListView());
        VerticalLayout v = new VerticalLayout();
        v.addComponent(createToolBar(showLogin));
        v.addComponent(horiztonalSplit);
        v.setSizeFull();

        v.setExpandRatio(horiztonalSplit, 1);
        horiztonalSplit.setSplitPosition(250, HorizontalSplitPanel.UNITS_PIXELS);

        horiztonalSplit.setFirstComponent(getRunViewer());
        getMainWindow().setContent(v);
        getMainWindow().addListener((Window.CloseListener) this);
        if (login != null) {
            getMainWindow().removeWindow(login);

            String newURL = getURL().toString().replace(":8080", "");
            newURL = getURL().toString();
            login.open(new ExternalResource(newURL));
        }

    }

    public Component createToolBar(boolean loggedIn) {
        HorizontalLayout h = new HorizontalLayout();
        h.setMargin(true);
        h.setWidth("100%");

        Embedded headerImg = new Embedded(null, new ThemeResource("../vahmpexplorer/img/header.png"));
        headerImg.setWidth(311, Embedded.UNITS_PIXELS);
        headerImg.setHeight(45, Embedded.UNITS_PIXELS);
        headerImg.setType(Embedded.TYPE_IMAGE);
        headerImg.setStyleName(BaseTheme.BUTTON_LINK);
        headerImg.setDescription("Version " + HelpMessages.VERSION);
        h.addComponent(headerImg);

        if (loggedIn) {
            Panel panel = new Panel();
            Label loggedInUser = new Label("Welcome: " + user.getFirstName() + " " + user.getLastName() + " (" + user.getLogin() + ")");
            changePassword = new Button("Change Password");
            changePassword.setStyleName(BaseTheme.BUTTON_LINK);
            changePassword.addListener((Button.ClickListener) this);

            logout = new Button("Logout");
            logout.setStyleName(BaseTheme.BUTTON_LINK);
            logout.addListener((Button.ClickListener) this);

            HorizontalLayout hl = new HorizontalLayout();
            hl.setSpacing(true);
            hl.addComponent(changePassword);
            hl.addComponent(logout);

            panel.addComponent(loggedInUser);
            panel.addComponent(hl);
            h.addComponent(panel);
            h.setComponentAlignment(panel, Alignment.MIDDLE_RIGHT);
        }
        PopupView help = new PopupView(new MainHelpPopup());
        h.addComponent(help);
        h.setComponentAlignment(help, Alignment.MIDDLE_RIGHT);
        h.setComponentAlignment(headerImg, Alignment.MIDDLE_LEFT);
        return h;
    }

    @Override
    public void itemClick(ItemClickEvent event) {
        if (event.getSource() == runTable) {
        }
    }

    public void setMainComponent(Component c) {
        horiztonalSplit.setSecondComponent(c);
    }

    public ListView getListView() {
        if (listView == null) {
            listView = new ListView(getSampleViewer(), getTabViewer(), this);
        }
        return listView;
    }

    public SampleTable getSampleTable() {
        if (sampleTable == null) {
            sampleTable = new SampleTable(this);
            sampleTable.addListener(new ValueChangeListener() {

                @Override
                public void valueChange(ValueChangeEvent event) {
                    sampleTable.requestRepaint();
                }
            });
        }
        return sampleTable;
    }

    public DataTabSheet getDataTabSheet() {
        if (dataTabSheet == null) {
            dataTabSheet = new DataTabSheet(this);
        }
        return dataTabSheet;
    }

    @Override
    public void selectedTabChange(SelectedTabChangeEvent event) {
        TabSheet tabSheet = event.getTabSheet();
        Tab tab = tabSheet.getTab(tabSheet.getSelectedTab());
        if (tab != null) {
//            getMainWindow().showNotification("Selected " + tab.getCaption() + " tab");
        }
    }

    @Override
    public void valueChange(ValueChangeEvent event) {
        Set<?> value = (Set<?>) event.getProperty().getValue();
        if (null == value || value.isEmpty()) {
        } else {
            getDataTabSheet().updateSelectedLabels((Set<Sample>) getSampleTable().getValue());
        }
    }

    @Override
    public void buttonClick(ClickEvent event) {
        Button button = event.getButton();
        if (button.equals(getDataTabSheet().getShowRdp())) {
            getDataTabSheet().createNewRdpTab(getTaxaMap(), getSampleTable().getValue());
        } else if (button.equals(changePassword)) {
            ChangePasswordWindow w = new ChangePasswordWindow(this);
            w.show();
        } else if (button.equals(logout)) {
            logout();
        }
    }

    @Override
    public void windowClose(CloseEvent e) {
        DBPool.getInstance().closeAll();
        getMainWindow().getApplication().close();
    }

    public TaxaMap getTaxaMap() {
        if (taxaMap == null) {
            taxaMap = new TaxaMap();
        }
        return taxaMap;
    }

    private RunTable getRunTable() {
        if (runTable == null) {
            runTable = new RunTable(this);
            runTable.addListener(new Table.ValueChangeListener() {

                @Override
                public void valueChange(ValueChangeEvent event) {
                    getSampleViewer().addSamplesForRuns(runTable.getValue());
                    runTable.requestRepaint();
                }
            });
        }
        return runTable;
    }

    private SampleViewer getSampleViewer() {
        if (sampleViewer == null) {
            sampleViewer = new SampleViewer(this, getSampleTable());
        }
        return sampleViewer;
    }

    private TabViewer getTabViewer() {
        if (tabViewer == null) {
            tabViewer = new TabViewer(this, getDataTabSheet());
        }
        return tabViewer;
    }

    private RunViewer getRunViewer() {
        if (runViewer == null) {
            runViewer = new RunViewer(this, getRunTable());
        }
        return runViewer;
    }


    public User getLoggedInUser() {
        return user;
    }

    private void logout() {
        DBPool.getInstance().closeAll();
        setMainWindow(login);
        main.open(new ExternalResource(getURL()));
    }
}