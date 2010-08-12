package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;

/**
 * Beanstalk Camel component
 *
 * URI:
 *
 * Parameters:
 * <code>command</code> - one of "put", "release", "bury", "touch", "delete".
 * "put" for Producers by default and is empty for Consumers.
 * <code>priority</code>
 * <code>delay</code>
 * <code>timeToRun</code>
 *
 * @author alaz
 */
public class BeanstalkComponent extends DefaultComponent {
    public static final String DEFAULT_TUBE     = "default";

    public final static String COMMAND_BURY     = "bury";
    public final static String COMMAND_RELEASE  = "release";
    public final static String COMMAND_PUT      = "put";
    public final static String COMMAND_TOUCH    = "touch";
    public final static String COMMAND_DELETE   = "delete";

    public final static long DEFAULT_PRIORITY       = 0; // 0 is highest
    public final static int  DEFAULT_DELAY          = 0;
    public final static int  DEFAULT_TIME_TO_RUN    = 0; // if 0 the daemon sets 1.

    static ConnectionSettingsFactory connFactory = new ConnectionSettingsFactory();

    public BeanstalkComponent() {
    }

    public BeanstalkComponent(final CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(final String uri, final String remaining, final Map<String,Object> parameters) throws IllegalArgumentException {
        return new BeanstalkEndpoint(uri, this, connFactory.parseUri(remaining));
    }

    public static void setConnectionSettingsFactory(ConnectionSettingsFactory connFactory) {
        BeanstalkComponent.connFactory = connFactory;
    }

    @Override
    protected boolean useIntrospectionOnEndpoint() {
        return true;
    }
}