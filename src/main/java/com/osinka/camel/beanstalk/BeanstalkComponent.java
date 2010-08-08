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

    public BeanstalkComponent() {
    }

    public BeanstalkComponent(final CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(final String uri, final String remaining, final Map<String,Object> parameters) throws IllegalArgumentException {
        return new BeanstalkEndpoint(uri, this, parseUri(remaining));
    }

    final Pattern HostPortTubeRE = Pattern.compile("^(([\\w.-]+)(:([\\d]+))?/)?([\\w%+]*)$");

    ConnectionSettings parseUri(final String remaining) throws IllegalArgumentException {
        final Matcher m = HostPortTubeRE.matcher(remaining);
        if (!m.matches())
            throw new IllegalArgumentException(String.format("Invalid path format: %s - should be [<hostName>[:<port>]/][<tubes>]", remaining));

        final String host = m.group(2) != null ? m.group(2) : Client.DEFAULT_HOST;
        final int port = m.group(4) != null ? Integer.parseInt(m.group(4)) : Client.DEFAULT_PORT;
        final String tubes = m.group(5) != null ? m.group(5) : "";
        return new ConnectionSettings(host, port, tubes);
    }

    @Override
    protected boolean useIntrospectionOnEndpoint() {
        return true;
    }
}