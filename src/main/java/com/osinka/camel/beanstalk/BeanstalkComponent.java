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
    public final static String COMMAND_BURY     = "bury";
    public final static String COMMAND_RELEASE  = "release";
    public final static String COMMAND_PUT      = "put";
    public final static String COMMAND_TOUCH    = "touch";
    public final static String COMMAND_DELETE   = "delete";

    public final static long DEFAULT_PRIORITY       = 0;
    public final static int  DEFAULT_DELAY          = 0;
    public final static int  DEFAULT_TIME_TO_RUN    = 0;

    public BeanstalkComponent() {
    }

    public BeanstalkComponent(CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map parameters) throws IllegalArgumentException {
        return new BeanstalkEndpoint(uri, this, parseUri(remaining));
    }

    final Pattern HostPortTubeRE = Pattern.compile("(([\\w.]+)(:([\\d]+))?/)?(.*)");

    ConnectionSettings parseUri(String remaining) throws IllegalArgumentException {
        Matcher m = HostPortTubeRE.matcher(remaining);
        if (!m.matches())
            throw new IllegalArgumentException(String.format("Invalid path format: %s - should be [<hostName>[:<port>]/][<tubes>]", remaining));

        String host = m.group(2) != null ? m.group(2) : Client.DEFAULT_HOST;
        int port = m.group(4) != null ? Integer.parseInt(m.group(4)) : Client.DEFAULT_PORT;
        String tubes = m.group(5) != null ? m.group(5) : "";
        return new ConnectionSettings(host, port, tubes);
    }
}