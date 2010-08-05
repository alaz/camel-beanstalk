package com.osinka.camel.beanstalk;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkComponent extends DefaultComponent {
    public final static long DEFAULT_PRIORITY    = 0;
    public final static int  DEFAULT_DELAY       = 0;
    public final static int  DEFAULT_TIME_TO_RUN = 0;

    private final transient Log log = LogFactory.getLog(BeanstalkComponent.class);

    public BeanstalkComponent() {
    }

    public BeanstalkComponent(CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map parameters) throws IllegalArgumentException {
        return new BeanstalkEndpoint(uri, this, parseUri(remaining));
    }

    final Pattern HostPortTubeRE = Pattern.compile("([\\w.]+):([\\d]+)/(.+)");
    final Pattern HostTubeRE = Pattern.compile("([\\w.]+)/(.+)");
    final Pattern TubeRE = Pattern.compile("([^/?]+)");

    ConnectionSettings parseUri(String remaining) throws IllegalArgumentException {
        Matcher m = HostPortTubeRE.matcher(remaining);
        if (m.matches()) {
            return new ConnectionSettings(m.group(1), Integer.parseInt(m.group(2)), m.group(3));
        }

//        m.reset();
        m.usePattern(HostTubeRE);
        if (m.matches()) {
            return new ConnectionSettings(m.group(1), m.group(2));
        }

//        m.reset();
        m.usePattern(TubeRE);
        if (m.matches()) {
            return new ConnectionSettings(m.group(1));
        }
        throw new IllegalArgumentException(String.format("Invalid path format: %s - should be <hostName>:<port>/<tubeName>", remaining));
    }
}