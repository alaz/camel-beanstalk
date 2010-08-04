package com.osinka.camel.beanstalk;

import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultComponent;
import com.surftools.BeanstalkClientImpl.ClientImpl;

/**
 *
 * @author alaz
 */
public class BeanstalkComponent extends DefaultComponent {
    public BeanstalkComponent() {
    }

    public BeanstalkComponent(CamelContext context) {
        super(context);
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map parameters) throws IllegalArgumentException {
        ConnectionSettings conn = parseUri(remaining);

        ClientImpl beanstalk = new ClientImpl(conn.host, conn.port);
        beanstalk.useTube(conn.tube);

        return new BeanstalkEndpoint(uri, this, beanstalk);
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