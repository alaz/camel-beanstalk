package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author alaz
 */
public class ConnectionSettingsFactory {
    final Pattern HostPortTubeRE = Pattern.compile("^(([\\w.-]+)(:([\\d]+))?/)?([\\w%+]*)$");

    public ConnectionSettings parseUri(final String remaining) throws IllegalArgumentException {
        final Matcher m = HostPortTubeRE.matcher(remaining);
        if (!m.matches())
            throw new IllegalArgumentException(String.format("Invalid path format: %s - should be [<hostName>[:<port>]/][<tubes>]", remaining));

        final String host = m.group(2) != null ? m.group(2) : Client.DEFAULT_HOST;
        final int port = m.group(4) != null ? Integer.parseInt(m.group(4)) : Client.DEFAULT_PORT;
        final String tubes = m.group(5) != null ? m.group(5) : "";
        return new ConnectionSettings(host, port, tubes);
    }
}
