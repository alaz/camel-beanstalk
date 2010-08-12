package com.osinka.camel.beanstalk;

import org.junit.Test;
import static org.junit.Assert.*;

import com.surftools.BeanstalkClient.Client;

/**
 *
 * @author alaz
 */
public class ConnectionSettingsTest {
    final ConnectionSettingsFactory factory = BeanstalkComponent.connFactory;

    @Test
    public void parseUriTest() {
        assertEquals("Full URI", new ConnectionSettings("host.domain.tld", 11300, "someTube"), factory.parseUri("host.domain.tld:11300/someTube"));
        assertEquals("No port", new ConnectionSettings("host.domain.tld", Client.DEFAULT_PORT, "someTube"), factory.parseUri("host.domain.tld/someTube"));
        assertEquals("Only tube", new ConnectionSettings(Client.DEFAULT_HOST, Client.DEFAULT_PORT, "someTube"), factory.parseUri("someTube"));
    }

    @Test
    public void parseTubesTest() {
        assertArrayEquals("Full URI", new String[] {"tube1", "tube2"}, factory.parseUri("host:90/tube1+tube2").tubes);
        assertArrayEquals("No port", new String[] {"tube1", "tube2"}, factory.parseUri("host/tube1+tube2").tubes);
        assertArrayEquals("Only tubes", new String[] {"tube1", "tube2"}, factory.parseUri("tube1+tube2").tubes);
        assertArrayEquals("Empty URI", new String[0], factory.parseUri("").tubes);
    }

    @Test(expected=IllegalArgumentException.class)
    public void notValidHost() {
        fail(String.format("Calling on not valid URI must raise exception, but got result %s", factory.parseUri("not_valid?host/tube?")));
    }
}
