package com.osinka.camel.beanstalk;

import org.junit.Test;
import static org.junit.Assert.*;

import com.surftools.BeanstalkClient.Client;

/**
 *
 * @author alaz
 */
public class ComponentTest {
    @Test
    public void parseUriTest() {
        final BeanstalkComponent component = getComponent();
        assertEquals("Full URI", new ConnectionSettings("host.domain.tld", 11300, "someTube"), component.parseUri("host.domain.tld:11300/someTube"));
        assertEquals("No port", new ConnectionSettings("host.domain.tld", Client.DEFAULT_PORT, "someTube"), component.parseUri("host.domain.tld/someTube"));
        assertEquals("Only tube", new ConnectionSettings(Client.DEFAULT_HOST, Client.DEFAULT_PORT, "someTube"), component.parseUri("someTube"));
    }

    @Test
    public void parseTubesTest() {
        final BeanstalkComponent component = getComponent();
        assertArrayEquals("Full URI", new String[] {"tube1", "tube2"}, component.parseUri("host:90/tube1+tube2").tubes);
        assertArrayEquals("No port", new String[] {"tube1", "tube2"}, component.parseUri("host/tube1+tube2").tubes);
        assertArrayEquals("Only tubes", new String[] {"tube1", "tube2"}, component.parseUri("tube1+tube2").tubes);
        assertArrayEquals("Empty URI", new String[0], component.parseUri("").tubes);
    }

    @Test(expected=IllegalArgumentException.class)
    public void notValidHost() {
        final BeanstalkComponent component = getComponent();
        fail(String.format("Calling on not valid URI must raise exception, but got result %s", component.parseUri("not_valid?host/tube?")));
    }

    BeanstalkComponent getComponent() {
        return new BeanstalkComponent();
    }

}
