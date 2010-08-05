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
    public void parseTest() {
        final BeanstalkComponent component = new BeanstalkComponent();
        assertEquals("Full URI", component.parseUri("host.domain.tld:11300/someTube"), new ConnectionSettings("host.domain.tld", 11300, "someTube"));
        assertEquals("No port", component.parseUri("host.domain.tld/someTube"), new ConnectionSettings("host.domain.tld", Client.DEFAULT_PORT, "someTube"));
        assertEquals("Only tube", component.parseUri("someTube"), new ConnectionSettings(Client.DEFAULT_HOST, Client.DEFAULT_PORT, "someTube"));
    }

    @Test
    public void fewTubesTest() {
        final BeanstalkComponent component = new BeanstalkComponent();
        assertArrayEquals("Full URI", component.parseUri("host:90/tube1+tube2").tubes, new String[] {"tube1", "tube2"});
        assertArrayEquals("No port", component.parseUri("host/tube1+tube2").tubes, new String[] {"tube1", "tube2"});
        assertArrayEquals("Only tubes", component.parseUri("tube1+tube2").tubes, new String[] {"tube1", "tube2"});
        assertArrayEquals("Empty URI", component.parseUri("").tubes, new String[0]);
    }

    @Test(expected=IllegalArgumentException.class)
    public void notValidHost() {
        final BeanstalkComponent component = new BeanstalkComponent();
        fail(String.format("Calling on not valid URI must raise exception, but got result %s", component.parseUri("not_valid?host/port")));
    }
}
