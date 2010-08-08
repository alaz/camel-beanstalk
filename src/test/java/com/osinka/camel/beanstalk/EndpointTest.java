package com.osinka.camel.beanstalk;

import org.apache.camel.CamelContext;
import org.apache.camel.FailedToCreateProducerException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

/**
 *
 * @author alaz
 */
public class EndpointTest {
    CamelContext context = null;

    @Before
    public void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.disableJMX();
        context.start();
    }

    @Test
    public void testPriority() {
        BeanstalkEndpoint endpoint = context.getEndpoint("beanstalk:default?jobPriority=1000", BeanstalkEndpoint.class);
        assertNotNull("Beanstalk endpoint", endpoint);
        assertEquals("Priority", 1000, endpoint.getJobPriority());
    }

    @Test
    public void testTimeToRun() {
        BeanstalkEndpoint endpoint = context.getEndpoint("beanstalk:default?jobTimeToRun=10", BeanstalkEndpoint.class);
        assertNotNull("Beanstalk endpoint", endpoint);
        assertEquals("Time to run", 10, endpoint.getJobTimeToRun());
    }

    @Test
    public void testDelay() {
        BeanstalkEndpoint endpoint = context.getEndpoint("beanstalk:default?jobDelay=10", BeanstalkEndpoint.class);
        assertNotNull("Beanstalk endpoint", endpoint);
        assertEquals("Delay", 10, endpoint.getJobDelay());
    }

    @Test
    public void testCommand() {
        BeanstalkEndpoint endpoint = context.getEndpoint("beanstalk:default?command=release", BeanstalkEndpoint.class);
        assertNotNull("Beanstalk endpoint", endpoint);
        assertEquals("Command", "release", endpoint.command);
    }

    @Test(expected=FailedToCreateProducerException.class)
    public void testWrongCommand() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").to("beanstalk:default?command=noCommand");
            }
        });
    }

    @After
    public void tearDown() throws Exception {
        context.stop();
    }
}
