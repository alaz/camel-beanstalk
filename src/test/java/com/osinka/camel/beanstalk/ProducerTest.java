package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.Produce;
import org.apache.camel.Producer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;
import static org.mockito.Mockito.*;

/**
 *
 * @author alaz
 */
public class ProducerTest extends CamelTestSupport {
    @Mock Client client;
    final String testMessage = "hello, world";

    @EndpointInject(uri = "beanstalk:tube")
    protected BeanstalkEndpoint endpoint;

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate direct;

    @Test
    public void testPut() throws Exception {
        final long priority = BeanstalkComponent.DEFAULT_PRIORITY;
        final int delay = BeanstalkComponent.DEFAULT_DELAY;
        final int timeToRun = BeanstalkComponent.DEFAULT_TIME_TO_RUN;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final long jobId = 111;

        when(client.put(priority, delay, timeToRun, payload)).thenReturn(jobId);

        final Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(PutProducer.class));

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() { // TODO: SetBodyProcessor(?)
            public void process(Exchange exchange) {
                exchange.getIn().setBody(testMessage);
            }
        });

        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).put(priority, delay, timeToRun, payload);
    }

    @Test
    public void testPutOut() throws Exception {
        final long priority = BeanstalkComponent.DEFAULT_PRIORITY;
        final int delay = BeanstalkComponent.DEFAULT_DELAY;
        final int timeToRun = BeanstalkComponent.DEFAULT_TIME_TO_RUN;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final long jobId = 111;

        when(client.put(priority, delay, timeToRun, payload)).thenReturn(jobId);

        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(PutProducer.class));

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOut, new Processor() { // TODO: SetBodyProcessor(?)
            public void process(Exchange exchange) {
                exchange.getIn().setBody(testMessage);
            }
        });

        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getOut().getHeader(Headers.JOB_ID, Long.class));
        verify(client).put(priority, delay, timeToRun, payload);
    }

    @Test
    public void testPutWithHeaders() throws Exception {
        final long priority = 111;
        final int delay = 5;
        final int timeToRun = 65;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final long jobId = 111;

        when(client.put(priority, delay, timeToRun, payload)).thenReturn(jobId);

        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(PutProducer.class));

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() { // TODO: SetBodyProcessor(?)
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.PRIORITY, priority);
                exchange.getIn().setHeader(Headers.DELAY, delay);
                exchange.getIn().setHeader(Headers.TIME_TO_RUN, timeToRun);
                exchange.getIn().setBody(testMessage);
            }
        });

        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).put(priority, delay, timeToRun, payload);
    }

    @Test
    public void testBury() throws Exception {
        final long priority = BeanstalkComponent.DEFAULT_PRIORITY;
        final long jobId = 111;

        endpoint.setCommand(BeanstalkComponent.COMMAND_BURY);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(BuryProducer.class));

        when(client.bury(jobId, priority)).thenReturn(true);

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.JOB_ID, jobId);
            }
        });

        assertEquals("Op result", Boolean.TRUE, exchange.getIn().getHeader(Headers.RESULT, Boolean.class));
        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).bury(jobId, priority);
    }

    @Test(expected=CamelExecutionException.class)
    public void testBuryNoJobId() throws Exception {
        endpoint.setCommand(BeanstalkComponent.COMMAND_BURY);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(BuryProducer.class));

        template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {}
        });

        verify(client, never()).bury(anyLong(), anyLong());
    }

    @Test
    public void testBuryWithHeaders() throws Exception {
        final long priority = 1000;
        final long jobId = 111;

        endpoint.setCommand(BeanstalkComponent.COMMAND_BURY);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(BuryProducer.class));

        when(client.bury(jobId, priority)).thenReturn(true);

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.PRIORITY, priority);
                exchange.getIn().setHeader(Headers.JOB_ID, jobId);
            }
        });

        assertEquals("Op result", Boolean.TRUE, exchange.getIn().getHeader(Headers.RESULT, Boolean.class));
        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).bury(jobId, priority);
    }

    @Test
    public void testDelete() throws Exception {
        final long jobId = 111;

        endpoint.setCommand(BeanstalkComponent.COMMAND_DELETE);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(DeleteProducer.class));

        when(client.delete(jobId)).thenReturn(true);

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.JOB_ID, jobId);
            }
        });

        assertEquals("Op result", Boolean.TRUE, exchange.getIn().getHeader(Headers.RESULT, Boolean.class));
        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).delete(jobId);
    }

    @Test(expected=CamelExecutionException.class)
    public void testDeleteNoJobId() throws Exception {
        endpoint.setCommand(BeanstalkComponent.COMMAND_DELETE);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(DeleteProducer.class));

        template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {}
        });

        verify(client, never()).delete(anyLong());
    }

    @Test
    public void testRelease() throws Exception {
        final long priority = BeanstalkComponent.DEFAULT_PRIORITY;
        final int delay = BeanstalkComponent.DEFAULT_DELAY;
        final long jobId = 111;

        endpoint.setCommand(BeanstalkComponent.COMMAND_RELEASE);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(ReleaseProducer.class));

        when(client.release(jobId, priority, delay)).thenReturn(true);

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.JOB_ID, jobId);
            }
        });

        assertEquals("Op result", Boolean.TRUE, exchange.getIn().getHeader(Headers.RESULT, Boolean.class));
        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).release(jobId, priority, delay);
    }

    @Test(expected=CamelExecutionException.class)
    public void testReleaseNoJobId() throws Exception {
        endpoint.setCommand(BeanstalkComponent.COMMAND_RELEASE);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(ReleaseProducer.class));

        template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {}
        });

        verify(client, never()).release(anyLong(), anyLong(), anyInt());
    }

    @Test
    public void testReleaseWithHeaders() throws Exception {
        final long priority = 1001;
        final int delay = 124;
        final long jobId = 111;

        endpoint.setCommand(BeanstalkComponent.COMMAND_RELEASE);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(ReleaseProducer.class));

        when(client.release(jobId, priority, delay)).thenReturn(true);

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.JOB_ID, jobId);
                exchange.getIn().setHeader(Headers.PRIORITY, priority);
                exchange.getIn().setHeader(Headers.DELAY, delay);
            }
        });

        assertEquals("Op result", Boolean.TRUE, exchange.getIn().getHeader(Headers.RESULT, Boolean.class));
        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).release(jobId, priority, delay);
    }

    @Test
    public void testTouch() throws Exception {
        final long jobId = 111;

        endpoint.setCommand(BeanstalkComponent.COMMAND_TOUCH);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(TouchProducer.class));

        when(client.touch(jobId)).thenReturn(true);

        final Exchange exchange = template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setHeader(Headers.JOB_ID, jobId);
            }
        });

        assertEquals("Op result", Boolean.TRUE, exchange.getIn().getHeader(Headers.RESULT, Boolean.class));
        assertEquals("Job ID in exchange", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        verify(client).touch(jobId);
    }

    @Test(expected=CamelExecutionException.class)
    public void testTouchNoJobId() throws Exception {
        endpoint.setCommand(BeanstalkComponent.COMMAND_TOUCH);
        Producer producer = endpoint.createProducer();
        assertNotNull("Producer", producer);
        assertThat("Producer class", producer, instanceOf(TouchProducer.class));

        template.send(endpoint, ExchangePattern.InOnly, new Processor() {
            public void process(Exchange exchange) {}
        });

        verify(client, never()).touch(anyLong());
    }

    @Test
    public void testHeaderOverride() throws Exception {
        final long priority = 1020;
        final int delay = 50;
        final int timeToRun = 75;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final long jobId = 113;

        when(client.put(priority, delay, timeToRun, payload)).thenReturn(jobId);

        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().body().isEqualTo(testMessage);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isEqualTo(Long.valueOf(jobId));

        direct.sendBodyAndHeader(testMessage, Headers.TIME_TO_RUN, timeToRun);
        resultEndpoint.assertIsSatisfied();

        final Long jobIdIn = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in 'In' message", jobIdIn);

        verify(client).put(priority, delay, timeToRun, payload);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").to("beanstalk:tube?jobPriority=1020&jobDelay=50&jobTimeToRun=65").to("mock:result");
            }
        };
    }

    @Before
    @Override
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        reset(client);
	Helper.mockComponent(client);
	super.setUp();
    }
}
