package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import java.io.IOException;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.CamelTestSupport;
import org.apache.camel.util.EndpointHelper;
import org.junit.Before;
import org.junit.Ignore;
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
public class ConsumerTest extends CamelTestSupport {
    @Mock Client client;
    BeanstalkEndpoint endpoint;
    final String testMessage = "hello, world";

    @Test
    public void testReceive() throws Exception {
        final long jobId = 111;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(null)).thenReturn(jobMock);

        final Exchange exchange = endpoint.createPollingConsumer().receive();
        assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
        verify(client).reserve(null);
    }

    @Test
    public void testReceiveEmpty() throws Exception {
        when(client.reserve(null)).thenReturn(null);

        final Exchange exchange = endpoint.createPollingConsumer().receive();
        assertNull(exchange);
        verify(client).reserve(null);
    }

    @Test
    public void testReceiveNoWait() throws Exception {
        final long jobId = 111;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(0)).thenReturn(jobMock);

        final Exchange exchange = endpoint.createPollingConsumer().receiveNoWait();
        assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
        verify(client).reserve(0);
    }

    @Test
    public void testReceiveTimeout() throws Exception {
        final int timeout = 15;
        final long jobId = 111;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(timeout)).thenReturn(jobMock);

        final Exchange exchange = endpoint.createPollingConsumer().receive(timeout);
        assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
        verify(client).reserve(timeout);
    }

    @Test
    @Ignore
    public void testReleaseOnComplete() throws Exception {
        final long jobId = 111;
        final long priority = BeanstalkComponent.DEFAULT_PRIORITY;
        final int delay = BeanstalkComponent.DEFAULT_DELAY;
        final int timeout = 0;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(anyInt())).thenReturn(jobMock);

        final Exchange exchange = endpoint.createPollingConsumer().receive(timeout);
        assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
        verify(client).reserve(timeout);
    }

    @Test
    public void testReceiveThroughEndpoint() throws Exception {
        final long jobId = 111;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(0))
                .thenReturn(jobMock)
                .thenReturn(null);

        EndpointHelper.pollEndpoint(endpoint, new Processor() {
            public void process(Exchange exchange) {
                assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
                assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
            }
        }, 0);

        verify(client, times(2)).reserve(0);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        reset(client);
        super.setUp();
        endpoint = Helper.getEndpoint("beanstalk:tube", context, client);
    }
}
