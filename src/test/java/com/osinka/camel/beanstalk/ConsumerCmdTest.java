package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import java.util.concurrent.TimeUnit;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.CamelTestSupport;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

/**
 *
 * @author alaz
 */
public class ConsumerCmdTest extends CamelTestSupport {
    final String testMessage = "hello, world";

    @Mock
    Client client;

    @EndpointInject(uri = "mock:result")
    MockEndpoint result;

    boolean shouldIdie = false;

    @Test
    public void testDeleteOnComplete() throws Exception {
        final long jobId = 111;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(anyInt()))
                .thenReturn(jobMock)
                .thenReturn(null);

        result.expectedMinimumMessageCount(1);
        result.expectedBodiesReceived(testMessage);
        result.message(0).header(Headers.JOB_ID).isEqualTo(Long.valueOf(jobId));
        result.assertIsSatisfied();

        verify(client, atLeastOnce()).reserve(anyInt());
        verify(client).delete(jobId);
    }

    @Test
    public void testReleaseOnFailure() throws Exception {
        shouldIdie = true;
        final long jobId = 111;
        final long priority = BeanstalkComponent.DEFAULT_PRIORITY;
        final int delay = BeanstalkComponent.DEFAULT_DELAY;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(anyInt()))
                .thenReturn(jobMock)
                .thenReturn(null);

        result.expectedMinimumMessageCount(1);
        result.await(1, TimeUnit.SECONDS);
        result.assertIsNotSatisfied();

        verify(client, atLeastOnce()).reserve(anyInt());
        verify(client).release(jobId, priority, delay);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("beanstalk:tube?onFailure=release").process(new Processor() {
                    @Override
                    public void process(Exchange exchange) throws InterruptedException {
                        if (shouldIdie) throw new InterruptedException("die");
                    }
                }).to("mock:result");
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
