package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Job;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author alaz
 */
public class PutProducerTest extends BeanstalkCamelTestSupport {
    final String tubeName = "putTest";
    final String testMessage = "Hello, world!";

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Test
    public void testPut() throws InterruptedException, IOException {
        final byte[] testBytes = Helper.stringToBytes(testMessage);

        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isNotNull();
        resultEndpoint.expectedBodiesReceived(testBytes);
        template.sendBody(testBytes);

        assertMockEndpointsSatisfied();

        final Long jobId = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in message", jobId);

        final Job job = beanstalk.reserve(1);
        assertNotNull("Beanstalk client got message", job);
        assertArrayEquals("Job body from the server", testBytes, job.getData());
        assertEquals("Job ID from the server", jobId.longValue(), job.getJobId());
        beanstalk.delete(jobId.longValue());
    }

    @Test
    public void testDelay() throws InterruptedException, IOException {
        final byte[] testBytes = new byte[0];

        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isNotNull();
        resultEndpoint.expectedBodiesReceived(testBytes);
        template.sendBodyAndHeader(testBytes, Headers.DELAY, 10);

        assertMockEndpointsSatisfied();

        final Long jobId = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in message", jobId);

        final Job job = beanstalk.reserve(0);
        assertNull("Beanstalk client has no message", job);
        beanstalk.delete(jobId.longValue());
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").to("beanstalk:"+tubeName+"?jobPriority=1000&jobTimeToRun=5").to("mock:result");
            }
        };
    }

    @Override
    protected String getTubeName() {
        return tubeName;
    }
}
