package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Job;
import java.io.IOException;
import org.apache.camel.EndpointInject;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Produce;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.Processor;
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
    protected ProducerTemplate direct;

    @Test
    public void testPut() throws InterruptedException, IOException {
        final byte[] testBytes = Helper.stringToBytes(testMessage);

        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isNotNull();
        resultEndpoint.expectedBodiesReceived(testBytes);
        direct.sendBody(testBytes);

        resultEndpoint.assertIsSatisfied();

        final Long jobId = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in 'In' message", jobId);

        final Job job = beanstalk.reserve(1);
        assertNotNull("Beanstalk client got message", job);
        assertArrayEquals("Job body from the server", testBytes, job.getData());
        assertEquals("Job ID from the server", jobId.longValue(), job.getJobId());
        beanstalk.delete(jobId.longValue());
    }

    @Test
    public void testOut() throws InterruptedException, IOException {
        final byte[] testBytes = Helper.stringToBytes(testMessage);

        final Endpoint endpoint = context.getEndpoint("beanstalk:"+tubeName);
        final Exchange exchange = template.send(endpoint, ExchangePattern.InOut, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setBody(testBytes);
            }
        });

        final Message out = exchange.getOut();
        assertNotNull("Out message", out);

        final Long jobId = out.getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in 'Out' message", jobId);

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
        direct.sendBodyAndHeader(testBytes, Headers.DELAY, 10);

        resultEndpoint.assertIsSatisfied();

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
