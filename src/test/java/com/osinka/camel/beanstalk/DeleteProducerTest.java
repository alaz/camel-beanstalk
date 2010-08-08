package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Job;
import java.io.IOException;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author alaz
 */
public class DeleteProducerTest extends BeanstalkCamelTestSupport {
    final String tubeName = "deleteTest";

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate direct;

    @Test
    public void testDelete() throws InterruptedException, IOException {
        long jobId = beanstalk.put(0, 0, 5, new byte[0]);
        assertTrue("Valid Job Id", jobId > 0);

        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isNotNull();
        resultEndpoint.allMessages().header(Headers.RESULT).isEqualTo(true);
        direct.sendBodyAndHeader(null, Headers.JOB_ID, jobId);

        assertMockEndpointsSatisfied();

        final Long messageJobId = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in message", messageJobId);
        assertEquals("Message Job ID equals", jobId, messageJobId.longValue());

        final Job job = beanstalk.peek(jobId);
        assertNull("Job has been deleted", job);
    }

    @Test(expected=CamelExecutionException.class)
    public void testNoJobId() throws InterruptedException, IOException {
        resultEndpoint.expectedMessageCount(0);
        direct.sendBody(new byte[0]);

        resultEndpoint.assertIsSatisfied();
        assertListSize("Number of exceptions", resultEndpoint.getFailures(), 1);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").to("beanstalk:"+tubeName+"?command=delete").to("mock:result");
            }
        };
    }

    @Override
    protected String getTubeName() {
        return tubeName;
    }
}
