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
public class BuryProducerTest extends BeanstalkCamelTestSupport {
    final String tubeName = "buryTest";

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Ignore("requires reserve - bury sequence")
    @Test
    public void testBury() throws InterruptedException, IOException {
        long jobId = beanstalk.put(0, 0, 5, new byte[0]);
        assertTrue("Valid Job Id", jobId > 0);

        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isNotNull();
        resultEndpoint.allMessages().header(Headers.RESULT).isEqualTo(true);
        template.sendBodyAndHeader(null, Headers.JOB_ID, jobId);

        assertMockEndpointsSatisfied();

        final Long messageJobId = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in message", messageJobId);
        assertEquals("Message Job ID equals", jobId, messageJobId.longValue());

        final Job job = beanstalk.reserve(0);
        assertNull("Beanstalk client has no message", job);

        final Job buried = beanstalk.peekBuried();
        assertNotNull("Job in buried", buried);
        assertEquals("Buried job id", jobId, buried.getJobId());
    }

    @Test(expected=CamelExecutionException.class)
    public void testNoJobId() throws InterruptedException, IOException {
        resultEndpoint.expectedMessageCount(0);
        template.sendBody(new byte[0]);

        resultEndpoint.assertIsSatisfied();
        assertListSize("Number of exceptions", resultEndpoint.getFailures(), 1);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").to("beanstalk:"+tubeName+"?command=bury").to("mock:result");
            }
        };
    }

    @Override
    protected String getTubeName() {
        return tubeName;
    }
}
