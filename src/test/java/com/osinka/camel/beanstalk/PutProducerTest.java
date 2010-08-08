package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.camel.EndpointInject;
import org.apache.camel.Produce;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.camel.test.CamelTestSupport;
import org.junit.After;

/**
 *
 * @author alaz
 */
public class PutProducerTest extends CamelTestSupport {
    final String tubeName = "putTest";

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate template;

    @Test
    public void testEndpoint() {
        BeanstalkEndpoint endpoint = context.getEndpoint("beanstalk:"+tubeName+"?priority=1000&timeToRun=5&delay=1", BeanstalkEndpoint.class);
        assertNotNull("Beanstalk endpoint", endpoint);
        assertEquals("Priority", 1000, endpoint.getPriority());
//        assertEquals("Delay", 1, endpoint.getDelay());
        assertEquals("Time to run", 5, endpoint.getTimeToRun());
    }

    @Test
    public void testProducer() throws InterruptedException, IOException {
        ByteArrayOutputStream byteOS = new ByteArrayOutputStream();
        DataOutputStream dataStream = new DataOutputStream(byteOS);

        String testMessage = "Hello, world!";
        dataStream.writeUTF(testMessage);
        byte[] testBytes = byteOS.toByteArray();

        Client client = new ClientImpl();
        client.watch(tubeName);

        resultEndpoint.expectedBodyReceived().constant(testBytes);
        resultEndpoint.expectedMessageCount(1);
        template.sendBody(testBytes);

        resultEndpoint.assertIsSatisfied();
        resultEndpoint.message(0).header(Headers.JOB_ID).isNotNull();

        Job job = client.reserve(4);
        assertNotNull("Beanstalk client got message", job);
        assertArrayEquals("Job body", testBytes, job.getData());
        client.delete(job.getJobId());
//        assertEquals("Job ID", header.jobId, job.getJobId());
    }

    @Test
    public void overrideTimeToRunTest() throws InterruptedException, IOException {
        byte[] testBytes = new byte[0];

        Client client = new ClientImpl();
        client.watch(tubeName);

        resultEndpoint.expectedBodyReceived().constant(testBytes);
        resultEndpoint.expectedMessageCount(1);
        template.sendBodyAndHeader(testBytes, Headers.TIME_TO_RUN, 1);
        resultEndpoint.await(2, TimeUnit.SECONDS);

        resultEndpoint.assertIsSatisfied();
        resultEndpoint.message(0).header(Headers.JOB_ID).isNotNull();

        Job job = client.reserve(1);
        assertNull("Beanstalk client has no message", job);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").to("beanstalk:"+tubeName+"?priority=1000&timeToRun=5&delay=1").to("mock:result").routeId("putNoCmd");
            }
        };
    }
}
