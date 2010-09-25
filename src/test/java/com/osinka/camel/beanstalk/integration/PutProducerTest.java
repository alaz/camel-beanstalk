/**
 * Copyright (C) 2010 Alexander Azarov <azarov@osinka.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.osinka.camel.beanstalk.integration;

import com.osinka.camel.beanstalk.Headers;
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

public class PutProducerTest extends BeanstalkCamelTestSupport {
    final String tubeName = "putTest";
    final String testMessage = "Hello, world!";

    @EndpointInject(uri = "mock:result")
    protected MockEndpoint resultEndpoint;

    @Produce(uri = "direct:start")
    protected ProducerTemplate direct;

    @Test
    public void testPut() throws InterruptedException, IOException {
        resultEndpoint.expectedMessageCount(1);
        resultEndpoint.allMessages().header(Headers.JOB_ID).isNotNull();
        direct.sendBody(testMessage);

        resultEndpoint.assertIsSatisfied();

        final Long jobId = resultEndpoint.getReceivedExchanges().get(0).getIn().getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in 'In' message", jobId);

        final Job job = client.reserve(1);
        assertNotNull("Beanstalk client got message", job);
        assertEquals("Job body from the server", testMessage, new String(job.getData()));
        assertEquals("Job ID from the server", jobId.longValue(), job.getJobId());
        client.delete(jobId.longValue());
    }

    @Test
    public void testOut() throws InterruptedException, IOException {
        final Endpoint endpoint = context.getEndpoint("beanstalk:"+tubeName);
        final Exchange exchange = template.send(endpoint, ExchangePattern.InOut, new Processor() {
            public void process(Exchange exchange) {
                exchange.getIn().setBody(testMessage);
            }
        });

        final Message out = exchange.getOut();
        assertNotNull("Out message", out);

        final Long jobId = out.getHeader(Headers.JOB_ID, Long.class);
        assertNotNull("Job ID in 'Out' message", jobId);

        final Job job = client.reserve(1);
        assertNotNull("Beanstalk client got message", job);
        assertEquals("Job body from the server", testMessage, new String(job.getData()));
        assertEquals("Job ID from the server", jobId.longValue(), job.getJobId());
        client.delete(jobId.longValue());
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

        final Job job = client.reserve(0);
        assertNull("Beanstalk client has no message", job);
        client.delete(jobId.longValue());
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
