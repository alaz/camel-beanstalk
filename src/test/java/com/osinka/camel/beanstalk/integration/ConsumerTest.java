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
import com.osinka.camel.beanstalk.Helper;
import java.io.IOException;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.junit.Ignore;
import org.junit.Test;
import static org.junit.Assert.*;

public class ConsumerTest extends BeanstalkCamelTestSupport {
    final String tubeName = "reserveTest";
    final String testMessage = "Hello, world!";

    @Test
    public void testReceive() throws IOException, InterruptedException {
        final long jobId = client.put(0, 0, 5, Helper.stringToBytes(testMessage));

        final Exchange exchange = consumer.receive("beanstalk:"+getTubeName());
        assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
    }

    @Test
    public void testReceiveNoWait() throws IOException, InterruptedException {
        assertNull(consumer.receiveNoWait("beanstalk:"+tubeName));

        final long jobId = client.put(0, 0, 5, Helper.stringToBytes(testMessage));

        final Exchange exchange = consumer.receiveNoWait("beanstalk:"+getTubeName());
        assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
        assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
    }

    @Test
    @Ignore
    public void testReceiveMock() throws IOException, InterruptedException {
        final MockEndpoint mock = getMockEndpoint("mock:result");
        final long jobId = client.put(0, 0, 5, Helper.stringToBytes(testMessage));

        mock.message(0).header(Headers.JOB_ID).isEqualTo(Long.valueOf(jobId));
        mock.expectedMessageCount(1);

        mock.assertIsSatisfied();
    }

    @Ignore
    @Test
    public void testBuryOnFailure() {
    }

    @Ignore
    @Test
    public void testDeleteOnFailure() {
    }

    @Ignore
    @Test
    public void testReleaseOnFailure() {
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("beanstalk:"+tubeName).to("mock:result");
            }
        };
    }

    @Override
    protected String getTubeName() {
        return tubeName;
    }
}
