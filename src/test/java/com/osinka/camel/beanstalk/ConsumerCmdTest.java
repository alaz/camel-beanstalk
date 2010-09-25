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

package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.JndiRegistry;
import org.apache.camel.test.CamelTestSupport;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.apache.camel.util.ObjectHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

public class ConsumerCmdTest extends CamelTestSupport {
    final String testMessage = "hello, world";

    @Mock
    Client client;

    @EndpointInject(uri = "mock:result")
    MockEndpoint result;

    boolean shouldIdie = false;
    final Processor processor = new Processor() {
        @Override
        public void process(Exchange exchange) throws InterruptedException {
            if (shouldIdie) throw new InterruptedException("die");
        }
    };

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
        result.assertIsNotSatisfied(50);

        verify(client, atLeastOnce()).reserve(anyInt());
        verify(client).release(jobId, priority, delay);
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("beanstalk:tube?onFailure=release").process(processor).to("mock:result").routeId("test");
            }
        };
    }

    @Override
    protected JndiRegistry createRegistry() throws Exception {
        JndiRegistry jndi = super.createRegistry();
        jndi.bind("myPoll", new MyPollStrategy());
        return jndi;
    }

    @Before
    @Override
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        reset(client);
	Helper.mockComponent(client);
	super.setUp();
    }

    private class MyPollStrategy implements PollingConsumerPollStrategy {
        @Override
        public boolean begin(Consumer consumer, Endpoint endpoint) {
            try {
                System.err.println("Starting "+consumer);
                consumer.start();
            } catch (Exception e) {
                ObjectHelper.wrapRuntimeCamelException(e);
            }
            return true;
        }

        @Override
        public void commit(Consumer consumer, Endpoint endpoint) {
        }

        @Override
        public boolean rollback(Consumer consumer, Endpoint endpoint, int retryCounter, Exception cause) throws Exception {
            throw cause;
        }
    }
}
