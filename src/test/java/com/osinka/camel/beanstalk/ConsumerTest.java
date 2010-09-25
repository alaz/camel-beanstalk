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
import org.apache.camel.EndpointInject;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.test.CamelTestSupport;
import org.apache.camel.util.EndpointHelper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ConsumerTest extends CamelTestSupport {
    @Mock Client client;
    final String testMessage = "hello, world";

    @EndpointInject(uri = "beanstalk:tube")
    protected BeanstalkEndpoint endpoint;

    @Test
    public void testReceive() throws Exception {
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

    @Test
    public void testReceiveEmpty() throws Exception {
        when(client.reserve(0)).thenReturn(null);

        EndpointHelper.pollEndpoint(endpoint, new Processor() {
            public void process(Exchange exchange) {
                fail();
            }
        }, 0);
        verify(client).reserve(0);
    }

    @Test
    public void testReceiveNoWait() throws Exception {
        final long jobId = 111;
        final byte[] payload = Helper.stringToBytes(testMessage);
        final Job jobMock = mock(Job.class);

        when(jobMock.getJobId()).thenReturn(jobId);
        when(jobMock.getData()).thenReturn(payload);
        when(client.reserve(anyInt())).thenReturn(jobMock);

        final Exchange exchange = endpoint.createPollingConsumer().receiveNoWait();
        assertNotNull("Exchange", exchange);
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
        when(client.reserve(timeout))
                .thenReturn(jobMock)
                .thenReturn(null);

        EndpointHelper.pollEndpoint(endpoint, new Processor() {
            public void process(Exchange exchange) {
                assertEquals("Job ID", Long.valueOf(jobId), exchange.getIn().getHeader(Headers.JOB_ID, Long.class));
                assertEquals("Job body", testMessage, exchange.getIn().getBody(String.class));
            }
        }, timeout);
        verify(client, times(2)).reserve(timeout);
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
