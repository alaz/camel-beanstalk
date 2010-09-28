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

package com.osinka.camel.beanstalk.processors;

import com.osinka.camel.beanstalk.BeanstalkEndpoint;
import com.osinka.camel.beanstalk.BeanstalkExchangeHelper;
import com.osinka.camel.beanstalk.Headers;
import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.NoSuchHeaderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReleaseProcessor extends DefaultProcessor {
    private final transient Log LOG = LogFactory.getLog(ReleaseProcessor.class);

    public ReleaseProcessor(BeanstalkEndpoint endpoint) {
        super(endpoint);
    }

    public ReleaseProcessor(BeanstalkEndpoint endpoint, Client client) {
        super(endpoint, client);
    }

    @Override
    public void process(Exchange exchange) throws NoSuchHeaderException {
        clientNotNull(exchange);

        final Message in = exchange.getIn();

        final Long jobId = BeanstalkExchangeHelper.getJobID(exchange);
        final long priority = BeanstalkExchangeHelper.getPriority(endpoint, in);
        final int delay = BeanstalkExchangeHelper.getDelay(endpoint, in);

        final boolean result = client.release(jobId.longValue(), priority, delay);
        if (!result && LOG.isWarnEnabled())
            LOG.warn(String.format("Failed to release job %d (priority %d, delay %d)", jobId, priority, delay));
        else if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d released with priority %d, delay %d seconds. Result is %b", jobId, priority, delay, result));

        answerWith(exchange, Headers.RESULT, result);
    }
}
