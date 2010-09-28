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
import org.apache.camel.NoSuchHeaderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BuryProcessor extends DefaultProcessor {
    private final transient Log LOG = LogFactory.getLog(BuryProcessor.class);

    public BuryProcessor(BeanstalkEndpoint endpoint) {
        super(endpoint);
    }

    public BuryProcessor(BeanstalkEndpoint endpoint, Client client) {
        super(endpoint, client);
    }

    @Override
    public void process(Exchange exchange) throws NoSuchHeaderException {
        clientNotNull(exchange);
        
        final Long jobId = BeanstalkExchangeHelper.getJobID(exchange);
        final long priority = BeanstalkExchangeHelper.getPriority(endpoint, exchange.getIn());
        final boolean result = client.bury(jobId.longValue(), priority);

        if (!result && LOG.isWarnEnabled())
            LOG.warn(String.format("Failed to bury job %d (with priority %d)", jobId, priority));
        else if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d buried with priority %d. Result is %b", jobId, priority, result));

        answerWith(exchange, Headers.RESULT, result);

    }
}
