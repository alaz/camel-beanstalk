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
import org.apache.camel.Exchange;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * "bury" implementation.
 *
 * @author <a href="mailto:azarov@osinka.com">Alexander Azarov</a>
 */
public class BuryProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(BuryProducer.class);
    final Client beanstalk;

    BuryProducer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(final Exchange exchange) throws NoSuchHeaderException {
        final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
        final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
        final boolean result = beanstalk.bury(jobId.longValue(), priority);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d buried with priority %d. Result is %b", jobId, priority, result));

        answerWith(exchange, Headers.RESULT, result);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
