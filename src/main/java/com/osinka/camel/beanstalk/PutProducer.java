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
import org.apache.camel.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * "put" implementation.
 *
 * @author <a href="mailto:azarov@osinka.com">Alexander Azarov</a>
 */
public class PutProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(PutProducer.class);
    final Client beanstalk;

    PutProducer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(final Exchange exchange) {
        final Message in = exchange.getIn();

        final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
        final int delay = BeanstalkExchangeHelper.getDelay(getEndpoint(), exchange.getIn());
        final int timeToRun = BeanstalkExchangeHelper.getTimeToRun(getEndpoint(), exchange.getIn());

        final long jobId = beanstalk.put(priority, delay, timeToRun, in.getBody(byte[].class));
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Created job %d with priority %d, delay %d seconds and time to run %d", jobId, priority, delay, timeToRun));

        answerWith(exchange, Headers.JOB_ID, jobId);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
