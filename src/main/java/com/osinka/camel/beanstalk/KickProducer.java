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
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.Message;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * "kick" implementation.
 *
 * @author <a href="mailto:azarov@osinka.com">Alexander Azarov</a>
 */
public class KickProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(KickProducer.class);

    KickProducer(final BeanstalkEndpoint endpoint, final ThreadLocal<Client> beanstalk) {
        super(endpoint, beanstalk);
    }

    public void process(final Exchange exchange) throws InvalidPayloadException {
        final Integer jobs = ExchangeHelper.getMandatoryInBody(exchange, Integer.class);
        final int result = beanstalk().kick(jobs);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Kick %d jobs. Result is %d", jobs, result));

        final Message answer = getAnswerMessage(exchange);
        answer.setBody(result, Integer.class);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
