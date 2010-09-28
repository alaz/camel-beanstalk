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
import org.apache.camel.Exchange;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.Message;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class KickProcessor extends DefaultProcessor {
    private final transient Log LOG = LogFactory.getLog(KickProcessor.class);

    public KickProcessor(BeanstalkEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(Exchange exchange) throws NoSuchHeaderException, InvalidPayloadException {
        clientNotNull(exchange);

        final Integer jobs = ExchangeHelper.getMandatoryInBody(exchange, Integer.class);
        final int result = client.kick(jobs);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Kick %d jobs. Kicked %d actually.", jobs, result));

        final Message answer = getAnswerMessage(exchange);
        answer.setBody(result, Integer.class);
    }
}
