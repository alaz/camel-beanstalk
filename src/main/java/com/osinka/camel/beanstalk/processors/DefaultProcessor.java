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
import com.surftools.BeanstalkClient.Client;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.util.ExchangeHelper;

abstract class DefaultProcessor implements CommandProcessor {
    protected final BeanstalkEndpoint endpoint;
    protected Client client = null;

    public DefaultProcessor(BeanstalkEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public DefaultProcessor(BeanstalkEndpoint endpoint, Client client) {
        this.endpoint = endpoint;
        this.client = client;
    }

    public void init() {
        this.client = endpoint.getConnection().newWritingClient();
    }

    protected Message getAnswerMessage(final Exchange exchange) {
        Message answer = exchange.getIn();
        if (ExchangeHelper.isOutCapable(exchange)) {
            answer = exchange.getOut();
            // preserve headers
            answer.getHeaders().putAll(exchange.getIn().getHeaders());
        }
        return answer;
    }

    protected void answerWith(final Exchange exchange, final String header, final Object value) {
        final Message answer = getAnswerMessage(exchange);
        answer.setHeader(header, value);
    }

    protected void clientNotNull(Exchange exchange) throws CamelExecutionException {
        if (client == null)
            throw new CamelExecutionException("Beanstalk client not initialized", exchange);
    }
}
