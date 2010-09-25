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
