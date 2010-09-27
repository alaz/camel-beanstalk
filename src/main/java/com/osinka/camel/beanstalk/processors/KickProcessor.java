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
