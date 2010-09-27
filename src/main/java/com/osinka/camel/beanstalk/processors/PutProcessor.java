package com.osinka.camel.beanstalk.processors;

import com.osinka.camel.beanstalk.BeanstalkEndpoint;
import com.osinka.camel.beanstalk.BeanstalkExchangeHelper;
import com.osinka.camel.beanstalk.Headers;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.NoSuchHeaderException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PutProcessor extends DefaultProcessor {
    private final transient Log LOG = LogFactory.getLog(PutProcessor.class);

    public PutProcessor(BeanstalkEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(Exchange exchange) throws NoSuchHeaderException {
        clientNotNull(exchange);

        final Message in = exchange.getIn();

        final long priority = BeanstalkExchangeHelper.getPriority(endpoint, in);
        final int delay = BeanstalkExchangeHelper.getDelay(endpoint, in);
        final int timeToRun = BeanstalkExchangeHelper.getTimeToRun(endpoint, in);

        final long jobId = client.put(priority, delay, timeToRun, in.getBody(byte[].class));
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Created job %d with priority %d, delay %d seconds and time to run %d", jobId, priority, delay, timeToRun));

        answerWith(exchange, Headers.JOB_ID, jobId);
    }
}
