package com.osinka.camel.beanstalk.processors;

import com.osinka.camel.beanstalk.BeanstalkEndpoint;
import com.osinka.camel.beanstalk.BeanstalkExchangeHelper;
import com.osinka.camel.beanstalk.Headers;
import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.util.ExchangeHelper;
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
        
        final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
        final long priority = BeanstalkExchangeHelper.getPriority(endpoint, exchange.getIn());
        final boolean result = client.bury(jobId.longValue(), priority);

        if (!result && LOG.isWarnEnabled())
            LOG.warn(String.format("Failed to bury job %d (with priority %d)", jobId, priority));
        else if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d buried with priority %d. Result is %b", jobId, priority, result));

        answerWith(exchange, Headers.RESULT, result);

    }
}
