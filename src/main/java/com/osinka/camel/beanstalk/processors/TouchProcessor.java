package com.osinka.camel.beanstalk.processors;

import com.osinka.camel.beanstalk.BeanstalkEndpoint;
import com.osinka.camel.beanstalk.Headers;
import org.apache.camel.Exchange;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TouchProcessor extends DefaultProcessor {
    private final transient Log LOG = LogFactory.getLog(TouchProcessor.class);

    public TouchProcessor(BeanstalkEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public void process(Exchange exchange) throws NoSuchHeaderException {
        clientNotNull(exchange);

        final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
        final boolean result = client.touch(jobId.longValue());
        if (!result && LOG.isWarnEnabled())
            LOG.warn(String.format("Failed to touch job %d", jobId));
        else if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d touched. Result is %b", jobId, result));

        answerWith(exchange, Headers.RESULT, result);
    }
}
