package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class TouchProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(TouchProducer.class);
    final Client beanstalk;

    TouchProducer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(final Exchange exchange) throws NoSuchHeaderException {
        final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
        final boolean result = beanstalk.touch(jobId.longValue());
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d touched. Result is %b", jobId, result));

        answerWith(exchange, Headers.RESULT, result);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
