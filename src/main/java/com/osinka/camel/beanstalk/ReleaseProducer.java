package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class ReleaseProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(ReleaseProducer.class);
    final Client beanstalk;

    ReleaseProducer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(final Exchange exchange) throws NoSuchHeaderException {
        final Message in = exchange.getIn();

        final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
        final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
        final int delay = BeanstalkExchangeHelper.getDelay(getEndpoint(), exchange.getIn());

        final boolean result = beanstalk.release(jobId.longValue(), priority, delay);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d released with priority %d, delay %d seconds. Result is %b", jobId, priority, delay, result));

        answerWith(exchange, Headers.RESULT, result);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
