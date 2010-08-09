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
public class BuryProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(BuryProducer.class);
    final Client beanstalk;

    BuryProducer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(final Exchange exchange) throws NoSuchHeaderException {
        final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
        final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
        final boolean result = beanstalk.bury(jobId.longValue(), priority);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Job %d buried with priority %d. Result is %b", jobId, priority, result));

        answerWith(exchange, Headers.RESULT, result);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
