package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.RuntimeExchangeException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BuryProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(BuryProducer.class);
    final Client beanstalk;

    BuryProducer(BeanstalkEndpoint endpoint, Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(Exchange exchange) {
        Message in = exchange.getIn();

        Long jobId = in.getHeader(Headers.JOB_ID, Long.class);
        if (jobId == null) {
            exchange.setException(new RuntimeExchangeException("No Job ID defined in exchange", exchange));
            return;
        }

        long priority = getPriority(in);

        try {
            boolean result = beanstalk.bury(jobId.longValue(), priority);
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Job %d buried with priority %d. Result is %b", jobId, priority, result));

            answerWith(exchange, Headers.RESULT, result);
        } catch (Exception e) {
            exchange.setException(e);
        }
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
