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
public class ReleaseProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(ReleaseProducer.class);
    final Client beanstalk;

    ReleaseProducer(BeanstalkEndpoint endpoint, Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(Exchange exchange) throws RuntimeExchangeException {
        Message in = exchange.getIn();

        Long jobId = in.getHeader(Headers.JOB_ID, Long.class);
        if (jobId == null) {
            exchange.setException(new RuntimeExchangeException("No Job ID defined in exchange", exchange));
            return;
        }

        long priority = getPriority(in);
        int delay = getDelay(in);

        try {
            boolean result = beanstalk.release(jobId.longValue(), priority, delay);
            if (LOG.isDebugEnabled())
                LOG.debug(String.format("Job %d released with priority %d, delay %d seconds. Result is %b", jobId, priority, delay, result));

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
