package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.InvalidPayloadException;
import org.apache.camel.Message;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class KickProducer extends AbstractBeanstalkProducer {
    private final transient Log LOG = LogFactory.getLog(KickProducer.class);
    final Client beanstalk;

    KickProducer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    public void process(final Exchange exchange) throws InvalidPayloadException {
        final Integer jobs = ExchangeHelper.getMandatoryInBody(exchange, Integer.class);
        final int result = beanstalk.kick(jobs);
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Kick %d jobs. Result is %d", jobs, result));

        final Message answer = getAnswerMessage(exchange);
        answer.setBody(result, Integer.class);
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
