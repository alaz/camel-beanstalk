package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import org.apache.camel.Exchange;
import org.apache.camel.impl.PollingConsumerSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkConsumer extends PollingConsumerSupport {
    private final transient Log LOG = LogFactory.getLog(BeanstalkConsumer.class);
    final Client beanstalk;

    public BeanstalkConsumer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    @Override
    protected void doStart() {
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("%s consumer started", getEndpoint().conn));
    }

    @Override
    protected void doStop() {
        if (LOG.isDebugEnabled())
            LOG.debug(String.format("%s consumer stopped", getEndpoint().conn));
    }

    @Override
    public Exchange receiveNoWait() {
        final Job job = beanstalk.reserve(0);
        // TODO: getEndpoint().createExchange(ExchangePattern.InOnly)
        // TODO: exchange.setUnitOfWork( new BeanstalkJob(jobId) )
        return null;
    }

    @Override
    public Exchange receive() {
        final Job job = beanstalk.reserve(null);
        return null;
    }

    @Override
    public Exchange receive(final long timeout) {
        final Job job = beanstalk.reserve(Integer.valueOf((int)timeout));
        return null;
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }
}
