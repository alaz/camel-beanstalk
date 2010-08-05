package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import org.apache.camel.Exchange;
import org.apache.camel.impl.PollingConsumerSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkConsumer extends PollingConsumerSupport {
    private final transient Log log = LogFactory.getLog(BeanstalkConsumer.class);
    final Client beanstalk;
    final BeanstalkEndpoint endpoint;

    public BeanstalkConsumer(BeanstalkEndpoint endpoint, Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() {
        log.debug(String.format("%s consumer started", endpoint.getConnection()));
    }

    @Override
    protected void doStop() {
        log.debug(String.format("%s consumer stopped", endpoint.getConnection()));
    }

    @Override
    public Exchange receiveNoWait() {
        return null;
    }

    @Override
    public Exchange receive() {
        beanstalk.reserve(null);
        return null;
    }

    @Override
    public Exchange receive(long timeout) {
        beanstalk.reserve(Integer.valueOf((int)timeout));
        return null;
    }
}
