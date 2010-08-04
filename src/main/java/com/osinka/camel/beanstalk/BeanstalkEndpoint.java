package com.osinka.camel.beanstalk;

import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Producer;
import org.apache.camel.Processor;
import org.apache.camel.impl.DefaultPollingEndpoint;
import com.surftools.BeanstalkClient.Client;

/**
 *
 * @author alaz
 */
public class BeanstalkEndpoint extends DefaultPollingEndpoint {
    final Client beanstalk;

    BeanstalkEndpoint(String uri, Component component, Client beanstalk) {
        super(uri, component);
        this.beanstalk = beanstalk;
    }

    @Override
    public Producer createProducer() {
        return null;
    }

    @Override
    public Consumer createConsumer(Processor processor) {
        return null;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }
}
