package com.osinka.camel.beanstalk;

import org.apache.camel.Component;
import org.apache.camel.Exchange;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClientImpl.ClientImpl;
import org.apache.camel.ExchangePattern;
import org.apache.camel.PollingConsumer;
import org.apache.camel.impl.DefaultExchange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkEndpoint extends DefaultPollingEndpoint {
    private final transient Log log = LogFactory.getLog(BeanstalkEndpoint.class);
    final Client beanstalk;
    final ConnectionSettings conn;

    long priority = BeanstalkComponent.DEFAULT_PRIORITY;
    int delay     = BeanstalkComponent.DEFAULT_DELAY;
    int timeToRun = BeanstalkComponent.DEFAULT_TIME_TO_RUN;

    BeanstalkEndpoint(String uri, Component component, ConnectionSettings conn) {
        super(uri, component);

        this.conn = conn;
        beanstalk = new ClientImpl(conn.host, conn.port);
        beanstalk.useTube(conn.tube);
    }

    ConnectionSettings getConnection() {
        return conn;
    }

    public long getPriority() {
        return priority;
    }

    public void setPriority(long priority) {
        this.priority = priority;
    }

    public int getDelay() {
        return delay;
    }

    public void setDelay(int delay) {
        this.delay = delay;
    }

    public int getTimeToRun() {
        return timeToRun;
    }

    public void setTimeToRun(int timeToRun) {
        this.timeToRun = timeToRun;
    }

    @Override
    public Producer createProducer() {
        return new BeanstalkProducer(this, beanstalk);
    }

    @Override
    public PollingConsumer createPollingConsumer() {
        return new BeanstalkConsumer(this, beanstalk);
    }

    @Override
    public Exchange createExchange(ExchangePattern pattern) {
        Exchange exchange = new DefaultExchange(this, pattern);
        return exchange;
    }

    @Override
    public boolean isLenientProperties() {
        return false;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
