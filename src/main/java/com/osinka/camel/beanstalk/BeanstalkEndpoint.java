package com.osinka.camel.beanstalk;

import java.util.Map;
import org.apache.camel.Component;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultPollingEndpoint;
import org.apache.camel.PollingConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkEndpoint extends DefaultPollingEndpoint {
    private final transient Log LOG = LogFactory.getLog(BeanstalkEndpoint.class);
    final ConnectionSettings conn;

    String command = BeanstalkComponent.COMMAND_PUT;
    long priority = BeanstalkComponent.DEFAULT_PRIORITY;
    int delay     = BeanstalkComponent.DEFAULT_DELAY;
    int timeToRun = BeanstalkComponent.DEFAULT_TIME_TO_RUN;

    BeanstalkEndpoint(final String uri, final Component component, final ConnectionSettings conn) {
        super(uri, component);

        this.conn = conn;
    }

    public void setCommand(final String command) {
        this.command = command;
    }

    public void setJobPriority(final long priority) {
        this.priority = priority;
    }

    public long getJobPriority() {
        return priority;
    }

    public void setJobDelay(final int delay) {
        this.delay = delay;
    }

    public int getJobDelay() {
        return delay;
    }

    public void setJobTimeToRun(final int timeToRun) {
        this.timeToRun = timeToRun;
    }

    public int getJobTimeToRun() {
        return timeToRun;
    }

    @Override
    public Producer createProducer() throws IllegalArgumentException {
        if (BeanstalkComponent.COMMAND_PUT.equals(command)) {
            if (LOG.isDebugEnabled())
                LOG.debug("Creating 'put' producer for "+getEndpointUri());
            return new PutProducer(this, conn.newWritingClient());
        } else if (BeanstalkComponent.COMMAND_RELEASE.equals(command)) {
            if (LOG.isDebugEnabled())
                LOG.debug("Creating 'release' producer for "+getEndpointUri());
            return new ReleaseProducer(this, conn.newWritingClient());
        } else if (BeanstalkComponent.COMMAND_BURY.equals(command)) {
            if (LOG.isDebugEnabled())
                LOG.debug("Creating 'bury' producer for "+getEndpointUri());
            return new BuryProducer(this, conn.newWritingClient());
        } else if (BeanstalkComponent.COMMAND_TOUCH.equals(command)) {
            if (LOG.isDebugEnabled())
                LOG.debug("Creating 'touch' producer for "+getEndpointUri());
            return new TouchProducer(this, conn.newWritingClient());
        } else if (BeanstalkComponent.COMMAND_DELETE.equals(command)) {
            if (LOG.isDebugEnabled())
                LOG.debug("Creating 'delete' producer for "+getEndpointUri());
            return new DeleteProducer(this, conn.newWritingClient());
        }

        throw new IllegalArgumentException(String.format("Unknown command for Beanstalk endpoint: %s", command));
    }

    @Override
    public PollingConsumer createPollingConsumer() {
        if (LOG.isDebugEnabled())
            LOG.debug("Creating polling consumer for "+getEndpointUri());
        return new BeanstalkConsumer(this, conn.newReadingClient());
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
