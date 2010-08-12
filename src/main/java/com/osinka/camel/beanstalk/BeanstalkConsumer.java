package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.NoSuchHeaderException;
import org.apache.camel.impl.PollingConsumerSupport;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author alaz
 */
public class BeanstalkConsumer extends PollingConsumerSupport implements Synchronization {
    private final transient Log LOG = LogFactory.getLog(BeanstalkConsumer.class);
    
    final Client beanstalk;
    String cmdOnFailure = "release";

    public BeanstalkConsumer(final BeanstalkEndpoint endpoint, final Client beanstalk) {
        super(endpoint);
        this.beanstalk = beanstalk;
    }

    @Override
    public Exchange receiveNoWait() {
        return reserve(Integer.valueOf(0));
    }

    @Override
    public Exchange receive() {
        return reserve(null);
    }

    @Override
    public Exchange receive(final long timeout) {
        return reserve( Integer.valueOf((int)timeout) );
    }

    public String getOnFailure() {
        return cmdOnFailure;
    }

    public void setOnFailure(String cmd) {
        this.cmdOnFailure = cmd;
    }

    Exchange reserve(Integer timeout) {
        final Job job = beanstalk.reserve(timeout);
        if (job == null)
            return null;

        if (LOG.isInfoEnabled())
            LOG.info(String.format("Received job ID %d (data length %d)", job.getJobId(), job.getData().length));

        final Exchange exchange = getEndpoint().createExchange(ExchangePattern.InOnly);
        exchange.getIn().setHeader(Headers.JOB_ID, job.getJobId());
        exchange.getIn().setBody(job.getData(), byte[].class);
        exchange.addOnCompletion(this);

        return exchange;
    }

    @Override
    public void onComplete(final Exchange exchange) {
        try {
            final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
            final boolean result = beanstalk.delete(jobId.longValue());

            if (LOG.isInfoEnabled())
                LOG.info(String.format("Job ID succeeded, deleting it. Result is %b", jobId.longValue(), result));
        } catch (Exception e) {
            exchange.setException(e);
        }
    }

    @Override
    public void onFailure(final Exchange exchange) {
        try {
            final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);

            if (BeanstalkComponent.COMMAND_BURY.equals(cmdOnFailure)) {
                final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
                final boolean result = beanstalk.bury(jobId.longValue(), priority);

                if (LOG.isWarnEnabled())
                    LOG.warn(String.format("Job ID failed, burying it with priority %d. Result is %b", jobId, priority, result));
            } else if (BeanstalkComponent.COMMAND_RELEASE.equals(cmdOnFailure)) {
                final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
                final int delay = BeanstalkExchangeHelper.getDelay(getEndpoint(), exchange.getIn());
                final boolean result = beanstalk.release(jobId.longValue(), priority, delay);

                if (LOG.isWarnEnabled())
                    LOG.warn(String.format("Job ID failed, releasing it with priority %d, delay %d. Result is %b", jobId, priority, delay, result));
            } else if (BeanstalkComponent.COMMAND_DELETE.equals(cmdOnFailure)) {
                final boolean result = beanstalk.delete(jobId.longValue());

                if (LOG.isWarnEnabled())
                    LOG.warn(String.format("Job ID failed, deleting it. Result is %b", jobId, result));
            }
        } catch (Exception e) {
            exchange.setException(e);
        }
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }
}
