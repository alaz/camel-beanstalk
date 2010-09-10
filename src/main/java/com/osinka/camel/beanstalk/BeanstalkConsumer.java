/**
 * Copyright (C) 2010 Alexander Azarov <azarov@osinka.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.impl.PollingConsumerSupport;
import org.apache.camel.spi.Synchronization;
import org.apache.camel.util.ExchangeHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PollingConsumer to read Beanstalk jobs.
 *
 * This consumer will add a {@link Synchronization} object to every {@link Exchange}
 * object it creates in order to react on successful exchange completion or failure.
 *
 * In the case of successful completion, Beanstalk's <code>delete</code> method is
 * called upon the job. In the case of failure the default reaction is to call
 * <code>bury</code>.
 *
 * The only configuration this consumer may have is the reaction on failures: possible
 * variants are "bury", "release" or "delete"
 *
 * @author <a href="mailto:azarov@osinka.com">Alexander Azarov</a>
 */
public class BeanstalkConsumer extends PollingConsumerSupport {
    private final transient Log LOG = LogFactory.getLog(BeanstalkConsumer.class);

    final Synchronization sync = new ExchangeSync();
    final ThreadLocal<Client> client;
    String onFailure = BeanstalkComponent.COMMAND_BURY;

    public BeanstalkConsumer(final BeanstalkEndpoint endpoint, final ThreadLocal<Client> client) {
        super(endpoint);
        this.client = client;
    }

    public Client beanstalk() {
        return client.get();
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
        return onFailure;
    }

    public void setOnFailure(String onFailure) {
        this.onFailure = onFailure;
    }

    Exchange reserve(Integer timeout) {
        final Job job = beanstalk().reserve(timeout);
        if (job == null)
            return null;

        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Received job ID %d (data length %d)", job.getJobId(), job.getData().length));

        final Exchange exchange = getEndpoint().createExchange(ExchangePattern.InOnly);
        exchange.getIn().setHeader(Headers.JOB_ID, job.getJobId());
        exchange.getIn().setBody(job.getData(), byte[].class);
        exchange.addOnCompletion(sync);

        return exchange;
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

    class ExchangeSync implements Synchronization {
        @Override
        public void onComplete(final Exchange exchange) {
            try {
                final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);
                final boolean result = beanstalk().delete(jobId.longValue());

                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Job %d succeeded, deleting it. Result is %b", jobId.longValue(), result));
            } catch (Exception e) {
                exchange.setException(e);
            }
        }

        @Override
        public void onFailure(final Exchange exchange) {
            try {
                final Long jobId = ExchangeHelper.getMandatoryHeader(exchange, Headers.JOB_ID, Long.class);

                if (BeanstalkComponent.COMMAND_BURY.equals(onFailure)) {
                    final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
                    final boolean result = beanstalk().bury(jobId.longValue(), priority);

                    if (LOG.isWarnEnabled())
                        LOG.warn(String.format("Job %d failed, burying it with priority %d. Result is %b", jobId, priority, result));
                } else if (BeanstalkComponent.COMMAND_RELEASE.equals(onFailure)) {
                    final long priority = BeanstalkExchangeHelper.getPriority(getEndpoint(), exchange.getIn());
                    final int delay = BeanstalkExchangeHelper.getDelay(getEndpoint(), exchange.getIn());
                    final boolean result = beanstalk().release(jobId.longValue(), priority, delay);

                    if (LOG.isWarnEnabled())
                        LOG.warn(String.format("Job %d failed, releasing it with priority %d, delay %d. Result is %b", jobId, priority, delay, result));
                } else if (BeanstalkComponent.COMMAND_DELETE.equals(onFailure)) {
                    final boolean result = beanstalk().delete(jobId.longValue());

                    if (LOG.isWarnEnabled())
                        LOG.warn(String.format("Job %d failed, deleting it. Result is %b", jobId, result));
                }
            } catch (Exception e) {
                exchange.setException(e);
            }
        }
    }
}
