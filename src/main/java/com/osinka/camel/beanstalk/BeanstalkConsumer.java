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

import com.osinka.camel.beanstalk.processors.*;
import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import java.util.concurrent.Future;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.impl.PollingConsumerSupport;
import org.apache.camel.spi.Synchronization;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.camel.RuntimeCamelException;

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
    final ExecutorService executorService;
    Client client = null;
    String onFailure = BeanstalkComponent.COMMAND_BURY;

    public BeanstalkConsumer(final BeanstalkEndpoint endpoint) {
        super(endpoint);
        this.executorService = endpoint.getCamelContext().getExecutorServiceStrategy().newSingleThreadExecutor(this, "Beanstalk");

        // FIXME: should be in doStart(). https://issues.apache.org/activemq/browse/CAMEL-3158
        executorService.execute(new Runnable() {
            @Override
            public void run() {
                if (LOG.isDebugEnabled())
                    LOG.debug("Consumer initializing, getting Beanstalk client instance");
                client = getEndpoint().getConnection().newReadingClient();
            }
        });
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

    Exchange reserve(final Integer timeout) {
        final Callable<Exchange> task = new Callable<Exchange>() {
            @Override
            public Exchange call() throws RuntimeCamelException {
                if (client == null)
                    throw new RuntimeCamelException("Beanstalk client not initialized");

                final Job job = client.reserve(timeout);
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
        };
        final Future<Exchange> exchangeFuture = executorService.submit(task);

        try {
            return exchangeFuture.get();
        } catch (CancellationException e) {
            LOG.warn("Job receive cancelled", e);
        } catch (InterruptedException e) {
            LOG.warn("Job receive interrupted", e);
        } catch (ExecutionException e) {
            LOG.error("Failed to get a job", e.getCause());
        }
        return null;
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }

    @Override
    protected void doStart() {
        // FIXME: DefaultScheduledPollConsumer wraps PollingConsumer
        // FIXME: and does not call its start()
        // FIXME: https://issues.apache.org/activemq/browse/CAMEL-3158
    }

    @Override
    protected void doStop() {
        executorService.shutdown();
    }

    class ExchangeSync implements Synchronization {
        @Override
        public void onComplete(final Exchange exchange) {
            final Processor processor = new DeleteProcessor(getEndpoint(), client);
            executorService.submit(new ProcessExchangeTask(exchange, processor));
        }

        @Override
        public void onFailure(final Exchange exchange) {
            Processor processor = null;
            if (BeanstalkComponent.COMMAND_BURY.equals(onFailure))
                processor = new BuryProcessor(getEndpoint(), client);
            else if (BeanstalkComponent.COMMAND_RELEASE.equals(onFailure))
                processor = new ReleaseProcessor(getEndpoint(), client);
            else if (BeanstalkComponent.COMMAND_DELETE.equals(onFailure))
                processor = new DeleteProcessor(getEndpoint(), client);
            else
                return;

            executorService.submit(new ProcessExchangeTask(exchange, processor));
        }
    }
}
