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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.Processor;
import org.apache.camel.spi.Synchronization;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.impl.ScheduledPollConsumer;

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
public class BeanstalkConsumer extends ScheduledPollConsumer {
    private final transient Log LOG = LogFactory.getLog(BeanstalkConsumer.class);

    String onFailure = BeanstalkComponent.COMMAND_BURY;

    private ExecutorService beanstalkExecutor;
    private Client client = null;

    private final Synchronization sync = new ExchangeSync();
    private final Callable<Exchange> pollTask = new Callable<Exchange>() {
        final Integer NO_WAIT = Integer.valueOf(0);

        @Override
        public Exchange call() throws RuntimeCamelException {
            if (client == null)
                throw new RuntimeCamelException("Beanstalk client not initialized");

            final Job job = client.reserve(NO_WAIT);
            if (job == null)
                return null;

            if (LOG.isDebugEnabled())
                LOG.debug(String.format("%s received job ID %d (data length %d)", getEndpoint().conn, job.getJobId(), job.getData().length));

            final Exchange exchange = getEndpoint().createExchange(ExchangePattern.InOnly);
            exchange.setProperty(Headers.JOB_ID, job.getJobId());
            exchange.getIn().setBody(job.getData(), byte[].class);
            exchange.addOnCompletion(sync);

            return exchange;
        }
    };

    public BeanstalkConsumer(BeanstalkEndpoint endpoint, Processor processor) {
        super(endpoint, processor);
    }

    public BeanstalkConsumer(BeanstalkEndpoint endpoint, Processor processor, ScheduledExecutorService executor) {
        super(endpoint, processor, executor);
    }

    @Override
    protected void poll() throws Exception {
        while (isPollAllowed()) {
            final Exchange exchange = beanstalkExecutor.submit(pollTask).get();
            if (exchange == null)
                break;

            getProcessor().process(exchange);
        }
    }

    public String getOnFailure() {
        return onFailure;
    }

    public void setOnFailure(String onFailure) {
        this.onFailure = onFailure;
    }

    @Override
    public BeanstalkEndpoint getEndpoint() {
        return (BeanstalkEndpoint) super.getEndpoint();
    }

    @Override
    protected void doStart() throws Exception {
        beanstalkExecutor = getEndpoint().getCamelContext().getExecutorServiceStrategy().newSingleThreadExecutor(this, "Beanstalk");
        beanstalkExecutor.execute(new Runnable() {
            @Override
            public void run() {
                client = getEndpoint().getConnection().newReadingClient();
            }
        });
        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        beanstalkExecutor.shutdown();
    }

    class ExchangeSync implements Synchronization {
        @Override
        public void onComplete(final Exchange exchange) {
            try {
                final Processor processor = new DeleteProcessor(getEndpoint(), client);
                beanstalkExecutor.submit(new ProcessExchangeTask(exchange, processor)).get();
            } catch (Throwable t) {
                if (LOG.isFatalEnabled())
                    LOG.fatal(String.format("%s failed to onComplete %s", getEndpoint().getConnection(), exchange), t);
            }
        }

        @Override
        public void onFailure(final Exchange exchange) {
            try {
                Processor processor = null;
                if (BeanstalkComponent.COMMAND_BURY.equals(onFailure))
                    processor = new BuryProcessor(getEndpoint(), client);
                else if (BeanstalkComponent.COMMAND_RELEASE.equals(onFailure))
                    processor = new ReleaseProcessor(getEndpoint(), client);
                else if (BeanstalkComponent.COMMAND_DELETE.equals(onFailure))
                    processor = new DeleteProcessor(getEndpoint(), client);
                else
                    return;

                beanstalkExecutor.submit(new ProcessExchangeTask(exchange, processor)).get();
            } catch (Throwable t) {
                if (LOG.isFatalEnabled())
                    LOG.fatal(String.format("%s failed to onFailure %s", getEndpoint().getConnection(), exchange), t);
            }
        }
    }
}
