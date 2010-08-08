package com.osinka.camel.beanstalk;

import com.surftools.BeanstalkClient.Client;
import com.surftools.BeanstalkClient.Job;
import java.util.UUID;
import org.apache.camel.test.junit4.CamelTestSupport;
import org.junit.After;
import org.junit.Before;

/**
 *
 * @author alaz
 */
public abstract class BeanstalkCamelTestSupport extends CamelTestSupport {
    protected Client beanstalk = null;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();

//        final UUID uuid = UUID.randomUUID();
        final String tube = getTubeName();
        beanstalk = new ConnectionSettings(tube).newWritingClient();
        beanstalk.watch(tube);
        clearTube();
    }

    @After
    @Override
    public void tearDown() throws Exception {
        clearTube();
        super.tearDown();
    }

    protected void clearTube() {
        if (beanstalk != null) {
            Job job;
            while (beanstalk.kick(100) >0)
                ;
            while ((job = beanstalk.reserve(0)) != null)
                beanstalk.delete(job.getJobId());
        }
    }

    protected abstract String getTubeName();
}
