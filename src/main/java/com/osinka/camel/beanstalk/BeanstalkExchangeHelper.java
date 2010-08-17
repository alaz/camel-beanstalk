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

import org.apache.camel.Message;

/**
 *
 * @author <a href="mailto:azarov@osinka.com">Alexander Azarov</a>
 */
public final class BeanstalkExchangeHelper {
    public static long getPriority(final BeanstalkEndpoint endpoint, final Message in) {
        return in.getHeader(Headers.PRIORITY, Long.valueOf(endpoint.getJobPriority()), Long.class).longValue();
    }

    public static int getDelay(final BeanstalkEndpoint endpoint, final Message in) {
        return in.getHeader(Headers.DELAY, Integer.valueOf(endpoint.getJobDelay()), Integer.class).intValue();
    }

    public static int getTimeToRun(final BeanstalkEndpoint endpoint, final Message in) {
        return in.getHeader(Headers.TIME_TO_RUN, Integer.valueOf(endpoint.getJobTimeToRun()), Integer.class).intValue();
    }
}
