package com.github.bernd.samsa;

import java.util.concurrent.TimeUnit;

public interface Scheduler {
    /**
     * Initialize this scheduler so it is ready to accept scheduling of tasks
     */
    public void startup();

    /**
     * Shutdown this scheduler. When this method is complete no more executions of background tasks will occur.
     * This includes tasks scheduled with a delayed execution.
     */
    public void shutdown();

    /**
     * Schedule a task
     *
     * @param name   The name of this task
     * @param delay  The amount of time to wait before the first execution
     * @param period The period with which to execute the task. If < 0 the task will execute only once.
     * @param unit   The unit for the preceding times.
     */
    public void schedule(final String name,
                         final Runnable runnable,
                         final long delay,
                         final long period,
                         final TimeUnit unit);
}