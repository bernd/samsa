package com.github.bernd.samsa.utils;

import com.google.common.collect.Queues;

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

/**
 * A mock scheduler that executes tasks synchronously using a mock time instance. Tasks are executed synchronously when
 * the time is advanced. This class is meant to be used in conjunction with MockTime.
 *
 * Example usage
 * <code>
 *   val time = new MockTime
 *   time.scheduler.schedule("a task", println("hello world: " + time.milliseconds), delay = 1000)
 *   time.sleep(1001) // this should cause our scheduled task to fire
 * </code>
 *
 * Incrementing the time to the exact next execution time of a task will result in that task executing (it as if execution itself takes no time).
 */
public class MockScheduler implements Scheduler {
    private final Time time;

    private final PriorityQueue<MockTask> tasks = Queues.newPriorityQueue();

    public MockScheduler(final Time time) {
        this.time = time;
    }

    @Override
    public void startup() {
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            tasks.clear();
        }
    }

    @Override
    public void schedule(String name, Runnable runnable, long delay, long period, TimeUnit unit) {
        synchronized (this) {
            tasks.add(new MockTask(name, runnable, time.milliseconds() + delay, period));
            tick();
        }
    }

    /**
     * Check for any tasks that need to execute. Since this is a mock scheduler this check only occurs
     * when this method is called and the execution happens synchronously in the calling thread.
     * If you are using the scheduler associated with a MockTime instance this call be triggered automatically.
     */
    public void tick() {
        synchronized (this) {
            final long now = time.milliseconds();

            while(!tasks.isEmpty() && tasks.peek().nextExecution <= now) {
                /* pop and execute the task with the lowest next execution time */
                final MockTask curr = tasks.remove();
                curr.fun.run();
                /* if the task is periodic, reschedule it and re-enqueue */
                if(curr.isPeriodic()) {
                    curr.nextExecution += curr.period;
                    this.tasks.add(curr);
                }
            }
        }
    }

    private class MockTask implements Comparable<MockTask> {
        public final String name;
        public final Runnable fun;
        public long nextExecution;
        public final long period;

        public MockTask(final String name, Runnable fun, final long nextExecution, final long period) {
            this.name = name;
            this.fun = fun;
            this.nextExecution = nextExecution;
            this.period = period;
        }

        public boolean isPeriodic() {
            return period >= 0;
        }

        @Override
        public int compareTo(MockTask t) {
            if (t.nextExecution == nextExecution) {
                return 0;
            } else if (t.nextExecution > nextExecution) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}
