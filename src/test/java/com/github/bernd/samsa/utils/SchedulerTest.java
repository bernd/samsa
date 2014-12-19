package com.github.bernd.samsa.utils;

import com.github.bernd.samsa.TestUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SchedulerTest {
    private Scheduler scheduler;
    private MockTime mockTime;
    private AtomicInteger counter1;
    private AtomicInteger counter2;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduler = new SamsaScheduler(1);
        mockTime = new MockTime();
        counter1 = new AtomicInteger(0);
        counter2 = new AtomicInteger(0);

        scheduler.startup();
    }

    @AfterMethod
    public void tearDown() throws Exception {
        scheduler.shutdown();
    }

    @Test
    public void testMockSchedulerNonPeriodictask() throws Exception {
        mockTime.scheduler.schedule("test1", new Runnable() {
            @Override
            public void run() {
                counter1.getAndIncrement();
            }
        }, 1, -1, TimeUnit.MILLISECONDS);
        mockTime.scheduler.schedule("test2", new Runnable() {
            @Override
            public void run() {
                counter2.getAndIncrement();
            }
        }, 100, -1, TimeUnit.MILLISECONDS);

        assertEquals(counter1.get(), 0, "Counter1 should not be incremented prior to task running.");
        assertEquals(counter2.get(), 0, "Counter2 should not be incremented prior to task running.");

        mockTime.sleep(1);

        assertEquals(counter1.get(), 1, "Counter1 should be incremented");
        assertEquals(counter2.get(), 0, "Counter2 should not be incremented");

        mockTime.sleep(100000);

        assertEquals(counter1.get(), 1, "More sleeping should not result in more incrementing on counter1.");
        assertEquals(counter2.get(), 1, "Counter2 should now be incremented.");
    }

    @Test
    public void testMockSchedulerPeriodicTask() throws Exception {
        mockTime.scheduler.schedule("test1", new Runnable() {
            @Override
            public void run() {
                counter1.getAndIncrement();
            }
        }, 1, 1, TimeUnit.MILLISECONDS);
        mockTime.scheduler.schedule("test2", new Runnable() {
            @Override
            public void run() {
                counter2.getAndIncrement();
            }
        }, 100, 100, TimeUnit.MILLISECONDS);

        assertEquals(counter1.get(), 0, "Counter1 should not be incremented prior to task running.");
        assertEquals(counter2.get(), 0, "Counter2 should not be incremented prior to task running.");

        mockTime.sleep(1);

        assertEquals(counter1.get(), 1, "Counter1 should be incremented");
        assertEquals(counter2.get(), 0, "Counter2 should not be incremented");

        mockTime.sleep(100);

        assertEquals(counter1.get(), 101, "Counter1 should be incremented 101 times");
        assertEquals(counter2.get(), 1, "Counter2 should not be incremented once");
    }

    @Test
    public void testReentrantTaskInMockScheduler() throws Exception {
        mockTime.scheduler.schedule("test1", new Runnable() {
            @Override
            public void run() {
                 mockTime.scheduler.schedule("test2", new Runnable() {
                     @Override
                     public void run() {
                         counter2.getAndIncrement();
                     }
                 }, 0, -1, TimeUnit.MILLISECONDS);
            }
        }, 1, -1, TimeUnit.MILLISECONDS);

        mockTime.sleep(1);

        assertEquals(counter2.get(), 1);
    }

    @Test
    public void testNonPeriodicTask() throws Exception {
        scheduler.schedule("test", new Runnable() {
            @Override
            public void run() {
                counter1.getAndIncrement();
            }
        },  0, -1, TimeUnit.MILLISECONDS);

        TestUtils.retry(30000, new Runnable() {
            @Override
            public void run() {
                assertEquals(counter1.get(), 1);
            }
        });

        Thread.sleep(5);
        assertEquals(counter1.get(), 1, "Should only run once");
    }

    @Test
    public void testPeriodicTask() throws Exception {
        scheduler.schedule("test", new Runnable() {
            @Override
            public void run() {
                counter1.getAndIncrement();
            }
        }, 0, 5, TimeUnit.MILLISECONDS);

        TestUtils.retry(30000, new Runnable() {
            @Override
            public void run() {
                assertTrue(counter1.get() >= 20, "Should count to 20");
            }
        });
    }
}
