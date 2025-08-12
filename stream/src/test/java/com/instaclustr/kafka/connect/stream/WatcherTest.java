package com.instaclustr.kafka.connect.stream;

import static java.lang.Thread.sleep;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import org.joda.time.Instant;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WatcherTest {

    private Callable<Boolean> condition;
    private Runnable action;
    
    @BeforeMethod
    public void setup() {
        condition = mock(Callable.class);
        action = mock(Runnable.class);
    }
    
    @Test
    public void testWatch() throws Exception {
        when(condition.call()).thenReturn(true, false, true, false);
        Watcher watcher = Watcher.of(Duration.ofMillis(100));
        watcher.watch(condition, action);
        sleep(Duration.ofMillis(450).toMillis());
        watcher.close();

        verify(condition, times(4)).call();
        verify(action, times(2)).run();
    }
    
    @Test
    public void testErrorInCondition() throws Exception {
        when(condition.call()).thenThrow(new RuntimeException("condition error"));
        Watcher watcher = Watcher.of(Duration.ofMillis(100));
        watcher.watch(condition, action);
        sleep(Duration.ofMillis(350).toMillis());
        watcher.close();

        verify(condition, times(3)).call();
        verifyNoInteractions(action);
    }
    
    @Test
    public void testTimeout() throws Exception {
        when(condition.call()).then(ignore -> { sleep(1000); return true; });
        Watcher watcher = Watcher.of(Duration.ofMillis(100)); // timeout after 100ms
        watcher.watch(condition, action);
        sleep(Duration.ofMillis(350).toMillis());
        watcher.close();

        verify(condition, times(2)).call(); // fired at 100(initial delay), 100(base)+100(timeout)+100(delay), 400(base)+100(timeout)+100(delay),...
        verifyNoInteractions(action);
    }
    
    @Test
    public void testShutdown() throws Exception {
        ScheduledExecutorService s = mock(ScheduledExecutorService.class);
        ExecutorService e = mock(ExecutorService.class);
        Duration expectedWait = Duration.ofMillis(100);
        Watcher watcher = new Watcher(null, expectedWait, null, s, e);
        
        when(s.awaitTermination(Mockito.anyLong(), Mockito.any())).thenAnswer(ignore -> { sleep(expectedWait.toMillis()); return true; });
        when(e.awaitTermination(Mockito.anyLong(), Mockito.any())).thenAnswer(ignore -> { sleep(expectedWait.toMillis()); return false; });
        
        Instant closeBegin = Instant.now();
        watcher.close();
        Instant closeEnd = Instant.now();
        
        verify(s).shutdown();
        verify(s, times(0)).shutdownNow();
        verify(e).shutdown();
        verify(e).shutdownNow();
        long tolerance = 50;
        Assert.assertTrue(closeEnd.getMillis() - closeBegin.getMillis() <= expectedWait.multipliedBy(2).toMillis() + tolerance);
    }
}
