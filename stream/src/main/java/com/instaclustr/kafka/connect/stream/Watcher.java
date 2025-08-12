package com.instaclustr.kafka.connect.stream;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Watcher implements Closeable {
	private static final Logger log = LoggerFactory.getLogger(Watcher.class);
	
	private final Duration checkDelay;
	private final Duration stopWait;
	private final Duration checkTimeout;
	private final ScheduledExecutorService scheduler;
	private final ExecutorService executor;
	
	Watcher(Duration checkDelay, Duration stopDelay, Duration checkTimeout, ScheduledExecutorService scheduler, ExecutorService executor) {
	    this.checkDelay = checkDelay;
	    this.stopWait = stopDelay;
	    this.checkTimeout = checkTimeout;
	    this.scheduler = scheduler;
	    this.executor = executor;
	}
	
	public static Watcher of(Duration checkDelay) {
	    return new Watcher(checkDelay, Duration.ofSeconds(5), checkDelay, Executors.newSingleThreadScheduledExecutor(), Executors.newSingleThreadExecutor());
	}

	public ScheduledFuture<?> watch(Callable<Boolean> condition, Runnable action) {
	    
	    return scheduler.scheduleWithFixedDelay(() -> {
	        var future = executor.submit(() -> {
                try {
                    if (condition.call()) {
                        log.debug("Condition met, running action, thread: {}", Thread.currentThread().getName());
                        action.run();
                    } else {
                        log.debug("Condition not met, thread: {}", Thread.currentThread().getName());
                    }
                } catch (Exception e) {
                    log.warn("Error in condition or action", e);
                }
	        });
	        try {
                future.get(checkTimeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException | InterruptedException | ExecutionException e) {
                log.warn("Unable to wait for condition to meet, canceling the action", e);
                future.cancel(true);
            }
	    }, checkDelay.toMillis(), checkDelay.toMillis(), TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() throws IOException {
	    log.debug("Closing watcher");
	    List<Thread> ts = List.of(
	            new Thread(() -> shutdown(scheduler, stopWait)),
	            new Thread(() -> shutdown(executor, stopWait)));

	    ts.stream().map(t -> { t.start(); return t; }).collect(Collectors.toList()).forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                log.error("Unable to terminate thread pool");
            }
        });
	}
	
	private static void shutdown(ExecutorService es, Duration wait) {
		es.shutdown();
		try {
			if (! es.awaitTermination(wait.toMillis(), TimeUnit.MILLISECONDS)) {
				es.shutdownNow();
				if (! es.awaitTermination(wait.toMillis(), TimeUnit.MILLISECONDS)) {
					log.error("Unable to terminate thread pool cleanly");
				}
			}
		} catch (InterruptedException e) {
			// (Re-)cancel if interrupted above
			es.shutdownNow();
			// Propagate interrupt
			Thread.currentThread().interrupt();
		}
	}
}
