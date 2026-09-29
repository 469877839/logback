/*
 * Logback: the reliable, generic, fast and flexible logging framework.
 * Copyright (C) 1999-2026, QOS.ch. All rights reserved.
 *
 * This program and the accompanying materials are dual-licensed under
 * either the terms of the Eclipse Public License v2.0 as published by
 * the Eclipse Foundation
 *
 *   or (per the licensee's choosing)
 *
 * under the terms of the GNU Lesser General Public License version 2.1
 * as published by the Free Software Foundation.
 */
package ch.qos.logback.core;

import ch.qos.logback.core.spi.AppenderAttachable;
import ch.qos.logback.core.spi.AppenderAttachableImpl;
import ch.qos.logback.core.util.InterruptUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This appender and derived classes, log events asynchronously. In order to
 * avoid loss of logging events, this appender should be closed. It is the
 * user's responsibility to close appenders, typically at the end of the
 * application lifecycle.
 * <p>
 * This appender buffers events in a {@link BlockingQueue}. {@link Worker}
 * thread created by this appender takes events from the head of the queue, and
 * dispatches them to the single appender attached to this appender.
 * <p>
 * Please refer to the
 * <a href="http://logback.qos.ch/manual/appenders.html#AsyncAppender">logback
 * manual</a> for further information about this appender.
 * </p>
 *
 * @param <E>
 * @author Ceki G&uuml;lc&uuml;
 * @author Torsten Juergeleit
 * @since 1.0.4
 */
public class AsyncAppenderBase<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E> {

    AppenderAttachableImpl<E> aai = new AppenderAttachableImpl<E>();
    BlockingQueue<E> blockingQueue;

    /**
     * The default buffer size.
     */
    public static final int DEFAULT_QUEUE_SIZE = 256;
    int queueSize = DEFAULT_QUEUE_SIZE;

    int appenderCount = 0;

    static final int UNDEFINED = -1;
    int discardingThreshold = UNDEFINED;
    boolean neverBlock = false;

    /**
     * Number of {@link #doAppend(Object)} invocations currently in flight.
     * Consulted by the worker thread while flushing during stop, in order to
     * decide when no more events can be added to the queue.
     */
    final AtomicInteger inFlightAppends = new AtomicInteger(0);

    Worker worker = new Worker();

    /**
     * The default maximum queue flush time allowed during appender stop. If the
     * worker takes longer than this time it will exit, discarding any remaining
     * items in the queue
     */
    public static final int DEFAULT_MAX_FLUSH_TIME = 1000;
    int maxFlushTime = DEFAULT_MAX_FLUSH_TIME;

    /**
     * Is the eventObject passed as parameter discardable? The base class's
     * implementation of this method always returns 'false' but sub-classes may (and
     * do) override this method.
     * <p>
     * Note that only if the buffer is nearly full are events discarded. Otherwise,
     * when the buffer is "not full" all events are logged.
     *
     * @param eventObject
     * @return - true if the event can be discarded, false otherwise
     */
    protected boolean isDiscardable(E eventObject) {
        return false;
    }

    /**
     * Pre-process the event prior to queueing. The base class does no
     * pre-processing but subclasses can override this behavior.
     *
     * @param eventObject
     */
    protected void preprocess(E eventObject) {
    }

    @Override
    public void start() {
        if (isStarted())
            return;
        if (appenderCount == 0) {
            addError("No attached appenders found.");
            return;
        }
        if (queueSize < 1) {
            addError("Invalid queue size [" + queueSize + "]");
            return;
        }
        blockingQueue = new ArrayBlockingQueue<E>(queueSize);

        if (discardingThreshold == UNDEFINED)
            discardingThreshold = queueSize / 5;
        addInfo("Setting discardingThreshold to " + discardingThreshold);
        worker.setDaemon(true);
        worker.setName("AsyncAppender-Worker-" + getName());
        // make sure this instance is marked as "started" before starting the
        // worker Thread
        super.start();
        worker.start();
    }

    @Override
    public void stop() {
        if (!isStarted())
            return;

        // mark this appender as stopped so that Worker can also processPriorToRemoval
        // if it is invoking
        // aii.appendLoopOnAppenders
        // and sub-appenders consume the interruption
        super.stop();

        // interrupt the worker thread so that it can terminate. Note that the
        // interruption can be consumed by sub-appenders
        worker.interrupt();

        InterruptUtil interruptUtil = new InterruptUtil(context);

        try {
            interruptUtil.maskInterruptFlag();

            worker.join(maxFlushTime);

            // check to see if the thread ended and if not add a warning message
            if (worker.isAlive()) {
                int remaining = blockingQueue.size();
                addWarn("Max queue flush timeout (" + maxFlushTime + " ms) exceeded. " + remaining
                        + " queued events could not be flushed in time. The worker thread will continue flushing them in the background.");
            } else {
                addInfo("Queue flush finished successfully within timeout.");
            }

        } catch (InterruptedException e) {
            int remaining = blockingQueue.size();
            addError("Failed to join worker thread. " + remaining + " queued events may be discarded.", e);
        } finally {
            interruptUtil.unmaskInterruptFlag();
        }
    }

    @Override
    protected void append(E eventObject) {
        if (isQueueBelowDiscardingThreshold() && isDiscardable(eventObject)) {
            return;
        }
        preprocess(eventObject);
        put(eventObject);
    }

    /**
     * An event is considered accepted by this appender once this method
     * observes the appender as started. In-flight invocations are counted
     * <em>before</em> the started-check performed in
     * {@link UnsynchronizedAppenderBase#doAppend(Object)}, so that during stop
     * the worker thread can wait for events which passed the check but have
     * not been enqueued yet. Events arriving after the appender is marked as
     * stopped are rejected by the started-check in the super class and are
     * therefore never accepted.
     */
    @Override
    public void doAppend(E eventObject) {
        inFlightAppends.incrementAndGet();
        try {
            super.doAppend(eventObject);
        } finally {
            inFlightAppends.decrementAndGet();
        }
    }

    public boolean isQueueBelowDiscardingThreshold() {
        return (blockingQueue.remainingCapacity() < discardingThreshold);
    }

    protected void put(E eventObject) {
        if (neverBlock) {
            blockingQueue.offer(eventObject);
        } else {
            putUninterruptibly(eventObject);
        }
    }

    private void putUninterruptibly(E eventObject) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    blockingQueue.put(eventObject);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public int getQueueSize() {
        return queueSize;
    }

    public void setQueueSize(int queueSize) {
        this.queueSize = queueSize;
    }

    public int getDiscardingThreshold() {
        return discardingThreshold;
    }

    public void setDiscardingThreshold(int discardingThreshold) {
        this.discardingThreshold = discardingThreshold;
    }

    public int getMaxFlushTime() {
        return maxFlushTime;
    }

    public void setMaxFlushTime(int maxFlushTime) {
        this.maxFlushTime = maxFlushTime;
    }

    /**
     * Returns the number of elements currently in the blocking queue.
     *
     * @return number of elements currently in the queue.
     */
    public int getNumberOfElementsInQueue() {
        return blockingQueue.size();
    }

    public void setNeverBlock(boolean neverBlock) {
        this.neverBlock = neverBlock;
    }

    public boolean isNeverBlock() {
        return neverBlock;
    }

    /**
     * The remaining capacity available in the blocking queue.
     * <p>
     * See also {@link java.util.concurrent.BlockingQueue#remainingCapacity()
     * BlockingQueue#remainingCapacity()}
     *
     * @return the remaining capacity
     *
     */
    public int getRemainingCapacity() {
        return blockingQueue.remainingCapacity();
    }

    public void addAppender(Appender<E> newAppender) {
        if (appenderCount == 0) {
            appenderCount++;
            addInfo("Attaching appender named [" + newAppender.getName() + "] to AsyncAppender.");
            aai.addAppender(newAppender);
        } else {
            addWarn("One and only one appender may be attached to AsyncAppender.");
            addWarn("Ignoring additional appender named [" + newAppender.getName() + "]");
        }
    }

    public Iterator<Appender<E>> iteratorForAppenders() {
        return aai.iteratorForAppenders();
    }

    public Appender<E> getAppender(String name) {
        return aai.getAppender(name);
    }

    public boolean isAttached(Appender<E> eAppender) {
        return aai.isAttached(eAppender);
    }

    public void detachAndStopAllAppenders() {
        aai.detachAndStopAllAppenders();
    }

    public boolean detachAppender(Appender<E> eAppender) {
        return aai.detachAppender(eAppender);
    }

    public boolean detachAppender(String name) {
        return aai.detachAppender(name);
    }

    class Worker extends Thread {

        public void run() {
            AsyncAppenderBase<E> parent = AsyncAppenderBase.this;
            AppenderAttachableImpl<E> aai = parent.aai;

            // loop while the parent is started
            while (parent.isStarted()) {
                try {
                    List<E> elements = new ArrayList<E>();
                    E e0 = parent.blockingQueue.take();
                    elements.add(e0);
                    parent.blockingQueue.drainTo(elements);
                    for (E e : elements) {
                        aai.appendLoopOnAppenders(e);
                    }
                } catch (InterruptedException e1) {
                    // exit if interrupted
                    break;
                }
            }

            addInfo("Worker thread will flush remaining events before exiting. ");

            // The interruption that made this thread exit the loop above may
            // still be reflected in the interrupt status. Clear it so that it
            // does not interfere with the flushing below.
            Thread.interrupted();

            parent.flushRemainingEvents(aai);

            aai.detachAndStopAllAppenders();
        }
    }

    /**
     * Flush the events remaining in the queue. Called by the worker thread
     * after its main loop has exited, i.e. after this appender was marked as
     * stopped.
     * <p>
     * In addition to events already queued, events of in-flight
     * {@link #doAppend(Object)} invocations are waited for and flushed as
     * well: such invocations entered before the appender was marked as
     * stopped and may not have enqueued their event yet. No lock is held
     * while invoking the attached appenders, so logging performed by the
     * attached appenders themselves (which may be routed back to this
     * appender, where it is rejected by the not-started check) cannot
     * deadlock the flush.
     */
    private void flushRemainingEvents(AppenderAttachableImpl<E> aai) {
        while (true) {
            E e = blockingQueue.poll();
            if (e != null) {
                aai.appendLoopOnAppenders(e);
                continue;
            }
            // The queue is empty. An in-flight doAppend() invocation may still
            // enqueue an event, so flushing is only complete once no such
            // invocation remains. The queue is polled a second time to catch
            // events enqueued between the first poll and the in-flight check.
            if (inFlightAppends.get() == 0 && blockingQueue.poll() == null) {
                return;
            }
            try {
                Thread.sleep(1);
            } catch (InterruptedException ie) {
                // Interruption must not cause remaining events to be
                // abandoned. Keep flushing.
            }
        }
    }
}
