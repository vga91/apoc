package apoc.cypher;

import apoc.Pools;
import apoc.result.MapResult;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.kernel.impl.coreapi.TransactionImpl;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author mh
 * @since 20.02.18
 */
public class Timeboxed {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Context
    public Pools pools;

    @Context
    public TerminationGuard terminationGuard;

    private final static Map<String,Object> POISON = Collections.singletonMap("__magic", "POISON");

    @Procedure
    @Description("apoc.cypher.runTimeboxed('cypherStatement',{params}, timeout) - abort kernelTransaction after timeout ms if not finished")
    public Stream<MapResult> runTimeboxed(@Name("cypher") String cypher, @Name("params") Map<String, Object> params, @Name("timeout") long timeout) {

        final BlockingQueue<Map<String, Object>> queue = new ArrayBlockingQueue<>(100);
        final AtomicReference<Transaction> txAtomic = new AtomicReference<>();

        // run query to be timeboxed in a separate thread to enable proper tx termination
        // if we'd run this in current thread, a tx.terminate would kill the transaction the procedure call uses itself.
        pools.getDefaultExecutorService().submit(() -> {
            try (Transaction innerTx = db.beginTx()) {
                txAtomic.set(innerTx);
                Result result = innerTx.execute(cypher, params == null ? Collections.EMPTY_MAP : params);
                while (result.hasNext()) {
                    if (Util.transactionIsTerminated(terminationGuard)) {
                        // todo - fare una cosa del genere anche per periodic e tutte le altre con innerTx
                        txAtomic.get().close();
                        offerToQueue(queue, POISON, timeout);
                        return;
                    }


                    final Map<String, Object> map = result.next();
                    // todo - *NEXT (put the termination control??)
                    offerToQueue(queue, map, timeout);
                }
                // todo - *NEXT (put the termination control??)
                offerToQueue(queue, POISON, timeout);
                if (Util.transactionIsTerminated(terminationGuard)) {
                    // todo - fare una cosa del genere anche per periodic e tutte le altre con innerTx
                    txAtomic.get().terminate();
                    offerToQueue(queue, POISON, timeout);
                    return;
                }
                System.out.println("result poison = ");
                innerTx.commit();
            } catch (TransactionTerminatedException e) {
                log.warn("query " + cypher + " has been terminated");
            } finally {
                txAtomic.set(null);
            }
        });


        // todo  - some operations not allowed, maybe scheduleATFixedTask??? Or maybe put something in *NEXT, see above

        //
        pools.getScheduledExecutorService().schedule(() -> {
            Transaction tx = txAtomic.get();
            if (tx==null) {
                System.out.println("tx = " + tx);
                log.debug("tx is null, either the other transaction finished gracefully or has not yet been start.");
            } else {
                System.out.println("tx1 = " + tx);
                tx.close();
                offerToQueue(queue, POISON, timeout);
                log.warn("terminating transaction, putting POISON onto queue");
            }
        }, timeout, MILLISECONDS);

        // consume the blocking queue using a custom iterator finishing upon POISON
        Iterator<Map<String,Object>> queueConsumer = new Iterator<>() {
            Map<String, Object> nextElement = null;
            boolean hasFinished = false;

            @Override
            public boolean hasNext() {
                if (hasFinished) {
                    return false;
                } else {
                    System.out.println("Timeboxed.hasNext");
//                    terminationGuard.check();
                    if (Util.transactionIsTerminated(terminationGuard)) {
                        System.out.println("is terminated here");
                        // todo - fare una cosa del genere anche per periodic e tutte le altre con innerTx
                        final Transaction transaction = txAtomic.get();
//                        ((TransactionImpl) transaction).isOpen()
                        transaction.close();
//                        throw new RuntimeException("aaaa");
//                        offerToQueue(queue, POISON, timeout);
                        return false;
                    }

                    try {
                        nextElement = queue.poll(timeout, MILLISECONDS);
                        if (nextElement == null) {
                            log.warn("couldn't grab queue element, aborting - this should never happen");
                            hasFinished = true;
                        } else {
                            hasFinished = POISON.equals(nextElement);
                        }
                        return !hasFinished;
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            @Override
            public Map<String, Object> next() {
                return nextElement;
            }
        };
        return StreamSupport
                .stream( Spliterators.spliteratorUnknownSize(queueConsumer, Spliterator.ORDERED), false)
                .map(MapResult::new);
    }

    private void offerToQueue(BlockingQueue<Map<String, Object>> queue, Map<String, Object> map, long timeout)  {
        try {
            boolean hasBeenAdded = queue.offer(map, timeout, MILLISECONDS);
            if (!hasBeenAdded) {
                throw new IllegalStateException("couldn't add a value to a queue of size " + queue.size() + ". Either increase capacity or fix consumption of the queue");
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
