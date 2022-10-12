package apoc.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.TransactionFailureException;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static apoc.util.MapUtil.map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class TransactionTestUtil {
    public static void checkTerminationGuard(GraphDatabaseService db, String query) {
        checkTerminationGuard(db, query, Collections.emptyMap());
    }
    
    public static void checkTerminationGuard(GraphDatabaseService db, String query, Map<String, Object> params) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Callable<String> callable = () -> db.executeTransactionally(query, params, Result::resultAsString);

        Future<String> future = executor.submit(callable);

        // waiting for apoc query to cancel when it is found
        final String transactionId = TestUtil.singleResultFirstColumn(db,
                "SHOW TRANSACTIONS YIELD currentQuery, transactionId WHERE currentQuery = $query RETURN transactionId",
                map("query", query));

        assertEventually(() -> db.executeTransactionally("TERMINATE TRANSACTION $transactionId",
                map("transactionId", transactionId),
                result -> {
                    final ResourceIterator<String> msgIterator = result.columnAs("message");
                    return msgIterator.hasNext() && msgIterator.next().equals("Transaction terminated.");
                }), (value) -> value, 10L, TimeUnit.SECONDS);

        // checking for query cancellation
        assertEventually(() -> {
            final String transactionListCommand = "SHOW TRANSACTIONS";
            return db.executeTransactionally(transactionListCommand,
                    map("query", query),
                    result -> {
                        final ResourceIterator<String> queryIterator = result.columnAs("currentQuery");
                        final String first = queryIterator.next();
                        return first.equals(transactionListCommand) && !queryIterator.hasNext();
                    } );
        }, (value) -> value, 10L, TimeUnit.SECONDS);

        // check that the procedure/function fails with TransactionFailureException when transaction is terminated
        try {
            future.get(10L, TimeUnit.SECONDS);
            fail("Should fail because of TransactionFailureException");
        } catch (ExecutionException e) {
            final Throwable rootCause = ExceptionUtils.getRootCause(e);
            assertTrue(rootCause instanceof TransactionFailureException);
            final String expected = "The transaction has been terminated. " +
                    "Retry your operation in a new transaction, and you should see a successful result. Explicitly terminated by the user. ";
            assertEquals(expected, rootCause.getMessage());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
