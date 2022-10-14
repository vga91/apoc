package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.graph.Graphs;
import apoc.meta.Meta;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Map;
import java.util.stream.IntStream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.apocConfig;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TransactionTestUtil.checkTerminationGuard;
import static org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation.OFF_HEAP;
import static org.neo4j.configuration.SettingValueParsers.BYTES;

public class ExportBigGraphTest {
    private static File directory = new File("target/import");
    static { //noinspection ResultOfMethodCallIgnored
        directory.mkdirs();
    }
    
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.memory_tracking, true)
            .withSetting(GraphDatabaseSettings.tx_state_memory_allocation, OFF_HEAP)
            .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("200m"))
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, ExportCSV.class, Graphs.class, Meta.class, ImportCsv.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);



        final String query = Util.readResourceFile("movies.cypher");
        IntStream.range(0, 25000).forEach(__-> db.executeTransactionally(query));
        
        
//        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
//        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    @Test
    public void testExportInvalidQuoteValue1() throws Exception {
//        final String query = Util.readResourceFile("movies.cypher");
//        IntStream.range(0, 20000).forEach(__-> db.executeTransactionally(query));
        String fileName = "allEEEE.csv";
        System.out.println("im here");
//        final long l = System.currentTimeMillis();
//        testCall(db, "CALL apoc.export.csv.all($file,{})", Map.of("file", fileName), r -> {
//            System.out.println("r" + r.values());
//        });
//        System.out.println("time=" + (System.currentTimeMillis() - l));
        checkTerminationGuard(db, "CALL apoc.export.csv.all($file,{})", Map.of("file", fileName));
//        try {
//            TestUtil.testCall(db, "CALL apoc.export.csv.all($file,{})",
//                    map("file", fileName),
//                    (r) -> assertResults(fileName, r, "database"));
//            fail();
//        } catch (RuntimeException e) {
//            final String expectedMessage = "Failed to invoke procedure `apoc.export.csv.all`: Caused by: java.lang.RuntimeException: The string value of the field quote is not valid";
//            assertEquals(expectedMessage, e.getMessage());
//        }
    }
}
