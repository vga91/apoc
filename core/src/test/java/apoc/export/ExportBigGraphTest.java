package apoc.export;

import apoc.export.csv.ExportCSV;
import apoc.export.csv.ImportCsv;
import apoc.export.cypher.ExportCypher;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ExportJson;
import apoc.graph.Graphs;
import apoc.meta.Meta;
import apoc.refactor.GraphRefactoring;
import apoc.refactor.rename.Rename;
import apoc.util.TestUtil;
import apoc.util.Util;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.File;
import java.util.Collections;
import java.util.List;
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
            .withSetting(GraphDatabaseSettings.tx_state_max_off_heap_memory, BYTES.parse("500m"))
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, directory.toPath().toAbsolutePath());

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, Rename.class, ExportCSV.class, ExportJson.class, ExportCypher.class, ExportGraphML.class, Graphs.class, Meta.class, ImportCsv.class, GraphRefactoring.class);
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, true);
        apocConfig().setProperty(APOC_EXPORT_FILE_ENABLED, true);

        final String query = Util.readResourceFile("movies.cypher");
        IntStream.range(0, 25000).forEach(__-> db.executeTransactionally(query));
        
        
//        db.executeTransactionally("CREATE (f:User1:User {name:'foo',age:42,male:true,kids:['a','b','c']})-[:KNOWS]->(b:User {name:'bar',age:42}),(c:User {age:12})");
//        db.executeTransactionally("CREATE (f:Address1:Address {name:'Andrea', city: 'Milano', street:'Via Garibaldi, 7'})-[:NEXT_DELIVERY]->(a:Address {name: 'Bar Sport'}), (b:Address {street: 'via Benni'})");
    }

    @Test
    public void testTerminateExportCsv() throws Exception {
//                final long l = System.currentTimeMillis();
//        testCall(db, "CALL apoc.export.csv.all('testTerminate1.csv',{})", r -> {
//            System.out.println("r" + r.values());
//        });
//        System.out.println("time=" + (System.currentTimeMillis() - l));
        
        checkTerminationGuard(db, "CALL apoc.export.csv.all('testTerminate.csv',{})");
        
    }

    @Test
    public void testTerminateExportGraphMl() throws Exception {
        checkTerminationGuard(db, "CALL apoc.export.graphml.all('testTerminate.graphml', null)");
    }

    @Test
    public void testTerminateExportCypher() throws Exception {
//        testCall(db, "CALL apoc.export.cypher.all('testTerminate.cypher',{})", r -> {
//            System.out.println("r" + r.values());
//        });
        checkTerminationGuard(db, "CALL apoc.export.cypher.all('testTerminate.cypher',{})");
    }

    @Test
    public void testTerminateExportJson() throws Exception {
        checkTerminationGuard(db, "CALL apoc.export.json.all('testTerminate.json',{})");
    }

    @Test
    public void testTerminateRenameNodeProp() {
        checkTerminationGuard(db, "CALL apoc.refactor.rename.nodeProperty('name', 'nameTwo')");
    }

    @Test
    public void testTerminateRenameTypeProp() throws Exception {
        checkTerminationGuard(db, "CALL apoc.refactor.rename.typeProperty('roles', 'rolesTwo')");
    }

    @Test
    public void testTerminateRenameType() throws Exception {
        checkTerminationGuard(db, "CALL apoc.refactor.rename.type('DIRECTED', 'DIRECTED_TWO')");
    }

    @Test
    public void testTerminateRenameLabel() throws Exception {
        checkTerminationGuard(db, "CALL apoc.refactor.rename.label('Other', 'OtherTwo')");
    }

    @Test
    public void testTerminateCloneNodes() throws Exception {
        List<Node> nodes = db.executeTransactionally("MATCH (n:Person) RETURN collect(n) as nodes", Collections.emptyMap(),
                r -> r.<List<Node>>columnAs("nodes").next());
        
        checkTerminationGuard(db, "CALL apoc.refactor.cloneNodes($nodes)", 
                Map.of("nodes", nodes));

        checkTerminationGuard(db, "CALL apoc.refactor.cloneSubgraph($nodes)",
                Map.of("nodes", nodes));

        db.executeTransactionally("CREATE CONSTRAINT FOR (n:BornLabel) REQUIRE n.targetKey IS UNIQUE");
        checkTerminationGuard(db, "CALL apoc.refactor.categorize('id', 'SOMETHING', true, 'BornLabel', 'targetKey', [], 1)");
    }
}
