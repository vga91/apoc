package apoc.index;

import apoc.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static apoc.util.TestUtil.testResult;
import static java.util.Collections.emptyList;
import static org.junit.Assert.*;

/**
 * @author mh
 * @since 23.05.16
 */
public class
SchemaIndexTest {

    private static final String SCHEMA_DISTINCT_COUNT_ORDERED = """
            CALL apoc.schema.properties.distinctCount($label, $key)
            YIELD label, key, value, count
            RETURN * ORDER BY label, key, value""";
    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static List<String> personNames;
    private static List<String> personAddresses;
    private static List<Long> personAges;
    private static List<Long> personIds;
    private static final int firstPerson = 1;
    private static final int lastPerson = 200;

    @BeforeClass
    public static void setUp() throws Exception {
        TestUtil.registerProcedure(db, SchemaIndex.class);
        db.executeTransactionally("CREATE (city:City {name:'London'}) WITH city UNWIND range("+firstPerson+","+lastPerson+") as id CREATE (:Person {name:'name'+id, id:id, age:id % 100, address:id+'Main St.'})-[:LIVES_IN]->(city)");
        
        // dataset for fulltext / composite indexes
        db.executeTransactionally("""
                CREATE (:FullTextOne {prop1: "Michael", prop2: 111}),
                    (:FullTextOne {prop1: "AA", prop2: 1}),
                    (:FullTextOne {prop1: "EE", prop2: 111}),
                    (:FullTextOne {prop1: "Ryan", prop2: 1}),
                    (:FullTextOne {prop1: "UU", prop2: "Ryan"}),
                    (:FullTextOne {prop1: "Ryan", prop2: 1}),
                    (:FullTextOne {prop1: "Ryan", prop3: 'qwerty'}),
                    (:FullTexTwo {prop1: "Ryan"}),
                    (:FullTexTwo {prop1: "omega"}),
                    (:FullTexTwo {prop1: "Ryan", prop3: 'abcde'}),
                    (:SchemaTestTwo {prop1: 'a', prop2: 'bar'}),
                    (:SchemaTestTwo {prop1: 'b', prop2: 'foo'}),
                    (:SchemaTestTwo {prop1: 'c', prop2: 'bar'})
                    """);
        //
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.name)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.age)");
        db.executeTransactionally("CREATE INDEX FOR (n:Person) ON (n.address)");
        db.executeTransactionally("CREATE CONSTRAINT FOR (p:Person) REQUIRE p.id IS UNIQUE");
        db.executeTransactionally("CREATE INDEX FOR (n:Foo) ON (n.bar)");
        db.executeTransactionally("CREATE INDEX rel_range_index_name FOR ()-[r:KNOWS]-() ON (r.since)");
        db.executeTransactionally("CREATE (f:Foo {bar:'three'}), (f2a:Foo {bar:'four'}), (f2b:Foo {bar:'four'})");
        personIds = LongStream.range(firstPerson, lastPerson+1).boxed().collect(Collectors.toList());
        personNames = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> "name"+i).sorted().collect(Collectors.toList());
        personAddresses = IntStream.range(firstPerson, lastPerson+1).mapToObj(Integer::toString).map(i -> i+"Main St.").sorted().collect(Collectors.toList());
        personAges = IntStream.range(firstPerson, lastPerson+1)
                .map(i -> i % 100)
                .sorted()
                .distinct()
                .mapToObj(Long::new).collect(Collectors.toList());
        try (Transaction tx=db.beginTx()) {
            tx.schema().awaitIndexesOnline(2,TimeUnit.SECONDS);
            tx.commit();
        }
    }

    @Test
    public void testDistinctPropertiesOnFirstIndex() throws Exception {
        testCall(db,"CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person","key", "name"),
                (row) -> assertEquals(new HashSet<>(personNames), new HashSet<>((Collection<String>) row.get("value")))
        );
    }


    @Test(timeout = 5000L)
    public void testDistinctWithoutIndexWaitingShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne FOR (n:FullTextOne) ON EACH [n.prop1]");
        // executing the apoc.schema.properties.distinct without CALL db.awaitIndexes() will throw an "Index is still populating" exception
        
        db.executeTransactionally("CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "FullTextOne","key", "prop1"),
                Result::resultAsString,
                Duration.ofSeconds(10));

        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
    }
    
    @Test(timeout = 5000L)
    public void testDistinctWithVoidIndexShouldNotHangs() {
        db.executeTransactionally("create index for (n:VoidIndex) on (n.myProp)");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "VoidIndex", "key", "myProp"),
                row -> assertEquals(emptyList(), row.get("value"))
        );
    }

    @Test(timeout = 5000L)
    public void testDistinctWithCompositeIndexShouldNotHangs() throws Exception {
        db.executeTransactionally("create index EmptyLabel for (n:EmptyLabel) on (n.one)");
        db.executeTransactionally("create index EmptyCompositeLabel for (n:EmptyCompositeLabel) on (n.two, n.three)");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "EmptyLabel", "key", "one"),
                row -> assertEquals(emptyList(), row.get("value"))
        );

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "EmptyCompositeLabel", "key", "two"),
                row -> assertEquals(emptyList(), row.get("value"))
        );

        db.executeTransactionally("drop index EmptyLabel");
        db.executeTransactionally("drop index EmptyCompositeLabel");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithCompositeIndexWithMixedRepeatedProps() throws Exception {
        db.executeTransactionally("create index SchemaTestTwo for (n:SchemaTestTwo) on (n.prop1, n.prop2)");

        testResult(db, "CALL apoc.schema.properties.distinctCount($label, $key)",
                map("label", "SchemaTestTwo", "key", "prop2"),
                res -> {
                    assertDistinctCountProperties("SchemaTestTwo", "prop2", List.of("bar"), () -> 2L, res);
                    assertDistinctCountProperties("SchemaTestTwo", "prop2", List.of("foo"), () -> 1L, res);
                    assertFalse(res.hasNext());
                });

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "SchemaTestTwo", "key", "prop2"),
                row -> assertEquals(Set.of("bar", "foo"), Set.copyOf((List)row.get("value")))
        );

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label","","key",""),
                (result) -> {
                    extractedFoo(result);
                    extractedPerson(result);
                    extractedSchemaTestTwo(result);
                    assertFalse(result.hasNext());
                });

        String label = "SchemaTestTwo";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY key,value",
                map("label",label,"key",""),
                (result) -> {
                    extractedSchemaTestTwo(result);
                    assertFalse(result.hasNext());
                });

        db.executeTransactionally("drop index SchemaTestTwo");
    }

    private void extractedFullTextFullTextOneProp1(Result res) {
        assertDistinctCountProperties("FullTextOne", "prop1", List.of("AA", "EE", "Michael"), () -> 1L, res);
        assertDistinctCountProperties("FullTextOne", "prop1", List.of("Ryan"), () -> 3L, res);
        assertDistinctCountProperties("FullTextOne", "prop1", List.of("UU"), () -> 1L, res);
    }
    
    private void extractedFullTextFullTextOneProp3(Result res) {
        assertDistinctCountProperties("FullTextOne", "prop3", List.of("qwerty"), () -> 1L, res);
    }

    private void extractedFullTextFullTexTwo(Result res) {
//        assertDistinctCountProperties("FullTexTwo", "prop1", List.of("AA"), () -> 1L, res);
//        assertDistinctCountProperties("FullTexTwo", "prop1", List.of("omega"), () -> 1L, res);
        assertDistinctCountProperties("FullTexTwo", "prop1", List.of("Ryan"), () -> 1L, res);
    }

    private void extractedSchemaTestTwo(Result result) {
        assertDistinctCountProperties("SchemaTestTwo", "prop1", List.of("a", "b", "c"), () -> 1L, result);
        assertDistinctCountProperties("SchemaTestTwo", "prop2", List.of("bar"), () -> 2L, result);
        assertDistinctCountProperties("SchemaTestTwo", "prop2", List.of("foo"), () -> 1L, result);
    }

    private void extractedFoo(Result result) {
        assertDistinctCountProperties("Foo", "bar", List.of("four"), () -> 2L, result);
        assertDistinctCountProperties("Foo", "bar", List.of("three"), () -> 1L, result);
    }

    private void extractedPerson(Result result) {
        assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
        assertDistinctCountProperties("Person", "age", personAges, () -> 2L, result);
        assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
        assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
    }

    @Test(timeout = 5000L)
    public void testDistinctWithFullTextIndexShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne23 FOR (n:FullTextOne) ON EACH [n.prop1]");

        db.executeTransactionally("CALL db.awaitIndexes()");

        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "FullTextOne", "key", "prop1"), 
                row -> assertEquals(Set.of("AA", "EE", "UU", "Ryan", "Michael"), Set.copyOf((List)row.get("value"))) 
        );
        
        testResult(db, "CALL apoc.schema.properties.distinctCount($label, $key)",
                map("label", "FullTextOne", "key", "prop1"),
                res -> {
                    extractedFullTextFullTextOneProp1(res);
                    assertFalse(res.hasNext());
                });

        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithMultiLabelFullTextIndexShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE FULLTEXT INDEX fulltextFullTextOne FOR (n:FullTextOne|FullTexTwo) ON EACH [n.prop1,n.prop3]");
        db.executeTransactionally("CREATE RANGE INDEX FOR (n:One) ON (n.prop1)");
        
        db.executeTransactionally("CALL db.awaitIndexes");

//        testCall(db, "CALL apoc.schema.properties.distinct($label, $key)",
//                map("label", "FullTextOne", "key", "prop1"),
//                row -> assertEquals(Set.of("AA", "EE", "UU", "Ryan", "Michael"), Set.copyOf((List)row.get("value")))
//        );
  
  
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label", "FullTextOne", "key", "prop1"),
                res -> {
                    extractedFullTextFullTextOneProp1(res);
                    assertFalse(res.hasNext());
                });

        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label","","key",""),
                (result) -> {
                    extractedFoo(result);
                    assertDistinctCountProperties("FullTexTwo", "prop1", List.of("Ryan"), () -> 2L, result);
                    assertDistinctCountProperties("FullTexTwo", "prop1", List.of("omega"), () -> 1L, result);
                    assertDistinctCountProperties("FullTexTwo", "prop3", List.of("abcde"), () -> 1L, result);
                    extractedFullTextFullTextOneProp1(result);
                    extractedFullTextFullTextOneProp3(result);
                    extractedPerson(result);
                    assertFalse(result.hasNext());
                });

        String label = "FullTextOne";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY key,value",
                map("label",label,"key",""),
                (result) -> {
                    extractedFullTextFullTextOneProp1(result);
                    assertFalse(result.hasNext());
                });
        
        db.executeTransactionally("DROP INDEX fulltextFullTextOne");
    }

    @Test(timeout = 5000L)
    public void testDistinctWithNoPreviousNodesShouldNotHangs() throws Exception {
        db.executeTransactionally("CREATE INDEX FOR (n:LabelNotExistent) ON n.prop");
        
        testCall(db, """
                        CREATE (:LabelNotExistent {prop:2})
                        WITH *
                        CALL apoc.schema.properties.distinct("LabelNotExistent", "prop")
                        YIELD value RETURN *""", 
                r -> assertEquals(emptyList(), r.get("value"))
        );
    }

    @Test
    public void testDistinctPropertiesOnSecondIndex() throws Exception {
        testCall(db,"CALL apoc.schema.properties.distinct($label, $key)",
                map("label", "Person","key", "address"),
                (row) -> assertEquals(new HashSet<>(personAddresses), new HashSet<>((Collection<String>) row.get("value")))
        );
    }

    @Test
    public void testDistinctCountPropertiesOnFirstIndex() throws Exception {
        String label = "Person";
        String key = "name";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
        });
    }

    @Test
    public void testDistinctCountPropertiesOnSecondIndex() throws Exception {
        String label = "Person";
        String key = "address";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label",label,"key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabel() throws Exception {
        String key = "name";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY value",
                map("label","","key",key),
                (result) -> {
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyKey() throws Exception {
        String label = "Person";
        testResult(db,"CALL apoc.schema.properties.distinctCount($label, $key) YIELD label,key,value,count RETURN * ORDER BY key,value",
                map("label",label,"key",""),
                (result) -> {
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertDistinctCountProperties("Person", "age", personAges, () -> 2L, result);
                    assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    @Test
    public void testDistinctCountPropertiesOnEmptyLabelAndEmptyKey() throws Exception {
        testResult(db, SCHEMA_DISTINCT_COUNT_ORDERED,
                map("label","","key",""),
                (result) -> {
                    assertTrue(result.hasNext());
                    assertEquals(map("label","Foo","key","bar","value","four","count",2L),result.next());
                    assertEquals(map("label","Foo","key","bar","value","three","count",1L),result.next());
                    assertDistinctCountProperties("Person", "address", personAddresses, () -> 1L, result);
                    assertDistinctCountProperties("Person", "age", personAges, () -> 2L, result);
                    assertDistinctCountProperties("Person", "id", personIds, () -> 1L, result);
                    assertDistinctCountProperties("Person", "name", personNames, () -> 1L, result);
                    assertFalse(result.hasNext());
                });
    }

    private <T> void assertDistinctCountProperties(String label, String key, Collection<T> values, Supplier<Long> counts, Result result) {
        Iterator<T> valueIterator = values.iterator();

        while (valueIterator.hasNext()) {
            assertTrue(result.hasNext());
            Map<String,Object> map = result.next();
            assertEquals(label, map.get("label"));
            assertEquals(key, map.get("key"));
            assertEquals(valueIterator.next(), map.get("value"));
            assertEquals(counts.get(), map.get("count"));
        }
    }
}
