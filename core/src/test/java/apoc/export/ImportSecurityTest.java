package apoc.export;

import apoc.ApocConfig;
import apoc.export.csv.ImportCsv;
import apoc.export.graphml.ExportGraphML;
import apoc.export.json.ImportJson;
import apoc.load.LoadJson;
import apoc.load.Xml;
import apoc.util.SensitivePathGenerator;
import apoc.util.TestUtil;
import com.ctc.wstx.exc.WstxUnexpectedCharException;
import com.fasterxml.jackson.core.JsonParseException;
import com.nimbusds.jose.util.Pair;
import inet.ipaddr.IPAddressString;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.export.SecurityUtil.ALLOWED_EXCEPTIONS;
import static apoc.export.SecurityUtil.IMPORT_PROCEDURES;
import static apoc.export.SecurityUtil.LOAD_PROCEDURES;
import static apoc.util.FileTestUtil.createTempFolder;
import static apoc.util.FileTestUtil.setAllowReadFromFs;
import static apoc.util.FileTestUtil.setFileImport;
import static apoc.util.FileTestUtil.setUseNeo4jConfig;
import static apoc.util.FileUtils.ACCESS_OUTSIDE_DIR_ERROR;
import static apoc.util.FileUtils.ERROR_READ_FROM_FS_NOT_ALLOWED;
import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class ImportSecurityTest {

    private static final Path import_folder = createTempFolder();
    // todo - getRight????
    private static final String fileName = SensitivePathGenerator.etcPasswd().getLeft();
    private static final String fileName2 = SensitivePathGenerator.etcPasswd().getRight();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, import_folder)
            .withSetting(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString("127.168.0.0/8")));


    private final String apocProcedure;
    private final String exportMethod;

    public ImportSecurityTest(String method, String methodArguments) {
        this.apocProcedure = "CALL " + method + methodArguments;
        this.exportMethod = method;
    }

    @BeforeClass
    public static void setUp() throws IOException {
        FileUtils.write(new File("target/import/test.file"),
                "test\ncsv",
                Charset.defaultCharset());

        TestUtil.registerProcedure(db,
                ImportJson.class, Xml.class, ImportCsv.class, ExportGraphML.class,
                LoadJson.class, Xml.class);
    }

    @Parameterized.Parameters
    public static Collection<String[]> data() {
        List<String[]> importAndLoadProcedures = IMPORT_PROCEDURES
                .map(e -> new String[]{ "apoc.import." + e.getLeft() , e.getRight() })
                .collect(Collectors.toList());

        List<String[]> loadProcedures = LOAD_PROCEDURES
                .map(e -> new String[]{ "apoc.load." + e.getLeft() , e.getRight() })
                .toList();

        importAndLoadProcedures.addAll(loadProcedures);
        return importAndLoadProcedures;
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled1() {
        setFileApocConfs(false, false, false);
        exportDisabled();
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled2() {
        setFileApocConfs(false, true, false);
        exportDisabled();
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled3() {
        setFileApocConfs(false, false, true);
        exportDisabled();
    }

    @Test
    public void testIllegalFSAccessWithImportDisabled34() {
        setFileApocConfs(false, true, true);
        exportDisabled();
    }

    private void exportDisabled() {
        extracted(fileName, ApocConfig.LOAD_FROM_FILE_ERROR, RuntimeException.class);
    }

    @Test
    public void testIllegalFSAccessWithImportEnabled() {
        setFileApocConfs(true, true, false);
        extracted(fileName);
    }

    @Test
    public void testIllegalFSAccessWithImportEnabled2() {
        setFileApocConfs(true, true, false);
        extracted(fileName2);
    }

    private void extracted(String fileName, String expectedError, Class exceptionClass) {
        final String message = apocProcedure + " should throw an exception";

        try {
            db.executeTransactionally(apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
            fail(message);
        } catch (Exception e) {
            TestUtil.assertError(e, expectedError, exceptionClass, apocProcedure);
        }
    }

    private void extracted(String fileName) {
        extracted(fileName,
                String.format(ERROR_READ_FROM_FS_NOT_ALLOWED, fileName), RuntimeException.class);
    }

    @Test
    public void testIllegalFSAccessWithImportEnabled222() {
        setFileApocConfs(true, true, true);
        extracted124(fileName);
    }

    @Test
    public void testIllegalFSAccessWithImportEnabledUseConfsTrueAndAllowFromFsTrue1() {
        setFileApocConfs(true, true, true);
        extracted12(fileName2);
    }

    @Test
    public void testIllegalFSAccessWithImportEnabledUseConfsTrueAndAllowFromFsFalse() {
        setFileApocConfs(true, false, true);
        shouldRead(fileName);
    }

    @Test
    public void testReadSensitiveFileWorks22() {
        setFileApocConfs(true, false, true);
        shouldRead(fileName2);
    }

    @Test
    public void testReadSensitiveFileWorks222222() {
        setFileApocConfs(true, false, false);
        shouldRead(fileName);
    }

    @Test
    public void testReadSensitiveFileWorks2222() {
        setFileApocConfs(true, false, false);
        shouldRead(fileName2);
    }

    @Test
    public void testIllegalFSAccessWithImportEnabledUseConfsTrueAndAllowFromFsTrueee1() {
        setFileApocConfs(true, true, true);
        extracted();
    }

    @Test
    public void testIllegalFSAccessWithImportEnabledUseConfsTrueAndAllowFromFsTrue111() {
        setFileApocConfs(true, false, false);
        extracted();
    }

    private void extracted() {
        var protocols = List.of("https", "http", "ftp");

        for (var protocol: protocols) {
            String url = String.format("%s://127.168.0.0/test.file", protocol);
            QueryExecutionException e = assertThrows(QueryExecutionException.class,
                    () -> testCall(db,
                            apocProcedure,
                            Map.of("fileName", url),
                            (r) -> {}
                    )
            );
            assertTrue(e.getMessage().contains("access to /127.168.0.0 is blocked via the configuration property internal.dbms.cypher_ip_blocklist"));
        }
    }

    private void shouldRead(String fileName) {
        try {
            db.executeTransactionally(apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
        } catch (Exception e) {
            if (ALLOWED_EXCEPTIONS.containsKey(exportMethod)) {
                assertEquals(ALLOWED_EXCEPTIONS.get(exportMethod), ExceptionUtils.getRootCause(e).getClass());
            }
        }
    }

    private void extracted124(String fileName) {
        extracted(fileName, ACCESS_OUTSIDE_DIR_ERROR, IOException.class);
    }

    private void extracted12(String fileName) {
        try {
            db.executeTransactionally(apocProcedure,
                    Map.of("fileName", fileName),
                    Result::resultAsString);
        } catch (Exception e) {
            String absolutePath = new File(import_folder.toFile(), fileName).getAbsolutePath();
            TestUtil.assertError(e, "Cannot open file " + absolutePath + " for reading.", IOException.class, apocProcedure);
        }
    }

    // todo - common
    private void setFileApocConfs(boolean importEnabled, boolean useNeo4jConfs, boolean allowReadFromFs) {
        setFileImport(importEnabled);
        setUseNeo4jConfig(useNeo4jConfs);
        setAllowReadFromFs(allowReadFromFs);
    }

}
