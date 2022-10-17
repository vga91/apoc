package apoc.load;

import apoc.util.TestUtil;
import com.github.stefanbirkner.fakesftpserver.rule.FakeSftpServerRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;

public class SftpLoadTest {
    private static final String USERNAME = "admin";
    private static final String PASSWORD = "password";
    private static final String JSON_FILE_NAME = "/subdir/sample.json";
    private static final String XML_FILE_NAME = "/subdir/sample.xml";
    
    private static String HOST;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @ClassRule
    public static final FakeSftpServerRule sftpServer = new FakeSftpServerRule()
            .addUser(USERNAME, PASSWORD);

    @BeforeClass
    public static void beforeClass() throws IOException {
        sftpServer.putFile(JSON_FILE_NAME, "{a: 1}", StandardCharsets.UTF_8);
        sftpServer.putFile(XML_FILE_NAME, "<catalog>1</catalog>", StandardCharsets.UTF_8);
        TestUtil.registerProcedure(db, LoadJson.class, Xml.class);

        HOST = String.format("sftp://%s:%s@localhost:%s",
                USERNAME, PASSWORD, sftpServer.getPort());
    }

    @Test
    public void testLoadJsonFtp() {
        testCall(db, "CALL apoc.load.json($url)",
                map("url", HOST + JSON_FILE_NAME),
                (row) -> assertEquals(Map.of("a", 1L), row.get("value")));
    }

    @Test
    public void testLoadXmlFtp()  {
        testCall(db, "CALL apoc.load.xml($url)",
                map("url", HOST + XML_FILE_NAME),
                (row) -> assertEquals(Map.of("_type", "catalog", "_text", "1"), row.get("value")));
    }
}
