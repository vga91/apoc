package apoc.load;

import apoc.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.util.Map;

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCall;
import static org.junit.Assert.assertEquals;


public class FtpLoadTest {
    private static final String PASSWORD = "password";
    private static final String USERNAME = "admin";
    private static final String JSON_FILE_NAME = "/subdir/sample.json";
    private static final String XML_FILE_NAME = "/subdir/sample.xml";
    private static String HOST;

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    private static FakeFtpServer ftpServer;


    @BeforeClass
    public static void beforeClass() {
        ftpServer = new FakeFtpServer();

        FileSystem fileSystem = new UnixFakeFileSystem();
        ftpServer.setFileSystem(fileSystem);

        UserAccount userAccount = new UserAccount(USERNAME, PASSWORD, "/");
        ftpServer.addUserAccount(userAccount);

        ftpServer.start();
        
        fileSystem.add(new FileEntry(JSON_FILE_NAME, "{a: 1}"));
        
        fileSystem.add(new FileEntry(XML_FILE_NAME, "<catalog>1</catalog>"));

        TestUtil.registerProcedure(db, LoadJson.class, Xml.class);

        HOST = String.format("ftp://%s:%s@localhost:%s",
                USERNAME, PASSWORD, ftpServer.getServerControlPort());
    }

    @AfterClass
    public static void afterClass()  {
        ftpServer.stop();
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
