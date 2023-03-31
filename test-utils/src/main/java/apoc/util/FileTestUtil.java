package apoc.util;

import apoc.ApocConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;
import static org.junit.Assert.assertEquals;

public class FileTestUtil {
    
    public static void assertStreamEquals(File directoryExpected, String fileName, String actualText) {
        String expectedText = TestUtil.readFileToString(new File(directoryExpected, fileName));
        String[] actualArray = actualText.split("\n");
        String[] expectArray = expectedText.split("\n");
        assertEquals(expectArray.length, actualArray.length);
        for (int i = 0; i < actualArray.length; i++) {
            assertEquals(JsonUtil.parse(expectArray[i],null, Object.class), JsonUtil.parse(actualArray[i],null, Object.class));
        }
    }

    public static Path createTempFolder() {
        try {
            return Files.createTempDirectory(UUID.randomUUID().toString());
//            return File.createTempFile(UUID.randomUUID().toString(), "tmp")
//                    .getParentFile().toPath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setFileImport(boolean allowed) {
        apocConfig().setProperty(ApocConfig.APOC_IMPORT_FILE_ENABLED, allowed);
    }

    public static void setUseNeo4jConfig(boolean allowed) {
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, allowed);
    }

    public static void setAllowReadFromFs(boolean allowed) {
        apocConfig().setProperty(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, allowed);
    }
}
