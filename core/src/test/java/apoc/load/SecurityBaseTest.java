package apoc.load;

import apoc.util.SensitivePathGenerator;
import inet.ipaddr.IPAddressString;
import org.junit.ClassRule;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static apoc.util.FileTestUtil.createTempFolder;

public abstract class SecurityBaseTest {
    public static final Path import_folder = createTempFolder();
    // todo - getRight????
    public static final String fileName = SensitivePathGenerator.etcPasswd().getLeft();
    public static final String fileName2 = SensitivePathGenerator.etcPasswd().getRight();

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule()
            .withSetting(GraphDatabaseSettings.load_csv_file_url_root, import_folder)
            .withSetting(GraphDatabaseInternalSettings.cypher_ip_blocklist, List.of(new IPAddressString("127.168.0.0/8")));


    public static Collection<String[]> data(Map<String, List<String>> APOC_PROCEDURE_WITH_ARGUMENTS) {
        return APOC_PROCEDURE_WITH_ARGUMENTS.entrySet()
                .stream()
                .flatMap(e -> e.getValue().stream().map(arg -> new String[]{ e.getKey(), arg }))
                .collect(Collectors.toList());
    }
}
