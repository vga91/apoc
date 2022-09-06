package apoc.export.json;

import apoc.Pools;
import apoc.export.util.CountingReader;
import apoc.export.util.ProgressReporter;
import apoc.result.ProgressInfo;
import apoc.util.FileUtils;
import apoc.util.JsonUtil;
import apoc.util.Util;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;
import org.neo4j.procedure.TerminationGuard;

import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Stream;

public class ImportJson {
    @Context
    public GraphDatabaseService db;

    @Context
    public Pools pools;
    
    @Context
    public TerminationGuard terminationGuard;

    @Procedure(value = "apoc.import.json", mode = Mode.WRITE)
    @Description("apoc.import.json(urlOrBinaryFile,config) - imports the json list to the provided file")
    public Stream<ProgressInfo> all(@Name("urlOrBinaryFile") Object urlOrBinaryFile, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        ProgressInfo result =
                Util.inThread(pools, () -> {
                    ImportJsonConfig importJsonConfig = new ImportJsonConfig(config);
                    String file = null;
                    String source = "binary";
                    if (urlOrBinaryFile instanceof String) {
                        file =  (String) urlOrBinaryFile;
                        source = "file";
                    }
                    ProgressReporter reporter = new ProgressReporter(null, null, new ProgressInfo(file, source, "json"));

                    try (final CountingReader reader = FileUtils.readerFor(urlOrBinaryFile, importJsonConfig.getCompressionAlgo());
                         final Scanner scanner = new Scanner(reader).useDelimiter("\n|\r");
                         JsonImporter jsonImporter = new JsonImporter(importJsonConfig, db, reporter)) {
                        while (scanner.hasNext() && !Util.transactionIsTerminated(terminationGuard)) {
                            final String content = scanner.nextLine();
                            final Object line = JsonUtil.OBJECT_MAPPER.readValue(content, Object.class);
                            if (line instanceof Map) {
                                jsonImporter.importRow((Map<String, Object>) line);
                            } else if (line instanceof List) {
                                // ARRAY_JSON case
                                ((List<Map<String, Object>>) line).forEach(jsonImporter::importRow);
                            } else {
                                throw new RuntimeException("Unable to convert from JSON with content: " + content);
                            }
                        }
                    }

                    return reporter.getTotal();
                });
        return Stream.of(result);
    }
}
