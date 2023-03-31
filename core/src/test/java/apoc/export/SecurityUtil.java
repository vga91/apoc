package apoc.export;

import com.ctc.wstx.exc.WstxUnexpectedCharException;
import com.fasterxml.jackson.core.JsonParseException;
import com.nimbusds.jose.util.Pair;
import org.xml.sax.SAXParseException;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public class SecurityUtil {
    public static final Map<String, Class<?>> ALLOWED_EXCEPTIONS = Map.of(
            "apoc.load.json", JsonParseException.class,
            "apoc.load.jsonArray", JsonParseException.class,
            "apoc.load.jsonParams", JsonParseException.class,
            "apoc.load.xml", SAXParseException.class,

            "apoc.import.json", JsonParseException.class,
            "apoc.import.csv", NoSuchElementException.class,
            "apoc.import.graphml", JsonParseException.class,
            "apoc.import.xml", WstxUnexpectedCharException.class
    );

    public static Stream<Pair<String, String>> IMPORT_PROCEDURES = Stream.of(
            Pair.of("json", "($fileName)"),
            Pair.of("csv", "([{fileName: $fileName, labels: ['Person']}], [], {})"), // this with not-allowed nodes
            Pair.of("csv", "([], [{fileName: $fileName, type: 'KNOWS'}], {})"), // this with not-allowed rels
            Pair.of("graphml", "($fileName, {})"),
            Pair.of("xml", "($fileName)")
    );

    public static Stream<Pair<String, String>> LOAD_PROCEDURES = Stream.of(
            Pair.of("json", "($fileName, '', {})"),
            Pair.of("jsonArray", "($fileName, '', {})"),
            Pair.of("jsonParams", "($fileName, {}, '')"),
            Pair.of("xml", "($fileName, '', {}, false)")
    );
}
