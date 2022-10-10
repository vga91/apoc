package apoc.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.exc.InputCoercionException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;

import java.io.IOException;

public class CustomNumberSerializer extends UntypedObjectDeserializer {

    public CustomNumberSerializer(JavaType listType, JavaType mapType) {
        super(listType, mapType);
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {

        if (p.hasToken(JsonToken.VALUE_NUMBER_FLOAT)) {

            // we convert it either to a double or plain string
            final double doubleValue = p.getDoubleValue();
            if (doubleValue != Double.POSITIVE_INFINITY && doubleValue != Double.NEGATIVE_INFINITY) {
                return doubleValue;
            }

            return p.getDecimalValue().toPlainString();
        }

        if (p.hasToken(JsonToken.VALUE_NUMBER_INT)) {

            try {
                return p.getLongValue();
            } catch (InputCoercionException e) {
                return p.getValueAsString();
            }
        }

        // fallback to standard deserialization
        return super.deserialize(p, ctxt);
    }
    
}
