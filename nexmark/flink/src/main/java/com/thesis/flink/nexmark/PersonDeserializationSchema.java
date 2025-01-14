package com.thesis.flink.nexmark;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class PersonDeserializationSchema implements DeserializationSchema<Person> {
    private static final ObjectMapper mapper = new ObjectMapper();
    

    @Override
    public Person deserialize(byte[] message) throws IOException {
        mapper.registerModule(new JavaTimeModule());
        return mapper.readValue(message, Person.class);
    }

    @Override
    public boolean isEndOfStream(Person nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Person> getProducedType() {
        return TypeInformation.of(Person.class);
    }
}