package com.thesis.flink.nexmark;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class BidDeserializationSchema implements DeserializationSchema<Bid> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Bid deserialize(byte[] message) throws IOException {
        mapper.registerModule(new JavaTimeModule());
        return mapper.readValue(message, Bid.class);
    }

    @Override
    public boolean isEndOfStream(Bid nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Bid> getProducedType() {
        return TypeInformation.of(Bid.class);
    }
}
