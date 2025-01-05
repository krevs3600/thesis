package com.thesis.flink.nexmark;

import java.io.IOException;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import com.fasterxml.jackson.databind.ObjectMapper;

public class AuctionDeserializationSchema implements DeserializationSchema<Auction> {
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Auction deserialize(byte[] message) throws IOException {
        return mapper.readValue(message, Auction.class);
    }

    @Override
    public boolean isEndOfStream(Auction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Auction> getProducedType() {
        return TypeInformation.of(Auction.class);
    }
}