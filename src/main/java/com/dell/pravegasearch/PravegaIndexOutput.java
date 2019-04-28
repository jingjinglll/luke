package com.dell.pravegasearch;


import org.apache.lucene.store.IndexOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

public class PravegaIndexOutput extends IndexOutput {

    private ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    private long pos = 0;
    private final Checksum digest = new CRC32();
    private String name;

    protected PravegaIndexOutput(String name) {
        super("PravegaIndexOutput(name=\"" + name + "\")", name);
        this.name = name;
    }

    @Override
    public void close() throws IOException {
        byte[] content = outputStream.toByteArray();
    }

    @Override
    public long getFilePointer() {
        return pos;
    }

    @Override
    public long getChecksum() throws IOException {
        return digest.getValue();
    }

    @Override
    public void writeByte(byte b) throws IOException {
        outputStream.write(b);
        digest.update(b);
        pos++;
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length) throws IOException {
        outputStream.write(b, offset, length);
        digest.update(b, offset, length);
        pos += length;
    }
}
