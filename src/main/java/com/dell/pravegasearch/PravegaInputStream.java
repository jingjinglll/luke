package com.dell.pravegasearch;


import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;

public class PravegaInputStream extends IndexInput {

    private int pos = 0;
    private ByteBuffer buffer;

    protected PravegaInputStream(String name, LuceneFileStore client) throws IOException {
        super("PravegaInputStream(name=" + name + ")");
        byte[] content;
        if (client.isSegFile(name)) {
            content = client.read(name);
        } else {
            content = client.receiveAsBytes(name);
        }
        buffer = content != null && content.length > 0 ? ByteBuffer.wrap(content) : ByteBuffer.allocate(0);
    }

    @Override
    public void close() throws IOException {
        this.buffer = null;
    }

    @Override
    public long getFilePointer() {
        return this.pos;
    }

    @Override
    public void seek(long pos) throws IOException {
        this.pos = (int) pos;
    }

    @Override
    public long length() {
        return buffer.limit();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new MemIndexInput(sliceDescription, offset, length, buffer);
    }

    @Override
    public byte readByte() throws IOException {
        byte b = buffer.array()[pos];
        pos++;
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        System.arraycopy(buffer.array(), pos, b, offset, len);
        pos += len;
    }
}
