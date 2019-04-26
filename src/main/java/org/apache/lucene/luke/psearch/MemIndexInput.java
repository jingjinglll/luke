package org.apache.lucene.luke.psearch;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/*
   Slice lucene documents from memory, rather than reading from pravega or rocksdb again.
*/
public class MemIndexInput extends IndexInput {

    private int slicedPos = 0;
    private ByteBuffer slicedBuffer;

    public MemIndexInput(String resourceDescription, long offset, long length, ByteBuffer buffer) {
        super(resourceDescription);
        byte[] tmp = buffer.array();
        tmp = Arrays.copyOfRange(tmp, (int) offset, (int) (offset + length));
        slicedBuffer = ByteBuffer.wrap(tmp);
    }

    @Override
    public void close() throws IOException {
        this.slicedBuffer = null;
    }

    @Override
    public long getFilePointer() {
        return this.slicedPos;
    }

    @Override
    public void seek(long pos) throws IOException {
        slicedPos = (int) pos;
    }

    @Override
    public long length() {
        return slicedBuffer.limit();
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
        return new MemIndexInput(sliceDescription, offset, length, this.slicedBuffer);
    }

    @Override
    public byte readByte() throws IOException {
        byte b = slicedBuffer.array()[slicedPos];
        slicedPos++;
        return b;
    }

    @Override
    public void readBytes(byte[] b, int offset, int len) throws IOException {
        System.arraycopy(slicedBuffer.array(), slicedPos, b, offset, len);
        slicedPos += len;
    }
}
