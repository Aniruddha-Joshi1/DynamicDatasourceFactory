package com.datamigrate.exportimportservice.utils;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * A Parquet InputFile backed by Java NIO — no Hadoop, no winutils.exe, no HADOOP_HOME.
 * Works on any OS.
 */
public class ParquetInputFileUtils implements InputFile {

    private final Path path;

    public ParquetInputFileUtils(Path path) {
        this.path = path;
    }

    @Override
    public long getLength() throws IOException {
        return Files.size(path);
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        SeekableByteChannel channel = Files.newByteChannel(path);
        return new SeekableInputStream() {

            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void seek(long newPos) throws IOException {
                channel.position(newPos);
                position = newPos;
            }

            @Override
            public void readFully(byte[] bytes) throws IOException {
                readFully(bytes, 0, bytes.length);
            }

            @Override
            public void readFully(byte[] bytes, int start, int len) throws IOException {
                ByteBuffer buf = ByteBuffer.wrap(bytes, start, len);
                while (buf.hasRemaining()) {
                    int n = channel.read(buf);
                    if (n == -1) throw new IOException("Unexpected end of file");
                }
                position += len;
            }

            @Override
            public int read(ByteBuffer buf) throws IOException {
                int n = channel.read(buf);
                if (n > 0) position += n;
                return n;
            }

            @Override
            public void readFully(ByteBuffer buf) throws IOException {
                while (buf.hasRemaining()) {
                    int n = channel.read(buf);
                    if (n == -1) throw new IOException("Unexpected end of file");
                }
                position = channel.position();
            }

            @Override
            public int read() throws IOException {
                ByteBuffer buf = ByteBuffer.allocate(1);
                int n = channel.read(buf);
                if (n == -1) return -1;
                position++;
                return buf.get(0) & 0xFF;
            }

            @Override
            public void close() throws IOException {
                channel.close();
            }
        };
    }
}