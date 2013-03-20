package com.leansoft.luxun.message.compress;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import com.leansoft.luxun.message.generated.CompressionCodec;

public class CompressionUtils {

	public static byte[] compress(byte[] data, CompressionCodec compressionCodec) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final CompressionFacade compressionFacade = CompressionFactory.create(//
                 compressionCodec
                , outputStream);
        try {
			compressionFacade.write(data);
		} catch (IOException e) {
			throw new IllegalStateException("writting data failed", e);
		} finally {
			compressionFacade.close();
		}
        return outputStream.toByteArray();
	}
	
	public static byte[] decompress(byte[] data, CompressionCodec compressionCodec) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        InputStream inputStream = new ByteArrayInputStream(data);
        
        byte[] intermediateBuffer = new byte[1024];
        final CompressionFacade compressionFacade = CompressionFactory.create(//
                compressionCodec
                , inputStream);
        try {
            int dataRead = 0;
            while ((dataRead = compressionFacade.read(intermediateBuffer)) > 0) {
                outputStream.write(intermediateBuffer, 0, dataRead);
            }
        } catch (IOException e) {
            throw new IllegalStateException("decompression data failed", e);
        } finally {
            compressionFacade.close();
        }
        return outputStream.toByteArray();
	}
}
