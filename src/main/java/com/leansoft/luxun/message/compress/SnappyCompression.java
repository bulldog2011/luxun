package com.leansoft.luxun.message.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class SnappyCompression extends CompressionFacade {

	public SnappyCompression(InputStream inputStream, OutputStream outputStream) throws IOException {
		super(inputStream != null ? new SnappyInputStream(inputStream) : null, //
                outputStream != null ? new SnappyOutputStream(outputStream) : null);
	}

}
