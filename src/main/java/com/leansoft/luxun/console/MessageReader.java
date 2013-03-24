package com.leansoft.luxun.console;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public interface MessageReader {
	
    void init(InputStream inputStream, Properties props);

    String readMessage() throws IOException;

}
