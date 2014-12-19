package com.leansoft.luxun.client;

import java.io.Closeable;

import org.apache.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import com.leansoft.luxun.api.generated.QueueService;
import com.leansoft.luxun.common.exception.ConnectionRefusedException;

public abstract class AbstractClient implements Closeable {
	
    private final Logger logger = Logger.getLogger(AbstractClient.class);

    private final String host;

    private final int port;

    private final int soTimeoutMs;
    private final int connectTimeoutMs;
    private final int MaxConnectBackoffMs = 60 * 1000;

    ////////////////////////////////
	private TTransport transport = null;
	protected QueueService.Client luxunClient = null;

    protected final Object lock = new Object();
    
    private volatile boolean shutdown = false;
    
    public AbstractClient(String host, int port) {
        this(host, port, 30 * 1000);
    }

    public AbstractClient(String host, int port, int soTimeoutMs) {
        this(host, port, soTimeoutMs, 60 * 1000);
    }
    
    public AbstractClient(String host, int port, int soTimeout, int connectTimeoutMs) {
        this.host = host;
        this.port = port;
        this.soTimeoutMs = soTimeout;
        this.connectTimeoutMs = connectTimeoutMs;
    }
    
    protected void getOrMakeConnection() {
        if (luxunClient == null) {
        	connect();
        }
    }
    
    protected void reconnect() {
		disconnect();
		connect();
    }
    
    
    protected void connect() {
		long connectBackoffMs = 1;
		long beginTimeMs = System.currentTimeMillis();
		while(luxunClient == null && !shutdown) {
			try {
				TSocket socket = new TSocket(host, port, soTimeoutMs);
				transport = new TFramedTransport(socket);
				TProtocol protocol = new TBinaryProtocol(transport);
				luxunClient = new QueueService.Client(protocol);
				transport.open();
	            logger.info("Connected to " + host + ":" + port + " for operating");
			} catch (TTransportException e) {
				disconnect();
				long endTimeMs = System.currentTimeMillis();
                if ((endTimeMs - beginTimeMs + connectBackoffMs) > connectTimeoutMs) {
                    logger.error(
                            "Connection attempt to " + host + ":" + port + " timing out after " + connectTimeoutMs + " ms",
                            e);
                    throw new ConnectionRefusedException(host + ":" + port, e);
                }
                logger.error(
                        "Connection attempt to " + host+ ":" + port + " failed, next attempt in " + connectBackoffMs + " ms",
                        e);
                try {
                    Thread.sleep(connectBackoffMs);
                } catch (InterruptedException e1) {
                    logger.warn(e1.getMessage());
                    Thread.currentThread().interrupt();
                }
                connectBackoffMs = Math.min(10 * connectBackoffMs, MaxConnectBackoffMs);
			}
		}
	}
    
    protected void disconnect() {
		if (transport != null) {
            logger.info("Disconnecting to " + host + ":" + port);
			transport.close();
			transport = null;
		}
		luxunClient = null;
	}
	
	public void close() {
		synchronized(lock) {
			disconnect();
			shutdown = true;
		}
	}
}
