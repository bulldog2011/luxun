package com.leansoft.luxun.server;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;

import com.leansoft.luxun.api.generated.QueueService;
import com.leansoft.luxun.mx.ThriftServerStats;
import com.leansoft.luxun.utils.Utils;

public class ThriftServer implements Closeable {
	
	private final Logger logger = Logger.getLogger(ThriftServer.class);
	
	
	private final QueueService.Iface queueService;
	
	private final ServerConfig serverConfig;
	
    private final ThriftServerStats stats;
    
    private final TNonblockingServer server;
    
    private final ServerThread serverThread;
    
    public final static int CLOSE_TIMEOUT_IN_SECONDS = 30; // seconds
    public final static int STARTUP_TIMEOUT_IN_SECONDS = 30; // seconds

	public ThriftServer(QueueService.Iface queueService, ServerConfig serverConfig, ThriftServerStats stats) throws TTransportException {
		this.queueService = queueService;
		this.serverConfig = serverConfig;
        this.stats = stats;
        
        // assemble thrift server
        TProcessor tprocessor = new QueueService.Processor(this.queueService);
        TNonblockingServerSocket tnbSocketTransport = new TNonblockingServerSocket(serverConfig.getPort());
        TNonblockingServer.Args tnbArgs = new TNonblockingServer.Args(tnbSocketTransport);
        tnbArgs.processor(tprocessor);
        // Nonblocking server mode must use TFramedTransport
        tnbArgs.transportFactory(new TFramedTransport.Factory());
        tnbArgs.protocolFactory(new TBinaryProtocol.Factory());
        
        this.server = new TNonblockingServer(tnbArgs);
        
        this.serverThread = new ServerThread(this.server);
	}

	@Override
	public void close() throws IOException {
		this.server.stop();
		int timeWaited = 0;
		int checkInterval = 1000;
		while(!this.server.isStopped() || this.server.isServing()) {
			logger.info("Wating server to close, time waited : " + timeWaited / 1000 + " s.");
			if (timeWaited > CLOSE_TIMEOUT_IN_SECONDS * 1000) {
				String errorMessage = "fail to stop server within timeout " + CLOSE_TIMEOUT_IN_SECONDS + " seconds";
				logger.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}
			try {
				Thread.sleep(checkInterval);
				timeWaited += checkInterval;
			} catch (InterruptedException e) {
				// ignore
			}
		}
		logger.info("Thrift server closed.");
	}
	
	public void startup() {
		Utils.newThread("luxun-server", this.serverThread, false).start();
		int timeWaited = 0;
		int checkInterval = 1000;
		while(!this.server.isServing()) {
			logger.info("Wating server to start, time waited : " + timeWaited / 1000 + " s.");
			if (timeWaited > STARTUP_TIMEOUT_IN_SECONDS * 1000) {
				String errorMessage = "fail to start server within timeout " + STARTUP_TIMEOUT_IN_SECONDS + " seconds";
				logger.error(errorMessage);
				throw new RuntimeException(errorMessage);
			}
			try {
				Thread.sleep(checkInterval);
				timeWaited += checkInterval;
			} catch (InterruptedException e) {
				// ignore
			}
		}
		logger.info("Thrift server started on port : " + this.serverConfig.getPort());
	}
	
    public ThriftServerStats getStats() {
        return stats;
    }
	
	static class ServerThread implements Runnable {
		
		private TNonblockingServer server;
		
		ServerThread(TNonblockingServer server) {
			this.server = server;
		}

		@Override
		public void run() {
			this.server.serve();
		}
		
	}

}
