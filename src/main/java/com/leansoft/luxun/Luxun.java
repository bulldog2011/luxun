package com.leansoft.luxun;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.Utils;

/**
 * Luxun main entry 
 * 
 * @author bulldog
 *
 */
public class Luxun implements Closeable {
	
    private final Logger logger = LoggerFactory.getLogger(Luxun.class);
	
	private volatile Thread shutdownHook;
	
	private LuxunServer server;
	
	public void start(String mainFile) {
		start(Utils.loadProps(mainFile));
	}
	
	public void start(Properties mainProperties) {
		final ServerConfig config = new ServerConfig(mainProperties);
		start(config);
	}
	
	public void start(ServerConfig config) {
		server = new LuxunServer(config);
		
		shutdownHook = new Thread() {
			
			@Override
			public void run() {
				try {
					logger.info("Executing shutdown hook ...");
					server.close();
					server.awaitShutdown();
					shutdownHook = null;
				} catch (IOException e) {
		            logger.error("Fatal error during MMQueue Server shutdown. Prepare to halt", e);
		            Runtime.getRuntime().halt(1);
				}  catch (InterruptedException e) {
		            logger.warn(e.getMessage(),e);
				}
			}
		};
		
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        server.startup();
	}
	
    public void awaitShutdown() {
		if (server != null) {
	    	try {
	    		server.awaitShutdown();
			} catch (InterruptedException e) {
	            logger.warn(e.getMessage(),e);
			}
		}
    }
	

	@Override
	public void close() {
        if (shutdownHook != null) {
    		logger.info("Closing luxun server ...");
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
            shutdownHook.run();
            shutdownHook = null;
        }
	}
	
    public static void main(String[] args) {
        int argsSize = args.length;
        if (argsSize != 1) {
            System.out.println("USAGE: java [options] Luxun server.properties");
            System.exit(1);
        }
        //
        Luxun luxun = new Luxun();
        luxun.start(args[0]);
        luxun.awaitShutdown();
        luxun.close();
    }

}
