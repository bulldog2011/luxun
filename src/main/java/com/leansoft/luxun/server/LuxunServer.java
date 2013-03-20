package com.leansoft.luxun.server;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import com.leansoft.luxun.log.LogManager;
import com.leansoft.luxun.mx.Log4jController;
import com.leansoft.luxun.mx.ServerInfo;
import com.leansoft.luxun.mx.ThriftServerStats;
import com.leansoft.luxun.utils.Mx4jLoader;
import com.leansoft.luxun.utils.Scheduler;
import com.leansoft.luxun.utils.Utils;

/**
 * The main luxun server container
 * 
 * @author bulldog
 *
 */
public class LuxunServer implements Closeable {
	
    final String CLEAN_SHUTDOWN_FILE = ".luxun_cleanshutdown";

    final private Logger logger = Logger.getLogger(LuxunServer.class);

    public final ServerConfig config;

    final Scheduler scheduler = new Scheduler(1, "luxun-logcleaner", false);

    private LogManager logManager;

    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
    
    ThriftServer thriftServer;
    private final ThriftServerStats stats;
    
    private final File logDir;

    private final ServerInfo serverInfo = new ServerInfo();
    private final Log4jController log4jController = new Log4jController();

    public LuxunServer(ServerConfig config) {
        this.config = config;
        logDir = new File(config.getLogDir());
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        this.stats = new ThriftServerStats(1000L * 1000L * 1000L * config.getMonitoringPeriodSecs());
        Utils.registerMBean(this.stats);
    }
    
    public void startup() {
    	try {
            logger.info("Starting luxun server " + serverInfo.getVersion());
    		
            Utils.registerMBean(serverInfo);
            Utils.registerMBean(log4jController);
            
            boolean needRecovery = true;
            File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
            if (cleanShutDownFile.exists()) {
                needRecovery = false;
                cleanShutDownFile.delete();
            }
            
            this.logManager = new LogManager(config,//
                    scheduler,//
                    1000L * 60 * config.getLogCleanupIntervalMinutes(),//
                    1000L * 60 * 60 * config.getLogRetentionHours(),//
                    needRecovery, this.stats);
            logManager.load();
            
            thriftServer = new ThriftServer(this.logManager, this.config, this.stats);
            thriftServer.startup();
            Mx4jLoader.maybeLoad();
    		
            logManager.startup();
            serverInfo.started();
            logger.info("Server started.");
    	} catch (Exception ex) {
            logger.fatal("Fatal error during startup.", ex);
            try {
				close();
			} catch (IOException e) {
				logger.error("exception during server closing.", e);
			}
        }
    }

	@Override
	public void close() throws IOException {
		boolean canShutdown = isShuttingDown.compareAndSet(false, true);
		if (canShutdown) {
			logger.info("Shutting down luxun server...");
			try {
				scheduler.shutdown();
				if (this.thriftServer != null) {
					this.thriftServer.close();
					Utils.unregisterMBean(this.stats);
				}
				if (this.logManager != null) {
					this.logManager.close();
				}
				
				File cleanShutDownFile = new File(new File(config.getLogDir()), CLEAN_SHUTDOWN_FILE);
				cleanShutDownFile.createNewFile();
			} catch (Exception ex) {
				logger.fatal(ex.getMessage(), ex);
			}
			shutdownLatch.countDown();
			logger.info("shutdown queue luxun completed");
			Utils.unregisterMBean(this.log4jController);
		}
	}
	
	public void awaitShutdown() throws InterruptedException {
		this.shutdownLatch.await();
	}
	
	public LogManager getLogManager() {
		return this.logManager;
	}
	
	public ThriftServerStats getStats() {
		return this.thriftServer.getStats();
	}
	
}
