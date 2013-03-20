package com.leansoft.luxun.log;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.leansoft.bigqueue.BigArrayImpl;
import com.leansoft.luxun.common.exception.InvalidTopicException;
import com.leansoft.luxun.log.ILog;
import com.leansoft.luxun.log.Log;
import com.leansoft.luxun.log.LogManager;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

import junit.framework.TestCase;

public class LogManagerTest extends TestCase {
	
	private ServerConfig config;
	private LogManager logManager;
	private int maxLogAge = 1000;
	private File logDir = null;
	
	@Before
	public void setUp() {
		Properties props = TestUtils.createBrokerConfig(0, -1);
		props.setProperty("log.backfile.page.size", String.valueOf(BigArrayImpl.MINIMUM_DATA_PAGE_SIZE));
		config = new ServerConfig(props);
		logManager = new LogManager(config, null, -1, maxLogAge, false, null);
		logManager.startup();
		logDir = logManager.logDir;
	}

	@Test
	public void testCreateLog() throws IOException {
		String name = "luxun";
		ILog log = logManager.getOrCreateLog(name);
		File logFile = new File(config.getLogDir(), name);
		assertTrue(logFile.exists());
		log.append("test".getBytes());
	}
	
	@Test
	public void testGetLog() {
		String name = "luxun";
		ILog log = logManager.getLog(name);
		File logFile = new File(config.getLogDir(), name);
		assertTrue(!logFile.exists());
	}
	
	@Test
	public void testInvalidTopicName() throws IOException {
		List<String> invalidTopicNames = new ArrayList<String>();
		invalidTopicNames.add("");
		invalidTopicNames.add(".");
		invalidTopicNames.add("..");
		char[] badChars = {'/', '\u0000', '\u0001', '\u0018', '\u001F', '\u008F', '\uD805', '\uFFFA'};
		for(char weirdChar : badChars) {
			invalidTopicNames.add("Is" + weirdChar + "funny");
		}
		
		for(String topicName : invalidTopicNames) {
			try {
				logManager.getOrCreateLog(topicName);
				fail("Should throw InvalidTopicException.");
			} catch (InvalidTopicException ite) {
				// expected
			}
		}
	}
	
	@Test
	public void testCleanupExpiredLogPages() throws IOException {
		ILog log = logManager.getOrCreateLog("cleanup");
		
		String randomString = TestUtils.randomString(32);
		for(int i = 0; i < 4 * 1024 * 1024; i++) { // generate 4 back data pages, size of per page = 32M
			log.append(randomString.getBytes());
		}
		
		log.read(0); // ok
		assertTrue(log.getNumberOfBackFiles() == 4);
		
		logManager.cleanupLogs();
		assertTrue(log.getNumberOfBackFiles() != 1); // hasn't expired yet
		
		// let logs expire
		TestUtils.sleepQuietly(2000);
		logManager.cleanupLogs();
		System.out.println(log.getNumberOfBackFiles());
		assertTrue(log.getNumberOfBackFiles() == 1);
		try {
			log.read(0); // corresponding log page has been deleted
		} catch (IndexOutOfBoundsException ex) {
			// expected
		}
		
		// log should still be appendable
		log.append(randomString.getBytes());
	}
	
	@Test
	public void testCleanupLogPageFilesToMaintainSize() throws IOException {
		int retentionHours = 1;
		long retentionMs = 1000 * 60 * 60 * retentionHours;
		this.tearDown();
		Properties props = TestUtils.createBrokerConfig(0, -1);
		props.setProperty("log.backfile.page.size", String.valueOf(BigArrayImpl.MINIMUM_DATA_PAGE_SIZE));
		props.setProperty("log.retention.size", String.valueOf(BigArrayImpl.MINIMUM_DATA_PAGE_SIZE * 3 * 2));// the size of index file should also be taken into account
		config = new ServerConfig(props);
		logManager = new LogManager(config, null, -1, retentionMs, false, null);
		logManager.startup();
		logDir = logManager.logDir;
		
		ILog log = logManager.getOrCreateLog("cleanup");
		
		String randomString = TestUtils.randomString(32);
		for(int i = 0; i < 4 * 1024 * 1024; i++) { // generate 4 back pages, size of per page = 32M
			log.append(randomString.getBytes());
		}
		log.read(0); // ok
		assertTrue(log.getNumberOfBackFiles() == 4);
		
		// let logs expire
		TestUtils.sleepQuietly(2000);
		
	    // this cleanup shouldn't find any expired segments but should delete some to reduce size
		logManager.cleanupLogs();
		assertTrue(log.getNumberOfBackFiles() == 3);
		try {
			log.read(0); // corresponding log page has been deleted
		} catch (IndexOutOfBoundsException ex) {
			// expected
		}
		
		// log should still be appendable
		log.append(randomString.getBytes());
	}
	
	@Test
	public void testTimeBasedFlush() throws IOException {
		this.tearDown();
		Properties props = TestUtils.createBrokerConfig(0, -1);
		props.setProperty("log.backfile.page.size", String.valueOf(BigArrayImpl.MINIMUM_DATA_PAGE_SIZE));
		props.setProperty("topic.flush.intervals.ms", "timebasedflush:200");
		props.setProperty("log.default.flush.scheduler.interval.ms", "50");
		ServerConfig config = new ServerConfig(props);
		logManager = new LogManager(config, null, -1, maxLogAge, false, null);
		logManager.startup();
		logDir = logManager.logDir;
		
		Log log = (Log) logManager.getOrCreateLog("timebasedflush");
		
		String randomString = TestUtils.randomString(32);
		for(int i = 0; i <  1024 * 1024; i++) {
			log.append(randomString.getBytes());
		}
		
		assertTrue(System.currentTimeMillis() - log.getLastFlushedTime() <= 200 * 2);
	}
	
	@After
	public void tearDown() throws IOException {
		logManager.close();
		TestUtils.sleepQuietly(1000);
		Utils.deleteDirectory(logDir);
	}
}
