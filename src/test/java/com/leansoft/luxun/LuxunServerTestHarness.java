package com.leansoft.luxun;

import java.util.ArrayList;
import java.util.List;

import com.leansoft.luxun.server.LuxunServer;
import com.leansoft.luxun.server.ServerConfig;
import com.leansoft.luxun.utils.TestUtils;
import com.leansoft.luxun.utils.Utils;

import junit.framework.TestCase;

public abstract class LuxunServerTestHarness extends TestCase  {
	
	protected List<ServerConfig> configs;
	protected List<LuxunServer> servers = null;
	
	@Override
	public void setUp() throws Exception {
		if (configs == null || configs.size() <= 0) {
			throw new IllegalArgumentException("Must supply at least one server config");
		}
		servers = new ArrayList<LuxunServer>();
		for(ServerConfig config : configs) {
			servers.add(TestUtils.createServer(config));
		}
		super.setUp();
	}
	
	public void tearDown() throws Exception {
		super.tearDown();
		for(LuxunServer server : servers) {
			server.close();
		}
		for(ServerConfig config : configs) {
			Utils.deleteDirectory(config.getLogDir());
		}
	}

}
