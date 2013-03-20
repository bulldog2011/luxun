package com.leansoft.luxun.mx;

import java.util.List;

/**
 * remote controller of log4j
 * 
 * @author bulldog
 * 
 */
public interface Log4jControllerMBean {
    /**
     * get all loggers seperated by '='.<br/>
     * for example: <code>root=INFO</code>
     * @return all loggers
     */
    List<String> getLoggers();

    String getLogLevel(String loggerName);

    boolean setLogLevel(String loggerName, String level);
}
