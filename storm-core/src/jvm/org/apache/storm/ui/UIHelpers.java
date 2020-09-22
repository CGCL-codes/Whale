/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.ui;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLEncoder;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import javax.servlet.DispatcherType;
import javax.servlet.Servlet;
import org.apache.storm.generated.ExecutorInfo;
import org.apache.storm.logging.filters.AccessLoggingFilter;
import org.apache.storm.utils.ObjectReader;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.servlets.CrossOriginFilter;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.json.simple.JSONValue;

public class UIHelpers {

    private static final Object[][] PRETTY_SEC_DIVIDERS = {
            new Object[]{"s", 60},
            new Object[]{"m", 60},
            new Object[]{"h", 24},
            new Object[]{"d", null}};

    private static final Object[][] PRETTY_MS_DIVIDERS = {
            new Object[]{"ms", 1000},
            new Object[]{"s", 60},
            new Object[]{"m", 60},
            new Object[]{"h", 24},
            new Object[]{"d", null}};

    public static String prettyUptimeStr(String val, Object[][] dividers) {
        int uptime = Integer.parseInt(val);
        LinkedList<String> tmp = new LinkedList<>();
        for (Object[] divider : dividers) {
            if (uptime > 0) {
                String state = (String) divider[0];
                Integer div = (Integer) divider[1];
                if (div != null) {
                    tmp.addFirst(uptime % div + state);
                    uptime = uptime / div;
                } else {
                    tmp.addFirst(uptime + state);
                }
            }
        }
        return Joiner.on(" ").join(tmp);
    }

    public static String prettyUptimeSec(String sec) {
        return prettyUptimeStr(sec, PRETTY_SEC_DIVIDERS);
    }

    public static String prettyUptimeSec(int secs) {
        return prettyUptimeStr(String.valueOf(secs), PRETTY_SEC_DIVIDERS);
    }

    public static String prettyUptimeMs(String ms) {
        return prettyUptimeStr(ms, PRETTY_MS_DIVIDERS);
    }

    public static String prettyUptimeMs(int ms) {
        return prettyUptimeStr(String.valueOf(ms), PRETTY_MS_DIVIDERS);
    }


    public static String urlFormat(String fmt, Object... args) {
        String[] argsEncoded = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            argsEncoded[i] = URLEncoder.encode(String.valueOf(args[i]));
        }
        return String.format(fmt, argsEncoded);
    }

    public static String prettyExecutorInfo(ExecutorInfo e) {
        return "[" + e.get_task_start() + "-" + e.get_task_end() + "]";
    }

    public static Map unauthorizedUserJson(String user) {
        return ImmutableMap.of(
                "error", "No Authorization",
                "errorMessage", String.format("User %s is not authorized.", user));
    }

    private static ServerConnector mkSslConnector(Server server, Integer port, String ksPath, String ksPassword, String ksType,
                                                     String keyPassword, String tsPath, String tsPassword, String tsType,
                                                     Boolean needClientAuth, Boolean wantClientAuth, Integer headerBufferSize) {
        SslContextFactory factory = new SslContextFactory();
        factory.setExcludeCipherSuites("SSL_RSA_WITH_RC4_128_MD5", "SSL_RSA_WITH_RC4_128_SHA");
        factory.setExcludeProtocols("SSLv3");
        factory.setRenegotiationAllowed(false);
        factory.setKeyStorePath(ksPath);
        factory.setKeyStoreType(ksType);
        factory.setKeyStorePassword(ksPassword);
        factory.setKeyManagerPassword(keyPassword);

        if (tsPath != null && tsPassword != null && tsType != null) {
            factory.setTrustStorePath(tsPath);
            factory.setTrustStoreType(tsType);
            factory.setTrustStorePassword(tsPassword);
        }

        if (needClientAuth != null && needClientAuth) {
            factory.setNeedClientAuth(true);
        } else if (wantClientAuth != null && wantClientAuth) {
            factory.setWantClientAuth(true);
        }

        HttpConfiguration httpsConfig = new HttpConfiguration();
        httpsConfig.addCustomizer(new SecureRequestCustomizer());
        if (null != headerBufferSize) {
            httpsConfig.setRequestHeaderSize(headerBufferSize);
        }
        ServerConnector sslConnector = new ServerConnector(server,
                new SslConnectionFactory(factory, HttpVersion.HTTP_1_1.asString()),
                new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(port);
        return sslConnector;
    }

    public static void configSsl(Server server, Integer port, String ksPath, String ksPassword, String ksType,
                                 String keyPassword, String tsPath, String tsPassword, String tsType,
                                 Boolean needClientAuth, Boolean wantClientAuth) {
        configSsl(server, port, ksPath, ksPassword, ksType, keyPassword,
                tsPath, tsPassword, tsType, needClientAuth, wantClientAuth, null);
    }

    public static void configSsl(Server server, Integer port, String ksPath, String ksPassword, String ksType,
                                 String keyPassword, String tsPath, String tsPassword, String tsType,
                                 Boolean needClientAuth, Boolean wantClientAuth, Integer headerBufferSize) {
        if (port > 0) {
            server.addConnector(mkSslConnector(server, port, ksPath, ksPassword, ksType, keyPassword,
                    tsPath, tsPassword, tsType, needClientAuth, wantClientAuth, headerBufferSize));
        }
    }

    public static FilterHolder corsFilterHandle() {
        FilterHolder filterHolder = new FilterHolder(new CrossOriginFilter());
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "*");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "GET, POST, PUT");
        filterHolder.setInitParameter(CrossOriginFilter.ALLOWED_ORIGINS_PARAM, "X-Requested-With, X-Requested-By, Access-Control-Allow-Origin, Content-Type, Content-Length, Accept, Origin");
        filterHolder.setInitParameter(CrossOriginFilter.ACCESS_CONTROL_ALLOW_ORIGIN_HEADER, "*");
        return filterHolder;
    }

    public static FilterHolder mkAccessLoggingFilterHandle() {
        return new FilterHolder(new AccessLoggingFilter());
    }

    public static void configFilter(Server server, Servlet servlet, List<FilterConfiguration> filtersConfs) {
        configFilter(server, servlet, filtersConfs, null);
    }
    
    public static void configFilter(Server server, Servlet servlet, List<FilterConfiguration> filtersConfs, Map<String, String> params) {
        if (filtersConfs != null) {
            ServletHolder servletHolder = new ServletHolder(servlet);
            servletHolder.setInitOrder(0);
            if (params != null) {
                servletHolder.setInitParameters(params);
            }
            ServletContextHandler context = new ServletContextHandler(server, "/");
            context.addServlet(servletHolder, "/");
            configFilters(context, filtersConfs);
            server.setHandler(context);
        }
    }
    
    public static void configFilters(ServletContextHandler context, List<FilterConfiguration> filtersConfs) {
        context.addFilter(corsFilterHandle(), "/*", EnumSet.allOf(DispatcherType.class));
        for (FilterConfiguration filterConf : filtersConfs) {
            String filterName = filterConf.getFilterName();
            String filterClass = filterConf.getFilterClass();
            Map filterParams = filterConf.getFilterParams();
            if (filterClass != null) {
                FilterHolder filterHolder = new FilterHolder();
                filterHolder.setClassName(filterClass);
                if (filterName != null) {
                    filterHolder.setName(filterName);
                } else {
                    filterHolder.setName(filterClass);
                }
                if (filterParams != null) {
                    filterHolder.setInitParameters(filterParams);
                } else {
                    filterHolder.setInitParameters(new HashMap<String, String>());
                }
                context.addFilter(filterHolder, "/*", EnumSet.allOf(DispatcherType.class));
            }
        }
        context.addFilter(mkAccessLoggingFilterHandle(), "/*", EnumSet.allOf(DispatcherType.class));
    }

    /**
     * Construct a Jetty Server instance.
     */
    public static Server jettyCreateServer(Integer port, String host, Integer httpsPort) {
        return jettyCreateServer(port, host, httpsPort, null);
    }

    /**
     * Construct a Jetty Server instance.
     */
    public static Server jettyCreateServer(Integer port, String host, Integer httpsPort, Integer headerBufferSize) {
        Server server = new Server();

        if (httpsPort == null || httpsPort <= 0) {
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setSendDateHeader(true);
            if (null != headerBufferSize) {
                httpConfig.setRequestHeaderSize(headerBufferSize);
            }
            ServerConnector httpConnector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
            httpConnector.setPort(ObjectReader.getInt(port, 80));
            httpConnector.setIdleTimeout(200000);
            httpConnector.setHost(host);
            server.addConnector(httpConnector);
        }

        return server;
    }

    /**
     * Modified version of run-jetty
     * Assumes configurator sets handler.
     */
    public static void stormRunJetty(Integer port, String host, Integer httpsPort, Integer headerBufferSize, IConfigurator configurator) throws Exception {
        Server s = jettyCreateServer(port, host, httpsPort, headerBufferSize);
        if (configurator != null) {
            configurator.execute(s);
        }
        s.start();
    }

    public static void stormRunJetty(Integer port, Integer headerBufferSize, IConfigurator configurator) throws Exception {
        stormRunJetty(port, null, null, headerBufferSize, configurator);
    }

    public static String wrapJsonInCallback(String callback, String response) {
        return callback + "(" + response + ");";
    }

    public static Map getJsonResponseHeaders(String callback, Map headers) {
        Map<String, String> headersResult = new HashMap<>();
        headersResult.put("Cache-Control", "no-cache, no-store");
        headersResult.put("Access-Control-Allow-Origin", "*");
        headersResult.put("Access-Control-Allow-Headers", "Content-Type, Access-Control-Allow-Headers, Access-Controler-Allow-Origin, X-Requested-By, X-Csrf-Token, Authorization, X-Requested-With");
        if (callback != null) {
            headersResult.put("Content-Type", "application/javascript;charset=utf-8");
        } else {
            headersResult.put("Content-Type", "application/json;charset=utf-8");
        }
        if (headers != null) {
            headersResult.putAll(headers);
        }
        return headersResult;
    }

    public static String getJsonResponseBody(Object data, String callback, boolean needSerialize) {
        String serializedData = needSerialize ? JSONValue.toJSONString(data) : (String) data;
        return callback != null ? wrapJsonInCallback(callback, serializedData) : serializedData;
    }

    public static Map exceptionToJson(Exception ex, int statusCode) {
        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        return ImmutableMap.of("error", statusCode + " " + HttpStatus.getMessage(statusCode), "errorMessage", sw.toString());
    }
}
