/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.security.auth;

import javax.security.auth.kerberos.KerberosTicket;
import org.apache.storm.Config;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.Subject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.URIParameter;
import java.security.MessageDigest;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.security.INimbusCredentialPlugin;
import org.apache.storm.utils.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class AuthUtils {
    private static final Logger LOG = LoggerFactory.getLogger(AuthUtils.class);
    public static final String LOGIN_CONTEXT_SERVER = "StormServer";
    public static final String LOGIN_CONTEXT_CLIENT = "StormClient";
    public static final String LOGIN_CONTEXT_PACEMAKER_DIGEST = "PacemakerDigest";
    public static final String LOGIN_CONTEXT_PACEMAKER_SERVER = "PacemakerServer";
    public static final String LOGIN_CONTEXT_PACEMAKER_CLIENT = "PacemakerClient";
    public static final String SERVICE = "storm_thrift_server";

    /**
     * Construct a JAAS configuration object per storm configuration file
     * @param topoConf Storm configuration
     * @return JAAS configuration object
     */
    public static Configuration GetConfiguration(Map<String, Object> topoConf) {
        Configuration login_conf = null;

        //find login file configuration from Storm configuration
        String loginConfigurationFile = (String)topoConf.get("java.security.auth.login.config");
        if ((loginConfigurationFile != null) && (loginConfigurationFile.length()>0)) {
            File config_file = new File(loginConfigurationFile);
            if (!config_file.canRead()) {
                throw new RuntimeException("File " + loginConfigurationFile +
                        " cannot be read.");
            }
            try {
                URI config_uri = config_file.toURI();
                login_conf = Configuration.getInstance("JavaLoginConfig", new URIParameter(config_uri));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        return login_conf;
    }

    /**
     * Get configurations for a section
     * @param configuration The config to pull the key/value pairs out of.
     * @param section The app configuration entry name to get stuff from.
     * @return Return array of config entries or null if configuration is null
     */
    public static AppConfigurationEntry[] getEntries(Configuration configuration, 
                                                String section) throws IOException {
        if (configuration == null) {
            return null;
        }

        AppConfigurationEntry configurationEntries[] = configuration.getAppConfigurationEntry(section);
        if (configurationEntries == null) {
            String errorMessage = "Could not find a '"+ section + "' entry in this configuration.";
            throw new IOException(errorMessage);
        }
        return configurationEntries;
    }

    /**
     * Pull a set of keys out of a Configuration.
     * @param configuration The config to pull the key/value pairs out of.
     * @param section The app configuration entry name to get stuff from.
     * @return Return a map of the configs in conf.
     */
    public static SortedMap<String, ?> pullConfig(Configuration configuration,
                                            String section) throws IOException {
        AppConfigurationEntry[] configurationEntries = AuthUtils.getEntries(configuration, section);

        if (configurationEntries == null) {
            return null;
        }
        
        TreeMap<String, Object> results = new TreeMap<>();

        for (AppConfigurationEntry entry: configurationEntries) {
            Map<String, ?> options = entry.getOptions();
            for (String key : options.keySet()) {
                results.put(key, options.get(key));
            }
        }

        return results;
    }

    /**
     * Pull a the value given section and key from Configuration
     * @param configuration The config to pull the key/value pairs out of.
     * @param section The app configuration entry name to get stuff from.
     * @param key The key to look up inside of the section
     * @return Return a the String value of the configuration value
     */
    public static String get(Configuration configuration, String section, String key) throws IOException {
        AppConfigurationEntry[] configurationEntries = AuthUtils.getEntries(configuration, section);

        if (configurationEntries == null){
            return null;
        }

        for (AppConfigurationEntry entry: configurationEntries) {
            Object val = entry.getOptions().get(key);
            if (val != null)
                return (String)val;
        }
        return null;
    }

    /**
     * Construct a principal to local plugin
     * @param topoConf storm configuration
     * @return the plugin
     */
    public static IPrincipalToLocal GetPrincipalToLocalPlugin(Map<String, Object> topoConf) {
        IPrincipalToLocal ptol = null;
        try {
            String ptol_klassName = (String) topoConf.get(Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN);
            if (ptol_klassName == null) {
                LOG.warn("No principal to local given {}", Config.STORM_PRINCIPAL_TO_LOCAL_PLUGIN);
            } else {
                ptol = ReflectionUtils.newInstance(ptol_klassName);
                //TODO this can only ever be null if someone is doing something odd with mocking
                // We should really fix the mocking and remove this
                if (ptol != null) {
                    ptol.prepare(topoConf);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ptol;
    }

    /**
     * Construct a group mapping service provider plugin
     * @param conf daemon configuration
     * @return the plugin
     */
    public static IGroupMappingServiceProvider GetGroupMappingServiceProviderPlugin(Map<String, Object> conf) {
        IGroupMappingServiceProvider gmsp = null;
        try {
            String gmsp_klassName = (String) conf.get(Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN);
            if (gmsp_klassName == null) {
                LOG.warn("No group mapper given {}", Config.STORM_GROUP_MAPPING_SERVICE_PROVIDER_PLUGIN);
            } else {
                gmsp = ReflectionUtils.newInstance(gmsp_klassName);
                if (gmsp != null) {
                    gmsp.prepare(conf);
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return gmsp;
    }

    /**
     * Get all of the configured Credential Renewer Plugins.
     * @param conf the storm configuration to use.
     * @return the configured credential renewers.
     */
    public static Collection<ICredentialsRenewer> GetCredentialRenewers(Map<String, Object> conf) {
        try {
            Set<ICredentialsRenewer> ret = new HashSet<>();
            Collection<String> clazzes = (Collection<String>)conf.get(Config.NIMBUS_CREDENTIAL_RENEWERS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    ICredentialsRenewer inst = ReflectionUtils.newInstance(clazz);
                    inst.prepare(conf);
                    ret.add(inst);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all the Nimbus Auto cred plugins.
     * @param conf nimbus configuration to use.
     * @return nimbus auto credential plugins.
     */
    public static Collection<INimbusCredentialPlugin> getNimbusAutoCredPlugins(Map<String, Object> conf) {
        try {
            Set<INimbusCredentialPlugin> ret = new HashSet<>();
            Collection<String> clazzes = (Collection<String>)conf.get(Config.NIMBUS_AUTO_CRED_PLUGINS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    INimbusCredentialPlugin inst = ReflectionUtils.newInstance(clazz);
                    inst.prepare(conf);
                    ret.add(inst);
                }
            }
            return ret;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all of the configured AutoCredential Plugins.
     * @param topoConf the storm configuration to use.
     * @return the configured auto credentials.
     */
    public static Collection<IAutoCredentials> GetAutoCredentials(Map<String, Object> topoConf) {
        try {
            Set<IAutoCredentials> autos = new HashSet<>();
            Collection<String> clazzes = (Collection<String>)topoConf.get(Config.TOPOLOGY_AUTO_CREDENTIALS);
            if (clazzes != null) {
                for (String clazz : clazzes) {
                    IAutoCredentials a = ReflectionUtils.newInstance(clazz);
                    a.prepare(topoConf);
                    autos.add(a);
                }
            }
            LOG.info("Got AutoCreds "+autos);
            return autos;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Populate a subject from credentials using the IAutoCredentials.
     * @param subject the subject to populate or null if a new Subject should be created.
     * @param autos the IAutoCredentials to call to populate the subject.
     * @param credentials the credentials to pull from
     * @return the populated subject.
     */
    public static Subject populateSubject(Subject subject, Collection<IAutoCredentials> autos, Map<String,String> credentials) {
        try {
            if (subject == null) {
                subject = new Subject();
            }
            for (IAutoCredentials autoCred : autos) {
                autoCred.populateSubject(subject, credentials);
            }
            return subject;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Update a subject from credentials using the IAutoCredentials.
     * @param subject the subject to update
     * @param autos the IAutoCredentials to call to update the subject.
     * @param credentials the credentials to pull from
     */
    public static void updateSubject(Subject subject, Collection<IAutoCredentials> autos, Map<String,String> credentials) {
        if (subject == null || autos == null) {
            throw new RuntimeException("The subject or auto credentials cannot be null when updating a subject with credentials");
        }

        try {
            for (IAutoCredentials autoCred : autos) {
                autoCred.updateSubject(subject, credentials);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct a transport plugin per storm configuration
     */
    public static ITransportPlugin GetTransportPlugin(ThriftConnectionType type, Map<String, Object> topoConf, Configuration login_conf) {
        try {
            String transport_plugin_klassName = type.getTransportPlugin(topoConf);
            ITransportPlugin transportPlugin = ReflectionUtils.newInstance(transport_plugin_klassName);
            transportPlugin.prepare(type, topoConf, login_conf);
            return transportPlugin;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static IHttpCredentialsPlugin GetHttpCredentialsPlugin(Map<String, Object> conf,
            String klassName) {
        try {
            IHttpCredentialsPlugin plugin = null;
            if (StringUtils.isNotBlank(klassName)) {
                plugin = ReflectionUtils.newInstance(klassName);
                plugin.prepare(conf);
            }
            return plugin;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Construct an HttpServletRequest credential plugin specified by the UI
     * storm configuration
     * @param conf storm configuration
     * @return the plugin
     */
    public static IHttpCredentialsPlugin GetUiHttpCredentialsPlugin(Map<String, Object> conf) {
        String klassName = (String)conf.get(Config.UI_HTTP_CREDS_PLUGIN);
        return AuthUtils.GetHttpCredentialsPlugin(conf, klassName);
    }

    /**
     * Construct an HttpServletRequest credential plugin specified by the DRPC
     * storm configuration
     * @param conf storm configuration
     * @return the plugin
     */
    public static IHttpCredentialsPlugin GetDrpcHttpCredentialsPlugin(Map<String, Object> conf) {
        String klassName = (String)conf.get(Config.DRPC_HTTP_CREDS_PLUGIN);
        return klassName == null ? null : AuthUtils.GetHttpCredentialsPlugin(conf, klassName);
    }

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    public static String makeDigestPayload(Configuration login_config, String config_section) {
        String username = null;
        String password = null;
        try {
            Map<String, ?> results = AuthUtils.pullConfig(login_config, config_section);
            username = (String)results.get(USERNAME);
            password = (String)results.get(PASSWORD);
        } catch (Exception e) {
            LOG.error("Failed to pull username/password out of jaas conf", e);
        }

        if (username == null || password == null) {
            return null;
        }

        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-512");
            byte[] output = digest.digest((username + ":" + password).getBytes());
            return Hex.encodeHexString(output);
        } catch (java.security.NoSuchAlgorithmException e) {
            LOG.error("Cant run SHA-512 digest. Algorithm not available.", e);
            throw new RuntimeException(e);
        }
    }

    public static byte[] serializeKerberosTicket(KerberosTicket tgt) throws Exception {
        ByteArrayOutputStream bao = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bao);
        out.writeObject(tgt);
        out.flush();
        out.close();
        return bao.toByteArray();
    }

    public static KerberosTicket deserializeKerberosTicket(byte[] tgtBytes) {
        KerberosTicket ret;
        try {

            ByteArrayInputStream bin = new ByteArrayInputStream(tgtBytes);
            ObjectInputStream in = new ObjectInputStream(bin);
            ret = (KerberosTicket)in.readObject();
            in.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }

    public static KerberosTicket cloneKerberosTicket(KerberosTicket kerberosTicket) {
        if (kerberosTicket != null) {
            try {
                return (deserializeKerberosTicket(serializeKerberosTicket(kerberosTicket)));
            } catch (Exception e) {
                throw new RuntimeException("Failed to clone KerberosTicket TGT!!", e);
            }
        }
        return null;
    }
}
