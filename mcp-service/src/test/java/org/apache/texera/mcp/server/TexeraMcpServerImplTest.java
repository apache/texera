/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.mcp.server;

import org.apache.texera.config.McpConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TexeraMcpServerImpl.
 * Tests server lifecycle, configuration, and metadata access.
 */
class TexeraMcpServerImplTest {

    private TexeraMcpServerImpl mcpServer;

    @BeforeEach
    void setUp() {
        mcpServer = new TexeraMcpServerImpl();
    }

    @AfterEach
    void tearDown() {
        if (mcpServer != null && mcpServer.isRunning()) {
            mcpServer.stop();
        }
    }

    @Test
    @DisplayName("Server should be created in stopped state")
    void testServerInitialState() {
        assertFalse(mcpServer.isRunning(), "Server should not be running initially");
    }

    @Test
    @DisplayName("Server should start successfully")
    void testServerStart() {
        assertDoesNotThrow(() -> mcpServer.start(), "Server start should not throw");
        assertTrue(mcpServer.isRunning(), "Server should be running after start");
    }

    @Test
    @DisplayName("Server should stop successfully")
    void testServerStop() {
        mcpServer.start();
        assertTrue(mcpServer.isRunning(), "Server should be running before stop");

        mcpServer.stop();
        assertFalse(mcpServer.isRunning(), "Server should not be running after stop");
    }

    @Test
    @DisplayName("Server should handle multiple stop calls gracefully")
    void testMultipleStopCalls() {
        mcpServer.start();
        mcpServer.stop();

        assertDoesNotThrow(() -> mcpServer.stop(), "Multiple stop calls should not throw");
        assertFalse(mcpServer.isRunning(), "Server should remain stopped");
    }

    @Test
    @DisplayName("Server info should contain correct metadata")
    void testServerInfo() {
        Map<String, String> info = mcpServer.getServerInfo();

        assertNotNull(info, "Server info should not be null");
        assertEquals(McpConfig.serverName(), info.get("name"), "Server name should match config");
        assertEquals(McpConfig.serverVersion(), info.get("version"), "Server version should match config");
        assertEquals("stopped", info.get("status"), "Initial status should be stopped");
    }

    @Test
    @DisplayName("Server info should reflect running status after start")
    void testServerInfoAfterStart() {
        mcpServer.start();
        Map<String, String> info = mcpServer.getServerInfo();

        assertEquals("running", info.get("status"), "Status should be running after start");
    }

    @Test
    @DisplayName("Server capabilities should match configuration")
    void testServerCapabilities() {
        Map<String, Object> caps = mcpServer.getCapabilities();

        assertNotNull(caps, "Capabilities should not be null");
        assertEquals(McpConfig.toolsEnabled(), caps.get("tools"), "Tools capability should match config");
        assertEquals(McpConfig.resourcesEnabled(), caps.get("resources"), "Resources capability should match config");
        assertEquals(McpConfig.promptsEnabled(), caps.get("prompts"), "Prompts capability should match config");
        assertEquals(McpConfig.samplingEnabled(), caps.get("sampling"), "Sampling capability should match config");
        assertEquals(McpConfig.loggingEnabled(), caps.get("logging"), "Logging capability should match config");
    }

    @Test
    @DisplayName("Capabilities should be accessible before server start")
    void testCapabilitiesBeforeStart() {
        assertDoesNotThrow(() -> mcpServer.getCapabilities(),
            "Getting capabilities should not require server to be running");
    }

    @Test
    @DisplayName("Server lifecycle: start, stop, restart")
    void testServerLifecycle() {
        // First start
        mcpServer.start();
        assertTrue(mcpServer.isRunning(), "Server should be running after first start");

        // Stop
        mcpServer.stop();
        assertFalse(mcpServer.isRunning(), "Server should be stopped");

        // Restart
        mcpServer.start();
        assertTrue(mcpServer.isRunning(), "Server should be running after restart");
    }

    @Test
    @DisplayName("Server info and capabilities should remain consistent across lifecycle")
    void testConsistencyAcrossLifecycle() {
        Map<String, String> infoBefore = mcpServer.getServerInfo();
        Map<String, Object> capsBefore = mcpServer.getCapabilities();

        mcpServer.start();
        mcpServer.stop();

        Map<String, String> infoAfter = mcpServer.getServerInfo();
        Map<String, Object> capsAfter = mcpServer.getCapabilities();

        assertEquals(infoBefore.get("name"), infoAfter.get("name"),
            "Server name should remain consistent");
        assertEquals(infoBefore.get("version"), infoAfter.get("version"),
            "Server version should remain consistent");
        assertEquals(capsBefore, capsAfter,
            "Capabilities should remain consistent");
    }
}
