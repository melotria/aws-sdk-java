/*
 * Copyright 2012-2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.http;

import com.amazonaws.annotation.SdkInternalApi;
import com.amazonaws.http.apache.client.impl.ConnectionManagerAwareHttpClient;
import com.amazonaws.http.client.HttpClientFactory;
import com.amazonaws.http.settings.HttpClientSettings;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.pool.ConnPoolControl;
import org.apache.http.pool.PoolStats;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A monitor for HTTP connection pools that periodically checks the pool stats
 * and refreshes the connection pool if necessary to prevent connection pool exhaustion.
 * <p>
 * This is particularly useful for long-running applications that may experience
 * connection pool exhaustion after several days of operation.
 */
@SdkInternalApi
public class ConnectionPoolMonitor {

    private static final Log LOG = LogFactory.getLog(ConnectionPoolMonitor.class);

    /**
     * Default period between checks of the connection pool, in minutes.
     */
    private static final int DEFAULT_CHECK_INTERVAL_MINUTES = 30;

    /**
     * Default threshold for the ratio of leased connections to max connections
     * that triggers a connection pool refresh.
     */
    private static final double DEFAULT_LEASED_RATIO_THRESHOLD = 0.8;

    /**
     * Default threshold for the number of pending connection requests
     * that triggers a connection pool refresh.
     */
    private static final int DEFAULT_PENDING_THRESHOLD = 5;

    private final AtomicReference<ConnectionManagerAwareHttpClient> httpClientRef;
    private final HttpClientSettings httpClientSettings;
    private final HttpClientFactory<ConnectionManagerAwareHttpClient> httpClientFactory;
    private final ScheduledExecutorService scheduler;
    private final int checkIntervalMinutes;
    private final double leasedRatioThreshold;
    private final int pendingThreshold;

    /**
     * Creates a new connection pool monitor with default settings.
     *
     * @param httpClientRef Reference to the HTTP client to monitor and refresh
     * @param httpClientSettings Settings to use when creating a new HTTP client
     * @param httpClientFactory Factory to use when creating a new HTTP client
     */
    public ConnectionPoolMonitor(
            AtomicReference<ConnectionManagerAwareHttpClient> httpClientRef,
            HttpClientSettings httpClientSettings,
            HttpClientFactory<ConnectionManagerAwareHttpClient> httpClientFactory) {
        this(httpClientRef, httpClientSettings, httpClientFactory,
             DEFAULT_CHECK_INTERVAL_MINUTES, DEFAULT_LEASED_RATIO_THRESHOLD, DEFAULT_PENDING_THRESHOLD);
    }

    /**
     * Creates a new connection pool monitor with custom settings.
     *
     * @param httpClientRef Reference to the HTTP client to monitor and refresh
     * @param httpClientSettings Settings to use when creating a new HTTP client
     * @param httpClientFactory Factory to use when creating a new HTTP client
     * @param checkIntervalMinutes Period between checks of the connection pool, in minutes
     * @param leasedRatioThreshold Threshold for the ratio of leased connections to max connections
     * @param pendingThreshold Threshold for the number of pending connection requests
     */
    public ConnectionPoolMonitor(
            AtomicReference<ConnectionManagerAwareHttpClient> httpClientRef,
            HttpClientSettings httpClientSettings,
            HttpClientFactory<ConnectionManagerAwareHttpClient> httpClientFactory,
            int checkIntervalMinutes,
            double leasedRatioThreshold,
            int pendingThreshold) {
        this.httpClientRef = httpClientRef;
        this.httpClientSettings = httpClientSettings;
        this.httpClientFactory = httpClientFactory;
        this.checkIntervalMinutes = checkIntervalMinutes;
        this.leasedRatioThreshold = leasedRatioThreshold;
        this.pendingThreshold = pendingThreshold;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "connection-pool-monitor");
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts the connection pool monitor.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::checkAndRefreshConnectionPool,
                                     checkIntervalMinutes,
                                     checkIntervalMinutes,
                                     TimeUnit.MINUTES);
        LOG.debug("Connection pool monitor started with check interval of " + checkIntervalMinutes + " minutes");
    }

    /**
     * Stops the connection pool monitor.
     */
    public void stop() {
        scheduler.shutdown();
        LOG.debug("Connection pool monitor stopped");
    }

    /**
     * Checks the connection pool stats and refreshes the connection pool if necessary.
     */
    private void checkAndRefreshConnectionPool() {
        try {
            ConnectionManagerAwareHttpClient httpClient = httpClientRef.get();
            if (httpClient == null) {
                return;
            }

            if (!(httpClient.getHttpClientConnectionManager() instanceof ConnPoolControl<?>)) {
                return;
            }

            PoolStats stats = ((ConnPoolControl<?>) httpClient.getHttpClientConnectionManager()).getTotalStats();
            int maxConnections = httpClientSettings.getMaxConnections();
            int leased = stats.getLeased();
            int pending = stats.getPending();
            double leasedRatio = (double) leased / maxConnections;

            LOG.debug(String.format("Connection pool stats: available=%d, leased=%d, pending=%d, max=%d, leasedRatio=%.2f",
                                   stats.getAvailable(), leased, pending, maxConnections, leasedRatio));

            if (leasedRatio >= leasedRatioThreshold || pending >= pendingThreshold) {
                LOG.warn(String.format(
                    "Connection pool approaching exhaustion: available=%d, leased=%d, pending=%d, max=%d, leasedRatio=%.2f. Refreshing connection pool.",
                    stats.getAvailable(), leased, pending, maxConnections, leasedRatio));
                refreshConnectionPool();
            }
        } catch (Exception e) {
            LOG.warn("Error checking connection pool stats", e);
        }
    }

    /**
     * Refreshes the connection pool by creating a new HTTP client and replacing the old one.
     */
    private void refreshConnectionPool() {
        try {
            // Create a new HTTP client with the same settings
            ConnectionManagerAwareHttpClient newHttpClient = httpClientFactory.create(httpClientSettings);

            // Get the old HTTP client
            ConnectionManagerAwareHttpClient oldHttpClient = httpClientRef.getAndSet(newHttpClient);

            // Shut down the old HTTP client's connection manager
            if (oldHttpClient != null) {
                IdleConnectionReaper.removeConnectionManager(oldHttpClient.getHttpClientConnectionManager());
                oldHttpClient.getHttpClientConnectionManager().shutdown();
            }

            LOG.info("Connection pool refreshed successfully");
        } catch (Exception e) {
            LOG.error("Error refreshing connection pool", e);
        }
    }
}
