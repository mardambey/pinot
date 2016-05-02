/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.transport.scattergather;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.MoreExecutors;
import com.linkedin.pinot.common.metrics.BrokerMeter;
import com.linkedin.pinot.common.metrics.BrokerMetrics;
import com.linkedin.pinot.common.metrics.MetricsHelper;
import com.linkedin.pinot.common.metrics.MetricsHelper.TimerContext;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.transport.common.BucketingSelection;
import com.linkedin.pinot.transport.common.CompositeFuture;
import com.linkedin.pinot.transport.common.CompositeFuture.GatherModeOnError;
import com.linkedin.pinot.transport.common.KeyedFuture;
import com.linkedin.pinot.transport.common.ReplicaSelection;
import com.linkedin.pinot.transport.common.ReplicaSelectionGranularity;
import com.linkedin.pinot.transport.common.SegmentId;
import com.linkedin.pinot.transport.common.SegmentIdSet;
import com.linkedin.pinot.transport.netty.NettyClientConnection;
import com.linkedin.pinot.transport.netty.NettyClientConnection.ResponseFuture;
import com.linkedin.pinot.transport.pool.KeyedPool;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.MetricName;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;


/**
 *
 * Scatter-Gather implementation
 *
 */
public class ScatterGatherImpl implements ScatterGather {

  private static final Logger LOGGER = LoggerFactory.getLogger(ScatterGatherImpl.class);

  private final ExecutorService _executorService;

  private final Histogram _latency = MetricsHelper.newHistogram(null, new MetricName(ScatterGatherImpl.class,
      "ScatterGatherLatency"), false);

  /**
   * Connection Pool for sending scatter-gather requests
   */
  private final KeyedPool<ServerInstance, NettyClientConnection> _connPool;

  public ScatterGatherImpl(KeyedPool<ServerInstance, NettyClientConnection> pool, ExecutorService service) {
    _connPool = pool;
    _executorService = service;
  }

  @Override
  public CompositeFuture<ServerInstance, ByteBuf> scatterGather(ScatterGatherRequest scatterRequest,
      final ScatterGatherStats scatterGatherStats, final BrokerMetrics brokerMetrics)
      throws InterruptedException {
    ScatterGatherRequestContext ctxt = new ScatterGatherRequestContext(scatterRequest);

    // Build services to segmentId group map
    buildInvertedMap(ctxt);

    LOGGER.debug("Context : {}", ctxt);

    // do Selection for each segment-set/segmentId
    selectServices(ctxt);

    return sendRequest(ctxt, scatterGatherStats, brokerMetrics);
  }

  /**
   *
   * Helper Function to send scatter-request. This method should be called after the servers are selected
   *
   * @param ctxt Scatter-Gather Request context with selected servers for each request.
   * @param scatterGatherStats
   * @param brokerMetrics for updating stats
   * @return a composite future representing the gather process.
   * @throws InterruptedException
   */
  protected CompositeFuture<ServerInstance, ByteBuf> sendRequest(ScatterGatherRequestContext ctxt,
      final ScatterGatherStats scatterGatherStats, final BrokerMetrics brokerMetrics)
      throws InterruptedException {
    TimerContext t = MetricsHelper.startTimer();

    // Servers are expected to be selected at this stage
    Map<ServerInstance, SegmentIdSet> mp = ctxt.getSelectedServers();

    CountDownLatch requestDispatchLatch = new CountDownLatch(mp.size());

    //Use same thread to send request once the connection is available
    ExecutorService executor = MoreExecutors.sameThreadExecutor();

    // async checkout of connections and then dispatch of request
    List<SingleRequestHandler> handlers = new ArrayList<SingleRequestHandler>(mp.size());

    int i = 0;
    for (Entry<ServerInstance, SegmentIdSet> e : mp.entrySet()) {
      scatterGatherStats.initServer(e.getKey().toString());
      SingleRequestHandler handler =
          new SingleRequestHandler(_connPool, e.getKey(), ctxt.getRequest(), e.getValue(), ctxt.getTimeRemaining(),
              requestDispatchLatch, brokerMetrics);
      // Submit to thread-pool for checking-out and sending request
      _executorService.submit(handler);
      handlers.add(handler);
    }

    // Create the composite future for returning
    CompositeFuture<ServerInstance, ByteBuf> response =
        new CompositeFuture<ServerInstance, ByteBuf>("scatterRequest", GatherModeOnError.SHORTCIRCUIT_AND);

    // Wait for requests to be sent
    long timeRemaining = ctxt.getTimeRemaining();
    boolean sentSuccessfully = requestDispatchLatch.await(timeRemaining, TimeUnit.MILLISECONDS);

    if (sentSuccessfully) {
      List<KeyedFuture<ServerInstance, ByteBuf>> responseFutures =
          new ArrayList<KeyedFuture<ServerInstance, ByteBuf>>();
      for (SingleRequestHandler h : handlers) {
        responseFutures.add(h.getResponseFuture());
        final String server = h.getServer().toString();
        scatterGatherStats.setSendStartTimeMillis(server, h.getConnStartTimeMillis());
        scatterGatherStats.setConnStartTimeMillis(server, h.getStartDelayMillis());
        scatterGatherStats.setSendCompletionTimeMillis(server, h.getSendCompletionTimeMillis());
      }
      response.start(responseFutures);
    } else {
      LOGGER.error("Request (" + ctxt.getRequest().getRequestId() + ") not sent completely within time ("
          + timeRemaining + " ms) !! Cancelling !!. NumSentFailed:" + requestDispatchLatch.getCount());
      response.start(null);

      // Some requests were not event sent (possibly because of checkout !!)
      // and so we cancel all of them here
      for (SingleRequestHandler h : handlers) {
        LOGGER.info("Request to {} was sent successfully:{}", h.getServer(), h.isSent());
        h.cancel();
      }
    }
    t.stop();
    _latency.update(t.getLatencyMs());
    return response;
  }

  /**
   * Merge segment-sets which have the same set of servers. If 2 segmentIds have overlapping
   * set of servers, they are not merged. If there is predefined-selection for a segmentId,
   * a separate entry is added for those in the inverted map.
   * @param requestContext Scatter gather request
   */
  protected void buildInvertedMap(ScatterGatherRequestContext requestContext) {
    ScatterGatherRequest request = requestContext.getRequest();
    Map<ServerInstance, SegmentIdSet> segmentIdToInstanceMap = request.getSegmentsServicesMap();

    Map<List<ServerInstance>, SegmentIdSet> instanceToSegmentMap = new HashMap<List<ServerInstance>, SegmentIdSet>();

    BucketingSelection sel = request.getPredefinedSelection();

    for (ServerInstance serverInstance : segmentIdToInstanceMap.keySet()) {
      instanceToSegmentMap.put(Arrays.asList(serverInstance), segmentIdToInstanceMap.get(serverInstance));
    }
    requestContext.setInvertedMap(instanceToSegmentMap);
  }

  private <T> void mergePartitionGroup(Map<T, SegmentIdSet> instanceToSegmentMap, T instances, SegmentIdSet pg) {

    SegmentIdSet pg2 = instanceToSegmentMap.get(instances);
    if (null != pg2) {
      pg2.addSegments(pg.getSegments());
    } else {
      instanceToSegmentMap.put(instances, pg);
    }
  }

  private <T> void mergePartitionGroup(Map<T, SegmentIdSet> instanceToSegmentMap, T instances, SegmentId p) {
    SegmentIdSet pg2 = instanceToSegmentMap.get(instances);
    if (null != pg2) {
      pg2.addSegment(p);
    } else {
      SegmentIdSet pg1 = new SegmentIdSet();
      pg1.addSegment(p);
      instanceToSegmentMap.put(instances, pg1);
    }
  }

  protected void selectServices(ScatterGatherRequestContext requestContext) {
    ScatterGatherRequest request = requestContext.getRequest();

    if (request.getReplicaSelectionGranularity() == ReplicaSelectionGranularity.SEGMENT_ID_SET) {
      selectServicesPerPartitionGroup(requestContext);
    } else {
      selectServicesPerPartition(requestContext);
    }
  }

  /**
   * For each segment-set in the instanceToSegmentMap, we select one (or more speculative) servers
   *
   * @param requestContext
   */
  private void selectServicesPerPartitionGroup(ScatterGatherRequestContext requestContext) {
    Map<ServerInstance, SegmentIdSet> selectedServers = new HashMap<ServerInstance, SegmentIdSet>();
    ScatterGatherRequest request = requestContext.getRequest();
    Map<List<ServerInstance>, SegmentIdSet> instanceToSegmentMap = requestContext.getInvertedMap();
    //int numDuplicateRequests = request.getNumSpeculativeRequests();
    ReplicaSelection selection = request.getReplicaSelection();
    for (Entry<List<ServerInstance>, SegmentIdSet> e : instanceToSegmentMap.entrySet()) {
      ServerInstance s = selection.selectServer(e.getValue().getOneSegment(), e.getKey(), request.getHashKey());
      mergePartitionGroup(selectedServers, s, e.getValue());

      /**
       * TODO:
       * We can easily add speculative execution here. The below code will pick a distinct server
       * for the segmentId, This entry needs to be maintained in a separate container in ScatterGatherRequestContext
       * Then in sndRequest, we need to construct SelectingFuture for the pairs of Future corresponding to original
       * and speculative(duplicate) request.
       *
       int numServers = e.getKey().size();

      // Pick Unique servers for speculative request
      int numIterations = Math.min(numServers - 1, numDuplicateRequests);
      for (int i = 0, c = 0; i < numIterations; i++, c++)
      {
        ServerInstance s1 = e.getKey().get(c);
        if ( s.equals(s1))
        {
          c++;
          s1 = e.getKey().get(c);
        }
        mergePartitionGroup(selectedServers, s1, e.getValue());
        //TODO: speculative servers need to be maintained in a separate entry in ScatterGatherRequestContext
      }
       **/
    }
    requestContext.setSelectedServers(selectedServers);
  }

  /**
   * For each segmentId in the instanceToSegmentMap, we select one (or more speculative) servers
   *
   * @param requestContext
   */
  private void selectServicesPerPartition(ScatterGatherRequestContext requestContext) {
    Map<ServerInstance, SegmentIdSet> selectedServers = new HashMap<ServerInstance, SegmentIdSet>();
    ScatterGatherRequest request = requestContext.getRequest();
    Map<List<ServerInstance>, SegmentIdSet> instanceToSegmentMap = requestContext.getInvertedMap();
    ReplicaSelection selection = request.getReplicaSelection();
    for (Entry<List<ServerInstance>, SegmentIdSet> e : instanceToSegmentMap.entrySet()) {
      SegmentId firstPartition = null;
      for (SegmentId p : e.getValue().getSegments()) {
        /**
         * For selecting the server, we always use first segmentId in the group. This will provide
         * more chance for fanning out the query
         */
        if (null == firstPartition) {
          firstPartition = p;
        }
        ServerInstance s = selection.selectServer(firstPartition, e.getKey(), request.getHashKey());

        mergePartitionGroup(selectedServers, s, p);
      }
    }

    requestContext.setSelectedServers(selectedServers);
  }

  public static class ScatterGatherRequestContext {
    private final long _startTimeMs;

    private final ScatterGatherRequest _request;

    private Map<List<ServerInstance>, SegmentIdSet> _invertedMap;

    private Map<ServerInstance, SegmentIdSet> _selectedServers;

    protected ScatterGatherRequestContext(ScatterGatherRequest request) {
      _request = request;
      _startTimeMs = System.currentTimeMillis();
    }

    public Map<List<ServerInstance>, SegmentIdSet> getInvertedMap() {
      return _invertedMap;
    }

    public void setInvertedMap(Map<List<ServerInstance>, SegmentIdSet> invertedMap) {
      _invertedMap = invertedMap;
    }

    public ScatterGatherRequest getRequest() {
      return _request;
    }

    public Map<ServerInstance, SegmentIdSet> getSelectedServers() {
      return _selectedServers;
    }

    public void setSelectedServers(Map<ServerInstance, SegmentIdSet> selectedServers) {
      _selectedServers = selectedServers;
    }

    @Override
    public String toString() {
      return "ScatterGatherRequestContext [_request=" + _request + ", _invertedMap=" + _invertedMap
          + ", _selectedServers=" + _selectedServers + "]";
    }

    /**
     * Return time remaining in MS
     * @return
     */
    public long getTimeRemaining() {
      long timeout = _request.getRequestTimeoutMS();

      if (timeout < 0) {
        return Long.MAX_VALUE;
      }

      long timeElapsed = System.currentTimeMillis() - _startTimeMs;
      return (timeout - timeElapsed);
    }
  }

  /**
   * Runnable responsible for sending a request to the server once the connection is available
   *
   */
  public static class SingleRequestHandler implements Runnable {
    private final static int MAX_CONN_RETRIES = 3;  // Max retries for getting a connection
    // Scatter Request
    private final ScatterGatherRequest _request;
    // List Of Partitions to be queried on the server
    private final SegmentIdSet _segmentIds;
    // Server Instance to be queried
    private final ServerInstance _server;
    // Latch to signal completion of dispatching request
    private final CountDownLatch _requestDispatchLatch;
    // Future for the response
    private volatile ResponseFuture _responseFuture;

    // Connection Pool: Used if we need to checkin/destroy object in case of timeout
    private final KeyedPool<ServerInstance, NettyClientConnection> _connPool;

    // Track if request has been dispatched
    private final AtomicBoolean _isSent = new AtomicBoolean(false);

    // Cancel dispatching request
    private final AtomicBoolean _isCancelled = new AtomicBoolean(false);

    // Remaining time budget to connect and process the request.
    private final long _timeoutMS;

    private final long _initTime;
    private final BrokerMetrics _brokerMetrics;
    private long _startTime;
    private long _endTime;

    public SingleRequestHandler(KeyedPool<ServerInstance, NettyClientConnection> connPool, ServerInstance server,
        ScatterGatherRequest request, SegmentIdSet segmentIds, long timeoutMS, CountDownLatch latch,
        final BrokerMetrics brokerMetrics) {
      _connPool = connPool;
      _server = server;
      _request = request;
      _segmentIds = segmentIds;
      _requestDispatchLatch = latch;
      _timeoutMS = timeoutMS;
      _initTime = System.currentTimeMillis();
      _brokerMetrics = brokerMetrics;
    }

    @Override
    public synchronized void run() {
      try {
        _startTime = System.currentTimeMillis();
        runInternal();
      } finally {
        _endTime = System.currentTimeMillis();
      }
    }

    public long getConnStartTimeMillis() {
      return _startTime - _initTime;
    }

    public long getSendCompletionTimeMillis() {
      return _endTime > _initTime ? _endTime - _initTime : 0;
    }

    // If the 'run' gets called more than 5ms after we created this object, something is wrong.
    public long getStartDelayMillis() {
      return _startTime - _initTime;
    }

    private void runInternal() {
      if (_isCancelled.get()) {
        LOGGER.error("Request {} to server {} cancelled even before request is sent !! Not sending request",
            _request.getRequestId(), _server);
        _requestDispatchLatch.countDown();
        return;
      }

      NettyClientConnection conn = null;
      boolean gotConnection = false;
      boolean error = true;
      try {
        KeyedFuture<ServerInstance, NettyClientConnection> c = _connPool.checkoutObject(_server);

        byte[] serializedRequest = _request.getRequestForService(_server, _segmentIds);
        long timeRemaining = _timeoutMS - (System.currentTimeMillis() - _startTime);
        int ntries = 0;
        // Try a maximum of pool size objects.
        while (true) {
          if (timeRemaining <= 0) {
            c.cancel(true);
            throw new TimeoutException("Timed out trying to connect to " + _server + "(timeout=" + _timeoutMS + "ms,ntries=" + ntries + ")");
          }
          conn = c.getOne(timeRemaining, TimeUnit.MILLISECONDS);
          // conn may be null if we cannot get any connection from the pool. This condition can happen either
          // due to a timeout (server host is switched off, or cable disconnected), or due to immediate connection refusal
          // (host is up, but server JVM is not running, or not up yet). It will also get a null when the AsyncPoolImpl.create()
          // is not able to create a connection, and this one was a waiting request.
          // In either case, there is no point in retrying  this request
          if (conn != null && conn.validate()) {
            gotConnection = true;
            break;
          }
          // If we get a null error map, then it is most likely a case of "Connection Refused" from remote.
          // The connect errors are obtained from two different objects -- 'conn' and 'c'.
          // We pick the error from 'c' here, if we find it. Unfortunately there is not a way (now) to pass the
          // error from 'c' to 'conn' (need to do it via AsyncPoolImpl)
          Map<ServerInstance, Throwable> errorMap = c.getError();
          String errStr = "";
          if (errorMap != null && errorMap.containsKey(_server)) {
            errStr = errorMap.get(_server).getMessage();
          }
          LOGGER.warn("Destroying invalid conn {}:{}", conn, errStr);
          if (conn != null) {
            _connPool.destroyObject(_server, conn);
          }
          if (++ntries == MAX_CONN_RETRIES-1) {
            throw new RuntimeException("Could not connect to " + _server + " after " + ntries + "attempts(timeRemaining=" + timeRemaining + "ms)");
          }
          c = _connPool.checkoutObject(_server);
          timeRemaining = _timeoutMS - (System.currentTimeMillis() - _startTime);
        }
        ByteBuf req = Unpooled.wrappedBuffer(serializedRequest);
        _responseFuture = conn.sendRequest(req, _request.getRequestId(), timeRemaining);
        _isSent.set(true);
        LOGGER.debug("Response Future is : {}", _responseFuture);
        error = false;
      } catch (TimeoutException e1) {
        LOGGER.error("Timed out waiting for connection for server ({})({})(gotConnection={}). Setting error future",
            _server, _request.getRequestId(), gotConnection, e1);
        _responseFuture = new ResponseFuture(_server, e1, "Error Future for request " + _request.getRequestId());
      } catch (Exception e) {
        LOGGER.error("Got exception sending request ({})(gotConnection={}). Setting error future",
            _request.getRequestId(), gotConnection, e);
        _responseFuture = new ResponseFuture(_server, e, "Error Future for request " + _request.getRequestId());
      } finally {
        _requestDispatchLatch.countDown();
        BrokerRequest brokerRequest = (BrokerRequest) _request.getBrokerRequest();
        if (error) {
          if (gotConnection) {
            _brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.REQUEST_DROPPED_DUE_TO_SEND_ERROR, 1);
          } else {
            _brokerMetrics.addMeteredQueryValue(brokerRequest, BrokerMeter.REQUEST_DROPPED_DUE_TO_CONNECTION_ERROR, 1);
          }
        }
      }
    }

    /**
     * Cancel checking-out request if possible. If in unsafe state (request already sent),
     * discard the connection from the pool.
     */
    public synchronized void cancel() {
      if (_isCancelled.get()) {
        return;
      }

      _isCancelled.set(true);

      if (_isSent.get()) {
        /**
         * If the request has already been sent, we cancel the
         * response future. The connection will automatically be returned to the pool if response
         * arrived within timeout or discarded if timeout happened. No need to handle it here.
         */
        _responseFuture.cancel(true);
      }
    }

    public ServerInstance getServer() {
      return _server;
    }

    public ResponseFuture getResponseFuture() {
      return _responseFuture;
    }

    public boolean isSent() {
      return _isSent.get();
    }
  }

  public Histogram getLatency() {
    return _latency;
  }

  /**
   * This is used to checkin the connections once the responses/errors are obtained
   *
   */
  /*
   * This is not needed as The Client connection resources has the ability to
   * self-checkin back to the pool. Look at {@Link PooledNettyClientResourceManager.PooledClientConnection
   *
  public static class RequestCompletionHandler implements Runnable
  {

    public final Collection<SingleRequestHandler> _handlers;
    public final KeyedPool<ServerInstance, NettyClientConnection> _connPool;

    public RequestCompletionHandler(Collection<SingleRequestHandler> handlers,
        KeyedPool<ServerInstance, NettyClientConnection> connPool)
    {
      _handlers = handlers;
      _connPool = connPool;
    }

    @Override
    public void run() {

      List<SingleRequestHandler> pendingRequestHandlers = new ArrayList<SingleRequestHandler>();

      // In the first pass checkin all completed connections
      for ( SingleRequestHandler h : _handlers)
      {
        ResponseFuture f = h.getResponseFuture();
        if (! f.getState().isCompleted())
        {
          pendingRequestHandlers.add(h);
        } else {
          try {
            _connPool.checkinObject(h.getServer(), h.getConnFuture().getOne());
          } catch (Exception e) {
            //Not expected
            throw new IllegalStateException("Not expected !!");
          }
        }
      }

      //In the second pass, wait for request completion for stragglers and checkin
      //TODO: Need to have a bounded wait and handle failures
      for (SingleRequestHandler p : pendingRequestHandlers )
      {
        try
        {
          // Wait for response to be returned
          p.getResponseFuture().getOne();
          _connPool.checkinObject(p.getServer(), p.getConnFuture().getOne());
        } catch (Exception e) {
          // Discard the connection if we get an exception
          try {
            _connPool.destroyObject(p.getServer(), p.getConnFuture().getOne());
            //TODO: We need to handle case when checking out of connFuture is blocking.
            // In that case, we need to cancel it and avoid race-condition (cancel vs valid checkout)
          } catch (Exception e1) {
            //Not expected
            throw new IllegalStateException("Not expected !!");
          }
        }
      }

    }

  }
   */

}
