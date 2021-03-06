package com.linkedin.thirdeye.dashboard.util;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import com.linkedin.thirdeye.client.ThirdEyeRawResponse;
import com.linkedin.thirdeye.client.ThirdEyeRequest;
import com.linkedin.thirdeye.dashboard.api.QueryResult;

public class QueryCache {
  private final ExecutorService executorService;
  private final ThirdEyeClientMap clientMap;

  public QueryCache(ThirdEyeClientMap clientMap, ExecutorService executorService) {
    this.executorService = executorService;
    this.clientMap = clientMap;
  }

  public void clear() throws Exception {
    clientMap.clear();
  }

  public QueryResult getQueryResult(String serverUri, ThirdEyeRequest request) throws Exception {
    ThirdEyeRawResponse rawResponse = clientMap.get(serverUri).getRawResponse(request);
    return QueryResult.fromThirdEyeResponse(rawResponse);
  }

  public Future<QueryResult> getQueryResultAsync(final String serverUri,
      final ThirdEyeRequest request) throws Exception {
    return executorService.submit(new Callable<QueryResult>() {
      @Override
      public QueryResult call() throws Exception {
        return getQueryResult(serverUri, request);
      }
    });
  }

}
