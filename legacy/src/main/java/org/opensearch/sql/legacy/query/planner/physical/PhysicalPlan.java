/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.physical;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.JoinRequest;
import org.opensearch.action.search.JoinResponse;
import org.opensearch.action.search.StreamedJoinAction;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.search.SearchHit;
import org.opensearch.sql.legacy.domain.Field;
import org.opensearch.sql.legacy.query.join.TableInJoinRequestBuilder;
import org.opensearch.sql.legacy.query.planner.core.ExecuteParams;
import org.opensearch.sql.legacy.query.planner.core.Plan;
import org.opensearch.sql.legacy.query.planner.core.PlanNode.Visitor;
import org.opensearch.sql.legacy.query.planner.core.QueryParams;
import org.opensearch.sql.legacy.query.planner.logical.LogicalPlan;
import org.opensearch.sql.legacy.query.planner.physical.estimation.Estimation;
import org.opensearch.sql.legacy.query.planner.resource.ResourceManager;

/** Physical plan */
public class PhysicalPlan implements Plan {

  private static final Logger LOG = LogManager.getLogger();

  /** Optimized logical plan that being ready for physical planning */
  private final LogicalPlan logicalPlan;

  /** Root of physical plan tree */
  private PhysicalOperator<SearchHit> root;

  public PhysicalPlan(LogicalPlan logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  @Override
  public void traverse(Visitor visitor) {
    if (root != null) {
      root.accept(visitor);
    }
  }

  @Override
  public void optimize() {
    Estimation<SearchHit> estimation = new Estimation<>();
    logicalPlan.traverse(estimation);
    root = estimation.optimalPlan();
  }

  /** Execute physical plan after verifying if system is healthy at the moment */
  public List<SearchHit> execute(ExecuteParams params) {
    if (shouldReject(params)) {
      throw new IllegalStateException("Query request rejected due to insufficient resource");
    }

    QueryParams queryParams = logicalPlan.getParams();
    TableInJoinRequestBuilder left = queryParams.firstRequest();
    TableInJoinRequestBuilder right = queryParams.secondRequest();

    for (String include : left.getRequestBuilder().request().source().fetchSource().includes()) {
      left.getRequestBuilder().addFetchField(include);
    }
    for (String include : right.getRequestBuilder().request().source().fetchSource().includes()) {
      right.getRequestBuilder().addFetchField(include);
    }

    List<List<Map.Entry<Field, Field>>> joinConditions = queryParams.joinConditions();
    NodeClient client = (NodeClient) params.get(ExecuteParams.ExecuteParamType.CLIENT);
    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    client.executeLocally(StreamedJoinAction.INSTANCE, new JoinRequest(
            left.getRequestBuilder().request(),
            right.getRequestBuilder().request(),
            joinConditions.get(0).get(0).getKey().getName()
    ), new ActionListener<>() {
        @Override
        public void onResponse(JoinResponse joinResponse) {
          System.out.println("THE TICKET:");
          System.out.println(new String(joinResponse.getTicket().getBytes(), StandardCharsets.UTF_8));
            future.complete(joinResponse);
        }

        @Override
        public void onFailure(Exception e) {
          future.completeExceptionally(e);
        }
    });
      try {
          future.get();
      } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
      }

    try (PhysicalOperator<SearchHit> op = root) {
      return doExecutePlan(op, params);
    } catch (Exception e) {
      LOG.error("Error happened during execution", e);
      // Runtime error or circuit break. Should we return partial result to customer?
      throw new IllegalStateException("Error happened during execution", e);
    }
  }

  /** Reject physical plan execution of new query request if unhealthy */
  private boolean shouldReject(ExecuteParams params) {
    return !((ResourceManager) params.get(ExecuteParams.ExecuteParamType.RESOURCE_MANAGER))
        .isHealthy();
  }

  /** Execute physical plan in order: open, fetch result, close */
  private List<SearchHit> doExecutePlan(PhysicalOperator<SearchHit> op, ExecuteParams params)
      throws Exception {
    List<SearchHit> hits = new ArrayList<>();
    op.open(params);

    while (op.hasNext()) {
      hits.add(op.next().data());
    }

    if (LOG.isTraceEnabled()) {
      hits.forEach(hit -> LOG.trace("Final result row: {}", hit.getSourceAsMap()));
    }
    return hits;
  }
}
