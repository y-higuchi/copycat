/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.internal.replication;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import net.kuujo.copycat.CopycatException;
import net.kuujo.copycat.internal.cluster.RemoteNode;
import net.kuujo.copycat.internal.log.CopycatEntry;
import net.kuujo.copycat.internal.log.SnapshotEntry;
import net.kuujo.copycat.internal.state.FollowerController;
import net.kuujo.copycat.internal.state.StateContext;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.protocol.PingRequest;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SyncRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Node replicator.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class NodeReplicator {
  private static final Logger LOGGER = LoggerFactory.getLogger(NodeReplicator.class);
  private static final int BATCH_SIZE = 100;
  private final RemoteNode<?> node;
  private final StateContext state;
  private final Log log;
  // Index of the next log entry to send to the server
  private volatile long nextIndex;
  // Index of highest log entry known to be replicated on server; Initialized to 0
  private volatile long matchIndex = 0;
  private volatile long sendIndex;
  private volatile boolean open;
  private final TreeMap<Long, CompletableFuture<Long>> pingFutures = new TreeMap<>();
  private final Map<Long, CompletableFuture<Long>> replicateFutures = new ConcurrentHashMap<>(1024);

  public NodeReplicator(RemoteNode<?> node, StateContext state) {
    this.node = node;
    this.state = state;
    this.log = state.log();
    this.nextIndex = log.lastIndex() + 1;
    this.sendIndex = nextIndex;
  }

  /**
   * Returns the replica node.
   *
   * @return The replica node.
   */
  RemoteNode<?> node() {
    return node;
  }

  /**
   * Returns the index of the last entry known to be replicated to the replica.
   */
  long index() {
    return matchIndex;
  }

  /**
   * Opens the connection to the replica.
   */
  CompletableFuture<Void> open() {
    if (!open) {
      return node.client().connect().whenComplete((result, error) -> {
        if (error == null) {
          open = true;
        }
      });
    }
    return CompletableFuture.completedFuture(null);
  }


  /**
   * Pings the replica.
   */
  synchronized CompletableFuture<Long> ping(long index) {
    if (!open) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      future.completeExceptionally(new CopycatException("Connection not open"));
      return future;
    }

    if (index > matchIndex) {
      return replicate(index);
    }

    CompletableFuture<Long> future = new CompletableFuture<>();
    if (!pingFutures.isEmpty() && pingFutures.lastKey() >= index) {
      final Map.Entry<Long, CompletableFuture<Long>> pendingEntry = pingFutures.lastEntry();
      LOGGER.trace("Pending ping future for {} exist, suppressing {}", pendingEntry.getKey(), index);
      return pendingEntry.getValue();
    }

    pingFutures.put(index, future);

    PingRequest request = new PingRequest(state.nextCorrelationId(), state.currentTerm(), state.cluster().localMember().id(), index, log.containsEntry(index) ? log.<CopycatEntry>getEntry(index).term() : 0, state.commitIndex());
    LOGGER.debug("{} - Sent {} to {}", state.clusterManager().localNode(), request, node);
    node.client().ping(request).whenComplete((response, error) -> {
      if (error != null) {
        triggerPingFutures(index, error);
      } else {
        LOGGER.debug("{} - Received {} from {}", state.clusterManager().localNode(), response, node);
        if (response.status().equals(Response.Status.OK)) {
          if (response.term() > state.currentTerm()) {
            state.currentTerm(response.term());
            state.currentLeader(null);
            state.transition(FollowerController.class);
            triggerPingFutures(index, new CopycatException("Not the leader"));
          } else if (!response.succeeded()) {
            triggerPingFutures(index, new ProtocolException("Replica not in sync"));
          } else {
            triggerPingFutures(index);
          }
        } else {
          triggerPingFutures(index, response.error());
        }
      }
    });
    return future;
  }

  /**
   * Commits the given index to the replica.
   */
  CompletableFuture<Long> replicate(long index) {
    if (!open) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      future.completeExceptionally(new CopycatException("Connection not open"));
      return future;
    }

    if (index <= matchIndex) {
      return CompletableFuture.completedFuture(index);
    }

    CompletableFuture<Long> future = replicateFutures.get(index);
    if (future != null) {
      LOGGER.trace("Pending replicate Future exist for {}", index);
      return future;
    }

    future = new CompletableFuture<>();
    CompletableFuture<Long> existingFuture = replicateFutures.putIfAbsent(index, future);
    if (existingFuture != null) {
      return existingFuture;
    }

    if (index >= sendIndex) {
      replicate();
    }
    return future;
  }

  /**
   * Performs a commit operation.
   */
  private synchronized void replicate() {
    final long prevIndex = sendIndex - 1;
    final CopycatEntry prevEntry = log.getEntry(prevIndex);

    // Create a list of up to BATCH_SIZE entries to send to the follower.
    // We can only send one snapshot entry in any given request. So, if any of
    // the entries are snapshot entries, send all entries up to the snapshot and
    // then send snapshot entries individually.
    List<CopycatEntry> entries = new ArrayList<>(BATCH_SIZE);
    final long firstIndex = Math.max(sendIndex, log.firstIndex());
    long lastIndex = Math.min(sendIndex + BATCH_SIZE - 1, log.lastIndex());
    for (long i = firstIndex; i <= lastIndex; i++) {
      CopycatEntry entry = log.getEntry(i);
      if (entry instanceof SnapshotEntry) {
        if (entries.isEmpty()) {
          doSync(prevIndex, prevEntry, Collections.singletonList(entry));
        } else {
          doSync(prevIndex, prevEntry, entries);
        }
        return;
      } else {
        entries.add(entry);
      }
    }

    if (!entries.isEmpty()) {
      doSync(prevIndex, prevEntry, entries);
    }
  }

  /**
   * Sends a sync request.
   */
  private void doSync(final long prevIndex, final CopycatEntry prevEntry, final List<CopycatEntry> entries) {
    final long commitIndex = state.commitIndex();

    SyncRequest request = new SyncRequest(state.nextCorrelationId(), state.currentTerm(), state.clusterManager().localNode().member().id(), prevIndex, prevEntry != null ? prevEntry.term() : 0, entries, commitIndex);

    // advance sendIndex
    sendIndex = Math.max(sendIndex, prevIndex + entries.size() + 1);

    LOGGER.debug("{} - Sent {} to {}", state.clusterManager().localNode(), request, node);
    node.client().sync(request).whenComplete((response, error) -> {
      if (error != null) {
        LOGGER.warn("{} - {} to {} Failed", state.clusterManager().localNode(), request, node, error);
        // prevIndex + 1 == (sendIndex before this call advanced it)
        triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size(), error);
        // reset sendIndex
        sendIndex = Math.max(nextIndex, Math.min(sendIndex, prevIndex + 1));
      } else {
        LOGGER.debug("{} - Received {} from {}", state.clusterManager().localNode(), response, node);
        if (response.status().equals(Response.Status.OK)) {
          if (response.succeeded()) {
            // Update the next index to send and the last index known to be replicated.
            if (!entries.isEmpty()) {
              nextIndex = Math.max(nextIndex, prevIndex + entries.size() + 1);
              matchIndex = Math.max(matchIndex, prevIndex + entries.size());
              triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size());
              replicate();
            }
          } else {
            if (response.term() > state.currentTerm()) {
              triggerReplicateFutures(prevIndex, prevIndex, new CopycatException("Not the leader"));
              state.transition(FollowerController.class);
            } else {
              // If replication failed then use the last log index indicated by
              // the replica in the response to generate a new nextIndex. This allows
              // us to skip repeatedly replicating one entry at a time if it's not
              // necessary.
              nextIndex = sendIndex = Math.max(response.lastLogIndex() + 1, log.firstIndex());
              replicate();
            }
          }
        } else {
          LOGGER.warn("{} - {} to {} failed", state.clusterManager().localNode(), request, node, error);
          triggerReplicateFutures(prevIndex + 1, prevIndex + entries.size(), response.error());
          // reset sendIndex
          sendIndex = Math.max(nextIndex, Math.min(sendIndex, prevIndex + 1));
        }
      }
    });

    // TODO: replicate() should immediately be called recursively for pipelining.
    // replicate();
  }

  /**
   * Triggers ping futures with a completion result.
   */
  private synchronized void triggerPingFutures(long index) {
    NavigableMap<Long, CompletableFuture<Long>> matchFutures = pingFutures.headMap(index, true);
    for (Map.Entry<Long, CompletableFuture<Long>> entry : matchFutures.entrySet()) {
      entry.getValue().complete(index);
    }
    matchFutures.clear();
  }

  /**
   * Triggers response futures with an error result.
   */
  private synchronized void triggerPingFutures(long index, Throwable t) {
    CompletableFuture<Long> future = pingFutures.remove(index);
    if (future != null) {
      future.completeExceptionally(t);
    }
  }

  /**
   * Triggers replicate futures with an error result.
   */
  private void triggerReplicateFutures(long startIndex, long endIndex, Throwable t) {
    if (endIndex >= startIndex) {
      for (long i = startIndex; i <= endIndex; i++) {
        CompletableFuture<Long> future = replicateFutures.remove(i);
        if (future != null) {
          future.completeExceptionally(t);
        }
      }
    }
  }

  /**
   * Triggers replicate futures with a completion result
   */
  private void triggerReplicateFutures(long startIndex, long endIndex) {
    if (endIndex >= startIndex) {
      for (long i = startIndex; i <= endIndex; i++) {
        CompletableFuture<Long> future = replicateFutures.remove(i);
        if (future != null) {
          future.complete(i);
        }
      }
    }
  }

  /**
   * Closes the replica.
   */
  CompletableFuture<Void> close() {
    return node.client().close().whenComplete((result, error) -> open = false);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof NodeReplicator && ((NodeReplicator) object).node.equals(node);
  }

  @Override
  public int hashCode() {
    int hashCode = 7;
    hashCode = 37 * hashCode + node.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return node.toString();
  }

}
