package net.kuujo.copycat;

import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import net.jodah.concurrentunit.ConcurrentTestCase;
import net.kuujo.copycat.cluster.Cluster;
import net.kuujo.copycat.cluster.LocalClusterConfig;
import net.kuujo.copycat.cluster.Member;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.protocol.AsyncLocalProtocol;
import net.kuujo.copycat.protocol.LocalProtocol;
import net.kuujo.copycat.spi.protocol.AsyncProtocol;
import net.kuujo.copycat.spi.protocol.Protocol;
import net.kuujo.copycat.test.TestStateMachine;

public abstract class AbstractCopycatTest extends ConcurrentTestCase {
  protected List<CopycatState> stateChanges = new Vector<>();

  protected List<Copycat> buildCluster(int clusterSize) {
    Protocol protocol = new LocalProtocol();
    return IntStream.range(0, clusterSize)
        .<Copycat>mapToObj(
            i -> {
              LocalClusterConfig config = buildClusterConfig(clusterSize, i);
              Copycat copycat =
                  Copycat.builder()
                      .withStateMachine(new TestStateMachine())
                      .withLog(new InMemoryLog())
                      .withCluster(new Cluster<Member>(config))
                      .withProtocol(protocol)
                      .withConfig(new CopycatConfig().withElectionTimeout(5000))
                      .build();
              recordStateChangesFor(copycat);
              return copycat;
            })
        .collect(Collectors.toList());
  }

  protected List<AsyncCopycat> buildAsyncCluster(int clusterSize) {
    AsyncProtocol protocol = new AsyncLocalProtocol();
    return IntStream.range(0, clusterSize)
        .<AsyncCopycat>mapToObj(
            i -> {
              LocalClusterConfig config = buildClusterConfig(clusterSize, i);
              AsyncCopycat copycat =
                  AsyncCopycat.builder()
                      .withStateMachine(new TestStateMachine())
                      .withLog(new InMemoryLog())
                      .withCluster(new Cluster<Member>(config))
                      .withProtocol(protocol)
                      .build();
              recordStateChangesFor(copycat);
              return copycat;
            })
        .collect(Collectors.toList());
  }

  private LocalClusterConfig buildClusterConfig(int clusterSize, int localNode) {
    LocalClusterConfig config = new LocalClusterConfig();
    config.setLocalMember(String.valueOf(localNode));
    for (int j = 0; j < clusterSize; j++) {
      if (j != localNode)
        config.addRemoteMember(String.valueOf(j));
    }
    return config;
  }

  private void recordStateChangesFor(AbstractCopycat copycat) {
    copycat.on().stateChange().run(e -> {
      stateChanges.add(e.state());
    });
  }
}
