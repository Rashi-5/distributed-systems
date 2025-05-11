package ds.tutorials.communication.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import distributed.NameServiceClient;
import ds.tutorials.communication.server.LeaderElection;
import org.apache.zookeeper.KeeperException;

public class ConcertServer {
    private static final String SERVICE_NAME = "concert-service";
    private static final String PROTOCOL = "grpc";
    
    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 3) {
            System.out.println("Usage: ConcertServer <port> <nameServiceAddress> <dataDir>");
            System.exit(1);
        }
        
        int port = Integer.parseInt(args[0]);
        String nameServiceAddress = args[1];
        String dataDir = args[2];

        // Leader election setup
        LeaderElection leaderElection;
        try {
            leaderElection = new LeaderElection("127.0.0.1:2181");
            leaderElection.setOnElectedLeader(() -> System.out.println("[LeaderElection] This node is now the LEADER."));
            leaderElection.setOnElectedFollower(() -> System.out.println("[LeaderElection] This node is a FOLLOWER."));
            leaderElection.volunteerForLeadership();
            leaderElection.electLeader();
        } catch (KeeperException | IOException | InterruptedException e) {
            System.err.println("Failed to start leader election: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        if (!leaderElection.isLeader()) {
            System.out.println("This node is not the leader. Waiting for leadership...");
            // Wait until this node becomes leader
            while (!leaderElection.isLeader()) {
                Thread.sleep(2000);
            }
            System.out.println("This node has become the leader. Starting server...");
        }
        
        // Register with name service
        NameServiceClient nameServiceClient = new NameServiceClient(nameServiceAddress);
        String hostAddress = "localhost"; // In production, this should be the actual host address
        nameServiceClient.registerService(SERVICE_NAME, hostAddress, port, PROTOCOL);
        
        // Start the server
        ConcertCommandServiceImpl commandService = new ConcertCommandServiceImpl(nameServiceAddress, dataDir);
        ConcertQueryServiceImpl queryService = new ConcertQueryServiceImpl(commandService.getConcerts());
        Server server = ServerBuilder.forPort(port)
                .addService(commandService)
                .addService(queryService)
                .build();
        
        System.out.println("ConcertServer started, listening on port " + port);
        server.start();
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down ConcertServer...");
            server.shutdown();
            try { leaderElection.close(); } catch (Exception ignore) {}
        }));
        
        server.awaitTermination();
    }
} 