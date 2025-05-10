package ds.tutorials.communication.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import distributed.NameServiceClient;

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
        
        // Register with name service
        NameServiceClient nameServiceClient = new NameServiceClient(nameServiceAddress);
        String hostAddress = "localhost"; // In production, this should be the actual host address
        nameServiceClient.registerService(SERVICE_NAME, hostAddress, port, PROTOCOL);
        
        // Start the server
        Server server = ServerBuilder.forPort(port)
                .addService(new ConcertServiceImpl(nameServiceAddress, dataDir))
                .build();
        
        System.out.println("ConcertServer started, listening on port " + port);
        server.start();
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down ConcertServer...");
            server.shutdown();
        }));
        
        server.awaitTermination();
    }
} 