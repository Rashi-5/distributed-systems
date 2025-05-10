package ds.tutorials.communication.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public class ConcertServer {
    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 50051; // Default port, can be parameterized
        Server server = ServerBuilder.forPort(port)
                .addService(new ConcertServiceImpl())
                .build();
        System.out.println("ConcertServer started, listening on port " + port);
        server.start();
        server.awaitTermination();
    }
} 