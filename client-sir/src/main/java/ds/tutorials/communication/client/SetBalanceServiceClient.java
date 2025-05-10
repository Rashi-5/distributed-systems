package ds.tutorials.communication.client;

import ds.tutorial.communication.grpc.generated.SetBalanceRequest;
import ds.tutorial.communication.grpc.generated.SetBalanceResponse;
import ds.tutorial.communication.grpc.generated.SetBalanceServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class SetBalanceServiceClient {
    private ManagedChannel channel = null;
    SetBalanceServiceGrpc.SetBalanceServiceBlockingStub clientStub = null;
    String host = null;
    int port = -1;

    public SetBalanceServiceClient (String host, int port) {
        this.host = host;
        this.port = port;
    }

    public void initializeConnection () {
        try {
            System.out.println("Initializing Connecting to server at " + host + ":" + port);
            channel = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            clientStub = SetBalanceServiceGrpc.newBlockingStub(channel);
        } catch (Exception e) {
            System.err.println("Error connecting to server: " + e.getMessage());
            System.exit(1);
        }
    }

    public void closeConnection() {
        try {
            if (channel != null) {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    public void processUserRequests() throws InterruptedException {
        while (true) {
            Scanner userInput = new Scanner(System.in);
            System.out.println("\nEnter Account ID, amount to set the balance (e.g., 123,100.50):");
            String input = userInput.nextLine().trim();
            String[] parts = input.split(",");
            
            if (parts.length != 2) {
                System.out.println("Invalid input format. Please enter in the format: accountId,amount");
                continue;
            }

            try {
                String accountId = parts[0].trim();
                double amount = Double.parseDouble(parts[1].trim());
                System.out.println("Requesting server to set the account balance to " + amount + " for " + accountId);
                SetBalanceRequest request = SetBalanceRequest
                        .newBuilder()
                        .setAccountId(accountId)
                        .setValue(amount)
                        .setIsSentByPrimary(false)
                        .build();
                SetBalanceResponse response = clientStub.setBalance(request);
                System.out.printf("Transaction Status: " + (response.getStatus() ? "Successful" : "Failed"));
            } catch (NumberFormatException e) {
                System.out.println("Invalid amount format. Please enter a valid number.");
            }
            Thread.sleep(1000);
        }
    }
}
