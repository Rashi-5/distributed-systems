package ds.tutorials.communication.client;

import java.io.IOException;

public class MainClass {
    public static void main(String[] args) throws InterruptedException, IOException {
        if (args.length != 3) {
            System.out.println("Usage CheckBalanceServiceClient <host> <port> <s(et)|c(heck)>");
            System.exit(1);
        }

        String host = args[0];
        int port = Integer.parseInt(args[1].trim());
        String operation = args[2];

        if ("s".equals(operation)) {
            SetBalanceServiceClient client = new SetBalanceServiceClient(host, port);
            client.initializeConnection();
            client.processUserRequests();
            client.closeConnection();
        } else if ("c".equals(operation)) {
            CheckBalanceServiceClient client = new CheckBalanceServiceClient(host, port);
            client.initializeConnection();
            client.processUserRequests();
            client.closeConnection();
        } else {
            System.out.println("Invalid operation. Use 's' for set balance or 'c' for check balance");
            System.exit(1);
        }
    }
}
