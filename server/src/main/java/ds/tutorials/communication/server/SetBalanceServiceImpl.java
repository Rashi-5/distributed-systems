package ds.tutorials.communication.server;

import ds.tutorial.communication.grpc.generated.SetBalanceRequest;
import ds.tutorial.communication.grpc.generated.SetBalanceServiceGrpc;
import ds.tutorial.communication.grpc.generated.SetBalanceResponse;
import ds.tutorials.sycnhronization.DistributedTx;
import ds.tutorials.sycnhronization.DistributedTxCoordinator;
import ds.tutorials.sycnhronization.DistributedTxListener;
import ds.tutorials.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.*;

public class SetBalanceServiceImpl extends SetBalanceServiceGrpc.SetBalanceServiceImplBase implements DistributedTxListener {

    private static final Map<String, Double> accountBalances = new HashMap<>();
    private BankServer server;
    private ManagedChannel channel = null;
    SetBalanceServiceGrpc.SetBalanceServiceBlockingStub clientStub = null;
    private Pair<String, Double> tempDataHolder;
    private boolean transactionStatus = false;

    public SetBalanceServiceImpl(BankServer server) {
        this.server = server;
    }

    @Override
    public void setBalance(ds.tutorial.communication.grpc.generated.SetBalanceRequest request, io.grpc.stub.StreamObserver<ds.tutorial.communication.grpc.generated.SetBalanceResponse> responseObserver) {

        String accountId = request.getAccountId();
        double value = request.getBalance();
        if (server.isLeader()){
// Act as primary
            try {
                System.out.println("Updating account balance as Primary");
                startDistributedTx(accountId, value);
                updateSecondaryServers(accountId, value);
                System.out.println("going to perform");
                if (value > 0){((DistributedTxCoordinator)server.getTransaction()).perform();
                } else {
                    ((DistributedTxCoordinator)server.getTransaction()).sendGlobalAbort();
                }
            } catch (Exception e) {
                System.out.println("Error while updating the account balance" + e.getMessage());
                e.printStackTrace();
            }
        } else {
// Act As Secondary
            if (request.getIsSentByPrimary()) {
                System.out.println("Updating account balance on secondary, on Primary's command");
                startDistributedTx(accountId, value);
                if (value != 0.0d) {
                    ((DistributedTxParticipant)server.getTransaction()).voteCommit();
                } else {
                    ((DistributedTxParticipant)server.getTransaction()).voteAbort();
                }
            } else {
                SetBalanceResponse response =
                        callPrimary(accountId, value);
                if (response.getStatus()) {
                    transactionStatus = true;
                }
            }
        }
        SetBalanceResponse response = SetBalanceResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void updateBalance() {

        if (tempDataHolder != null) {
            String accountId = tempDataHolder.getKey();
            double value = tempDataHolder.getValue();
            server.setAccountBalance(accountId, value);
            System.out.println("Account " + accountId + " updated to value " + value + " committed");
            tempDataHolder = null;
        }
    }

    private SetBalanceResponse callServer(String accountId, double value, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = SetBalanceServiceGrpc.newBlockingStub(channel);
        SetBalanceRequest request = SetBalanceRequest
                .newBuilder()
                .setAccountId(accountId)
                .setBalance(value)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        SetBalanceResponse response = clientStub.setBalance(request);
        return response;
    }

    private SetBalanceResponse callPrimary(String accountId, double value) {

        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];

        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(accountId, value, false, IPAddress, port);
    }

    private void updateSecondaryServers(String accountId, double value) throws KeeperException, InterruptedException, KeeperException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            if (data.length < 2) {
                System.err.println("Invalid secondary server data: " + Arrays.toString(data));
                continue;
            }
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(accountId, value, true, IPAddress, port);
        }
    }

    private void startDistributedTx(String accountId, double value) {
        try {
            server.getTransaction().start(accountId, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<>(accountId, value);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onGlobalCommit() {
        updateBalance();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction Aborted by the Coordinator");
    }
} 