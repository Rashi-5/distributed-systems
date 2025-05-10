package ds.tutorials.communication.client;

import concert.ConcertServiceGrpc;
import concert.ConcertServiceOuterClass.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.*;

public class ConcertClient {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 50051;
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        ConcertServiceGrpc.ConcertServiceBlockingStub stub = ConcertServiceGrpc.newBlockingStub(channel);

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\nConcert Ticket Reservation System");
            System.out.println("1. List Concerts");
            System.out.println("2. Add Concert");
            System.out.println("3. Update Concert");
            System.out.println("4. Cancel Concert");
            System.out.println("5. Reserve Tickets");
            System.out.println("6. Add/Update Seat Tier");
            System.out.println("7. Add After-Party Tickets");
            System.out.println("8. Bulk Reserve (Coordinator)");
            System.out.println("9. Exit");
            System.out.print("Choose an option: ");
            String option = scanner.nextLine();
            if (option.equals("1")) {
                ListConcertsResponse response = stub.listConcerts(Empty.newBuilder().build());
                System.out.println("Available Concerts:");
                for (Concert concert : response.getConcertsList()) {
                    System.out.println("- " + concert.getName() + " (ID: " + concert.getId() + ")");
                    System.out.println("  Date: " + concert.getDate());
                    System.out.println("  Seat Tiers: " + concert.getSeatTiersMap());
                    System.out.println("  After-Party Tickets: " + concert.getAfterPartyTickets());
                    System.out.println("  Prices: " + concert.getPricesMap());
                }
            } else if (option.equals("2")) {
                System.out.print("Concert Name: ");
                String name = scanner.nextLine();
                System.out.print("Date (YYYY-MM-DD): ");
                String date = scanner.nextLine();
                String id = UUID.randomUUID().toString();
                Concert concert = Concert.newBuilder()
                        .setId(id)
                        .setName(name)
                        .setDate(date)
                        .build();
                AddConcertRequest req = AddConcertRequest.newBuilder().setConcert(concert).build();
                ConcertResponse resp = stub.addConcert(req);
                System.out.println(resp.getMessage());
            } else if (option.equals("3")) {
                System.out.print("Concert ID to update: ");
                String id = scanner.nextLine();
                System.out.print("New Name: ");
                String name = scanner.nextLine();
                System.out.print("New Date (YYYY-MM-DD): ");
                String date = scanner.nextLine();
                Concert concert = Concert.newBuilder()
                        .setId(id)
                        .setName(name)
                        .setDate(date)
                        .build();
                UpdateConcertRequest req = UpdateConcertRequest.newBuilder().setConcert(concert).build();
                ConcertResponse resp = stub.updateConcert(req);
                System.out.println(resp.getMessage());
            } else if (option.equals("4")) {
                System.out.print("Concert ID to cancel: ");
                String id = scanner.nextLine();
                CancelConcertRequest req = CancelConcertRequest.newBuilder().setConcertId(id).build();
                ConcertResponse resp = stub.cancelConcert(req);
                System.out.println(resp.getMessage());
            } else if (option.equals("5")) {
                System.out.print("Concert ID: ");
                String concertId = scanner.nextLine();
                System.out.print("Seat Tier: ");
                String tier = scanner.nextLine();
                System.out.print("Number of Tickets: ");
                int count = Integer.parseInt(scanner.nextLine());
                System.out.print("After Party (true/false): ");
                boolean afterParty = Boolean.parseBoolean(scanner.nextLine());
                System.out.print("Customer ID: ");
                String customerId = scanner.nextLine();
                ReserveTicketsRequest req = ReserveTicketsRequest.newBuilder()
                        .setConcertId(concertId)
                        .setTier(tier)
                        .setCount(count)
                        .setAfterParty(afterParty)
                        .setCustomerId(customerId)
                        .build();
                ReservationResponse resp = stub.reserveTickets(req);
                System.out.println(resp.getMessage() + " Reservation ID: " + resp.getReservationId());
            } else if (option.equals("6")) {
                System.out.print("Concert ID: ");
                String concertId = scanner.nextLine();
                System.out.print("Seat Tier Name: ");
                String tier = scanner.nextLine();
                System.out.print("Number of Seats to Add: ");
                int count = Integer.parseInt(scanner.nextLine());
                System.out.print("Price: ");
                double price = Double.parseDouble(scanner.nextLine());
                AddTicketStockRequest stockReq = AddTicketStockRequest.newBuilder()
                        .setConcertId(concertId)
                        .setTier(tier)
                        .setCount(count)
                        .setAfterParty(false)
                        .build();
                ConcertResponse stockResp = stub.addTicketStock(stockReq);
                UpdateTicketPriceRequest priceReq = UpdateTicketPriceRequest.newBuilder()
                        .setConcertId(concertId)
                        .setTier(tier)
                        .setPrice(price)
                        .build();
                ConcertResponse priceResp = stub.updateTicketPrice(priceReq);
                System.out.println(stockResp.getMessage());
                System.out.println(priceResp.getMessage());
            } else if (option.equals("7")) {
                System.out.print("Concert ID: ");
                String concertId = scanner.nextLine();
                System.out.print("Number of After-Party Tickets to Add: ");
                int count = Integer.parseInt(scanner.nextLine());
                AddTicketStockRequest req = AddTicketStockRequest.newBuilder()
                        .setConcertId(concertId)
                        .setTier("")
                        .setCount(count)
                        .setAfterParty(true)
                        .build();
                ConcertResponse resp = stub.addTicketStock(req);
                System.out.println(resp.getMessage());
            } else if (option.equals("8")) {
                System.out.print("Concert ID: ");
                String concertId = scanner.nextLine();
                System.out.print("Seat Tier: ");
                String tier = scanner.nextLine();
                System.out.print("Number of Tickets: ");
                int count = Integer.parseInt(scanner.nextLine());
                System.out.print("After Party (true/false): ");
                boolean afterParty = Boolean.parseBoolean(scanner.nextLine());
                System.out.print("Group ID: ");
                String groupId = scanner.nextLine();
                BulkReserveRequest req = BulkReserveRequest.newBuilder()
                        .setConcertId(concertId)
                        .setTier(tier)
                        .setCount(count)
                        .setAfterParty(afterParty)
                        .setGroupId(groupId)
                        .build();
                ReservationResponse resp = stub.bulkReserve(req);
                System.out.println(resp.getMessage() + " Reservation ID: " + resp.getReservationId());
            } else if (option.equals("9")) {
                break;
            } else {
                System.out.println("Invalid option.");
            }
        }
        channel.shutdown();
    }
}
