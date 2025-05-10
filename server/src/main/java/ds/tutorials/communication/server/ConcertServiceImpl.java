package ds.tutorials.communication.server;

import concert.ConcertServiceGrpc;
import concert.ConcertServiceOuterClass.*;
import io.grpc.stub.StreamObserver;
import ds.tutorials.synchronization.DistributedTxCoordinator;
import ds.tutorials.synchronization.DistributedTxListener;
import ds.tutorials.synchronization.DistributedLock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.zookeeper.KeeperException;
import java.io.*;
import java.nio.file.*;

public class ConcertServiceImpl extends ConcertServiceGrpc.ConcertServiceImplBase implements DistributedTxListener {
    // In-memory data store for concerts and reservations
    private final Map<String, Concert.Builder> concerts = new ConcurrentHashMap<>();
    private final Map<String, ReservationResponse> reservations = new ConcurrentHashMap<>();
    private final DistributedTxCoordinator coordinator;
    private final String nodeId;
    private final String dataDir;
    private final String nameServiceAddress;
    private final DistributedLock distributedLock;

    public ConcertServiceImpl(String nameServiceAddress, String dataDir) {
        this.nodeId = UUID.randomUUID().toString();
        this.coordinator = new DistributedTxCoordinator(this);
        this.dataDir = dataDir;
        this.nameServiceAddress = nameServiceAddress;
        try {
            DistributedLock.setZooKeeperURL("127.0.0.1:2181");
            this.distributedLock = new DistributedLock("concert-lock");
        } catch (IOException | KeeperException | InterruptedException e) {
            throw new RuntimeException("Failed to initialize distributed lock", e);
        }
        loadData();
    }

    private void loadData() {
        try {
            Files.createDirectories(Paths.get(dataDir));
            loadConcerts();
            loadReservations();
        } catch (IOException e) {
            System.err.println("Failed to load data: " + e.getMessage());
        }
    }

    private void loadConcerts() throws IOException {
        File concertsFile = new File(dataDir, "concerts.dat");
        if (concertsFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(concertsFile))) {
                @SuppressWarnings("unchecked")
                Map<String, Concert.Builder> loadedConcerts = (Map<String, Concert.Builder>) ois.readObject();
                concerts.putAll(loadedConcerts);
            } catch (ClassNotFoundException e) {
                System.err.println("Failed to load concerts: " + e.getMessage());
            }
        }
    }

    private void loadReservations() throws IOException {
        File reservationsFile = new File(dataDir, "reservations.dat");
        if (reservationsFile.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(reservationsFile))) {
                @SuppressWarnings("unchecked")
                Map<String, ReservationResponse> loadedReservations = (Map<String, ReservationResponse>) ois.readObject();
                reservations.putAll(loadedReservations);
            } catch (ClassNotFoundException e) {
                System.err.println("Failed to load reservations: " + e.getMessage());
            }
        }
    }

    private void saveData() {
        try {
            saveConcerts();
            saveReservations();
        } catch (IOException e) {
            System.err.println("Failed to save data: " + e.getMessage());
        }
    }

    private void saveConcerts() throws IOException {
        File concertsFile = new File(dataDir, "concerts.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(concertsFile))) {
            oos.writeObject(new HashMap<>(concerts));
        }
    }

    private void saveReservations() throws IOException {
        File reservationsFile = new File(dataDir, "reservations.dat");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(reservationsFile))) {
            oos.writeObject(new HashMap<>(reservations));
        }
    }

    @Override
    public void onGlobalCommit() {
        saveData();
    }

    @Override
    public void onGlobalAbort() {
        // No need to save data on abort
    }

    private boolean performAtomicOperation(String operationId, Runnable operation) {
        try {
            coordinator.start(operationId, nodeId);
            operation.run();
            return coordinator.perform();
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void addConcert(AddConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
        Concert concert = request.getConcert();
        concerts.put(concert.getId(), concert.toBuilder());
        ConcertResponse response = ConcertResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Concert added successfully.")
                .setConcert(concert)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void updateConcert(UpdateConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
        Concert concert = request.getConcert();
        concerts.put(concert.getId(), concert.toBuilder());
        ConcertResponse response = ConcertResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Concert updated successfully.")
                .setConcert(concert)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void cancelConcert(CancelConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
        String concertId = request.getConcertId();
        concerts.remove(concertId);
        ConcertResponse response = ConcertResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Concert cancelled successfully.")
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void addTicketStock(AddTicketStockRequest request, StreamObserver<ConcertResponse> responseObserver) {
        try {
            distributedLock.acquireLock();
            try {
                String concertId = request.getConcertId();
                Concert.Builder concert = concerts.get(concertId);
                if (concert == null) {
                    ConcertResponse response = ConcertResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert not found.")
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                if (request.getAfterParty()) {
                    int current = concert.getAfterPartyTickets();
                    concert.setAfterPartyTickets(current + request.getCount());
                } else {
                    Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
                    seatTiers.put(request.getTier(), seatTiers.getOrDefault(request.getTier(), 0) + request.getCount());
                    concert.putAllSeatTiers(seatTiers);
                }
                concerts.put(concertId, concert);
                ConcertResponse response = ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Ticket stock updated.")
                        .setConcert(concert.build())
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } finally {
                distributedLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertResponse response = ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void updateTicketPrice(UpdateTicketPriceRequest request, StreamObserver<ConcertResponse> responseObserver) {
        try {
            distributedLock.acquireLock();
            try {
                String concertId = request.getConcertId();
                Concert.Builder concert = concerts.get(concertId);
                if (concert == null) {
                    ConcertResponse response = ConcertResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert not found.")
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                Map<String, Double> prices = new HashMap<>(concert.getPricesMap());
                prices.put(request.getTier(), request.getPrice());
                concert.putAllPrices(prices);
                concerts.put(concertId, concert);
                ConcertResponse response = ConcertResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Ticket price updated.")
                        .setConcert(concert.build())
                        .build();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } finally {
                distributedLock.releaseLock();
            }
        } catch (Exception e) {
            ConcertResponse response = ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    @Override
    public void listConcerts(Empty request, StreamObserver<ListConcertsResponse> responseObserver) {
        ListConcertsResponse response = ListConcertsResponse.newBuilder()
                .addAllConcerts(concerts.values().stream().map(Concert.Builder::build).collect(Collectors.toList()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void reserveTickets(ReserveTicketsRequest request, StreamObserver<ReservationResponse> responseObserver) {
        String operationId = "reserve_" + UUID.randomUUID().toString();
        boolean success = performAtomicOperation(operationId, () -> {
            String concertId = request.getConcertId();
            Concert.Builder concert = concerts.get(concertId);
            if (concert == null) {
                throw new RuntimeException("Concert not found");
            }
            
            synchronized (concert) {
                int availableSeats = concert.getSeatTiersMap().getOrDefault(request.getTier(), 0);
                int availableAfterParty = concert.getAfterPartyTickets();
                
                if (request.getCount() > availableSeats) {
                    throw new RuntimeException("Not enough seats available");
                }
                
                if (request.getAfterParty() && request.getCount() > availableAfterParty) {
                    throw new RuntimeException("Not enough after-party tickets available");
                }
                
                Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
                seatTiers.put(request.getTier(), availableSeats - request.getCount());
                concert.putAllSeatTiers(seatTiers);
                
                if (request.getAfterParty()) {
                    concert.setAfterPartyTickets(availableAfterParty - request.getCount());
                }
                
                concerts.put(concertId, concert);
                String reservationId = UUID.randomUUID().toString();
                ReservationResponse response = ReservationResponse.newBuilder()
                        .setSuccess(true)
                        .setMessage("Reservation successful")
                        .setReservationId(reservationId)
                        .build();
                reservations.put(reservationId, response);
            }
        });
        
        if (success) {
            responseObserver.onNext(reservations.get(operationId));
        } else {
            responseObserver.onNext(ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Failed to reserve tickets")
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void bulkReserve(BulkReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {
        try {
            distributedLock.acquireLock();
            try {
                // Same logic as reserveTickets, but for group
                String concertId = request.getConcertId();
                Concert.Builder concert = concerts.get(concertId);
                if (concert == null) {
                    ReservationResponse response = ReservationResponse.newBuilder()
                            .setSuccess(false)
                            .setMessage("Concert not found.")
                            .build();
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    return;
                }
                synchronized (concert) {
                    int availableSeats = concert.getSeatTiersMap().getOrDefault(request.getTier(), 0);
                    int availableAfterParty = concert.getAfterPartyTickets();
                    if (request.getCount() > availableSeats) {
                        ReservationResponse response = ReservationResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Not enough seats available.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                    if (request.getAfterParty() && request.getCount() > availableAfterParty) {
                        ReservationResponse response = ReservationResponse.newBuilder()
                                .setSuccess(false)
                                .setMessage("Not enough after-party tickets available.")
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                        return;
                    }
                    Map<String, Integer> seatTiers = new HashMap<>(concert.getSeatTiersMap());
                    seatTiers.put(request.getTier(), availableSeats - request.getCount());
                    concert.putAllSeatTiers(seatTiers);
                    if (request.getAfterParty()) {
                        concert.setAfterPartyTickets(availableAfterParty - request.getCount());
                    }
                    concerts.put(concertId, concert);
                    String reservationId = UUID.randomUUID().toString();
                    ReservationResponse response = ReservationResponse.newBuilder()
                            .setSuccess(true)
                            .setMessage("Bulk reservation successful.")
                            .setReservationId(reservationId)
                            .build();
                    reservations.put(reservationId, response);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                }
            } finally {
                distributedLock.releaseLock();
            }
        } catch (Exception e) {
            ReservationResponse response = ReservationResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Distributed lock error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }
} 