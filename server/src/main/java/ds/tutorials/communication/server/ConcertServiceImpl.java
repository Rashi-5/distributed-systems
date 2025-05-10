package ds.tutorials.communication.server;

import concert.ConcertServiceGrpc;
import concert.ConcertServiceOuterClass.*;
import io.grpc.stub.StreamObserver;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import ds.tutorials.synchronization.DistributedLock;
import org.apache.zookeeper.KeeperException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class ConcertServiceImpl extends ConcertServiceGrpc.ConcertServiceImplBase {
    // In-memory data store for concerts and reservations
    private final Map<String, Concert.Builder> concerts = new ConcurrentHashMap<>();
    private final Map<String, ReservationResponse> reservations = new ConcurrentHashMap<>();
    private DistributedLock distributedLock;

    public ConcertServiceImpl() {
        try {
            DistributedLock.setZooKeeperURL("127.0.0.1:2181");
            this.distributedLock = new DistributedLock("concert-lock");
        } catch (IOException | KeeperException | InterruptedException e) {
            throw new RuntimeException("Failed to initialize distributed lock", e);
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
        try {
            distributedLock.acquireLock();
            try {
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
                // Atomicity: check and update seat and after-party in one step
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
                    // All-or-nothing: only reserve if both are available
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
                            .setMessage("Reservation successful.")
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