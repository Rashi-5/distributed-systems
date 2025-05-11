package ds.tutorials.communication.server;

import concert.ConcertQueryServiceGrpc;
import concert.ConcertService;
import io.grpc.stub.StreamObserver;
import java.util.Map;
import java.util.stream.Collectors;

public class ConcertQueryServiceImpl extends ConcertQueryServiceGrpc.ConcertQueryServiceImplBase {
    private final Map<String, ConcertService.Concert.Builder> concerts;

    public ConcertQueryServiceImpl(Map<String, ConcertService.Concert.Builder> concerts) {
        this.concerts = concerts;
    }

    @Override
    public void listConcerts(ConcertService.Empty request, StreamObserver<ConcertService.ListConcertsResponse> responseObserver) {
        ConcertService.ListConcertsResponse response = ConcertService.ListConcertsResponse.newBuilder()
                .addAllConcerts(concerts.values().stream().map(ConcertService.Concert.Builder::build).collect(Collectors.toList()))
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
} 