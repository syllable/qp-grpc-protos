import com.google.protobuf.Timestamp;
import com.qpalym.grpc.GetUploadLogRequest;
import com.qpalym.grpc.UploadLog;
import com.qpalym.grpc.UploadLogResponse;
import com.qpalym.grpc.UploadLogServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoFileDescriptorSupplier;
import io.grpc.stub.StreamObserver;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * Compatibility harness for the protobuf-java 4.x + grpc-java combination.
 *
 * <p>protoc 4.x emits gencode that hard-fails against a protobuf-java 3.x runtime, and
 * grpc-protobuf still pulls 3.x in transitively, so this exercises the real gRPC code
 * paths (marshalling, service/file descriptors, StatusProto) rather than just compiling.
 *
 * <p>Run with:
 * <pre>
 * mvn test-compile dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=target/cp.txt
 * java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" VerifyCompat
 * </pre>
 */
public class VerifyCompat {

  static class Impl extends UploadLogServiceGrpc.UploadLogServiceImplBase {
    @Override
    public void getLogs(GetUploadLogRequest req, StreamObserver<UploadLogResponse> obs) {
      // Echo the request's Timestamp back, so a field round-trip is actually verified.
      obs.onNext(UploadLogResponse.newBuilder()
          .addLog(UploadLog.newBuilder()
              .setId(42L)
              .setBucketName("bucket")
              .setKey("some/key")
              .setBytes(1024L)
              .setFilename("f.txt")
              .setDeleted(true)
              .setUploadedAt(req.getUploadedAfter())
              .setUploadDurationMs(17L)
              .build())
          .build());
      obs.onCompleted();
    }
  }

  static void check(String label, boolean ok, Object detail) {
    System.out.println((ok ? "  PASS  " : "  FAIL  ") + label + (ok ? "" : "  -> " + detail));
    if (!ok) {
      failures++;
    }
  }

  static int failures = 0;

  public static void main(String[] args) throws Exception {
    System.out.println("protobuf-java jar : " + com.google.protobuf.Message.class
        .getProtectionDomain().getCodeSource().getLocation());
    System.out.println("grpc-api jar      : " + io.grpc.Status.class
        .getProtectionDomain().getCodeSource().getLocation());
    System.out.println();

    // 1. In-process client/server round trip through grpc's ProtoUtils marshaller.
    String name = InProcessServerBuilder.generateName();
    Server server = InProcessServerBuilder.forName(name).directExecutor()
        .addService(new Impl()).build().start();
    ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
    try {
      Timestamp sent = Timestamp.newBuilder().setSeconds(1700000000L).setNanos(123456789).build();
      UploadLogResponse resp = UploadLogServiceGrpc.newBlockingStub(channel)
          .getLogs(GetUploadLogRequest.newBuilder().setUploadedAfter(sent).build());

      check("in-process RPC returns 1 log", resp.getLogCount() == 1, resp.getLogCount());
      UploadLog log = resp.getLog(0);
      check("int64 field survived marshalling", log.getId() == 42L, log.getId());
      check("string field survived marshalling", "some/key".equals(log.getKey()), log.getKey());
      check("bool field survived marshalling", log.getDeleted(), log.getDeleted());
      check("nested Timestamp survived marshalling", sent.equals(log.getUploadedAt()),
          log.getUploadedAt());
    } finally {
      channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
      server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
    }

    // 2. FileDescriptor path used by grpc reflection / ProtoFileDescriptorSupplier.
    Object schema = UploadLogServiceGrpc.getServiceDescriptor().getSchemaDescriptor();
    check("service exposes ProtoFileDescriptorSupplier",
        schema instanceof ProtoFileDescriptorSupplier, schema);
    if (schema instanceof ProtoFileDescriptorSupplier) {
      var fd = ((ProtoFileDescriptorSupplier) schema).getFileDescriptor();
      check("FileDescriptor resolves from protobuf 4.x runtime",
          "com.qpalym.grpc".equals(fd.getPackage()), fd.getPackage());
    }

    // 3. StatusProto (the other protobuf-dependent class in grpc-protobuf) uses Any.
    try {
      var status = com.google.rpc.Status.newBuilder().setCode(3).setMessage("boom").build();
      StatusRuntimeException e = io.grpc.protobuf.StatusProto.toStatusRuntimeException(status);
      check("StatusProto.toStatusRuntimeException works",
          e.getStatus().getCode() == io.grpc.Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
      check("StatusProto round trips the message back",
          "boom".equals(e.getStatus().getDescription()), e.getStatus().getDescription());
    } catch (Throwable t) {
      check("StatusProto.toStatusRuntimeException works", false, t);
    }

    System.out.println();
    System.out.println(failures == 0 ? "ALL CHECKS PASSED" : failures + " CHECK(S) FAILED");
    System.exit(failures == 0 ? 0 : 1);
  }
}
