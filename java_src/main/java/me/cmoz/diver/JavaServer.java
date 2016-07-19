package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.name.Named;
import com.google.inject.Inject;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import lombok.extern.slf4j.Slf4j;
import org.hbase.async.HBaseClient;

@Slf4j
class JavaServer extends AbstractExecutionThreadService {

  /** Represents the local OTP node. */
  private final OtpNode otpNode;
  /** The Asynchbase HBase client. */
  private final HBaseClient hbaseClient;
  /** The name to register for this server's mailbox. */
  private final String registeredProcName;

  /** A mailbox for exchanging messages with Erlang processes. */
  private OtpMbox mbox;

  @Inject
  public JavaServer(
      final OtpNode otpNode,
      final HBaseClient hbaseClient,
      @Named("erlang.registered_proc_name") final String registeredProcName) {
    this.otpNode = otpNode;
    this.hbaseClient = hbaseClient;
    this.registeredProcName = registeredProcName;
  }

  private void handle(final OtpErlangTuple tuple) throws OtpErlangDecodeException {
    final OtpErlangObject[] elements = tuple.elements();
    final OtpErlangAtom opType = (OtpErlangAtom) elements[0];
    switch (opType.atomValue()) {
    case "stop":
      stopAsync();
      break;
    case "$gen_call":
      final OtpErlangTuple from = (OtpErlangTuple) elements[1];
      final OtpErlangTuple req = (OtpErlangTuple) elements[2];
      handleCall(from, req);
      break;
    default:
      final String message = String.format("Bad message: \"%s\"", tuple);
      throw new OtpErlangDecodeException(message);
    }
  }

  private void handleCall(final OtpErlangTuple from, final OtpErlangTuple req)
      throws OtpErlangDecodeException {
    final OtpErlangObject[] elements = req.elements();
    final OtpErlangAtom reqType = (OtpErlangAtom) elements[0];
    switch (reqType.atomValue()) {
    case "client_stats":
      reply(from, TypeUtil.clientStats(hbaseClient.stats()));
      break;
    case "delete":
      final OtpErlangBinary table1 = (OtpErlangBinary) elements[1];
      final OtpErlangBinary key1 = (OtpErlangBinary) elements[2];
      final OtpErlangBinary family1 = (OtpErlangBinary) elements[3];
      final OtpErlangList qualifiers1 = (OtpErlangList) elements[4];
      hbaseClient.delete(TypeUtil.deleteRequest(table1, key1, family1, qualifiers1))
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "ensure_table_exists":
      final OtpErlangBinary table2 = (OtpErlangBinary) elements[1];
      hbaseClient.ensureTableExists(table2.binaryValue())
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "ensure_table_family_exists":
      final OtpErlangBinary table3 = (OtpErlangBinary) elements[1];
      final OtpErlangBinary key2 = (OtpErlangBinary) elements[2];
      hbaseClient.ensureTableFamilyExists(table3.binaryValue(), key2.binaryValue())
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "flush":
      hbaseClient.flush()
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "get":
      final OtpErlangBinary table4 = (OtpErlangBinary) elements[1];
      final OtpErlangBinary key3 = (OtpErlangBinary) elements[2];
      final OtpErlangBinary family2 = (OtpErlangBinary) elements[3];
      OtpErlangBinary qualifier = null;
      if(elements.length > 4) {
        qualifier = (OtpErlangBinary) elements[4];
      }
      hbaseClient.get(TypeUtil.getRequest(table4, key3, family2, qualifier))
          .addCallback(new GenServerGetCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "get_flush_interval":
      final short flushInterval1 = hbaseClient.getFlushInterval();
      reply(from, TypeUtil.tuple(new OtpErlangAtom("ok"), new OtpErlangShort(flushInterval1)));
      break;
    case "get_increment_buffer_size":
      final int incrementBufferSize1 = hbaseClient.getIncrementBufferSize();
      reply(from, TypeUtil.tuple(new OtpErlangAtom("ok"), new OtpErlangInt(incrementBufferSize1)));
      break;
    case "pid":
      reply(from, TypeUtil.tuple(reqType, mbox.self()));
      break;
    case "prefetch_meta":
      final OtpErlangBinary table5 = (OtpErlangBinary) elements[1];
      hbaseClient.prefetchMeta(table5.binaryValue())
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "put":
      final OtpErlangBinary table6 = (OtpErlangBinary) elements[1];
      final OtpErlangBinary key4 = (OtpErlangBinary) elements[2];
      final OtpErlangBinary family3 = (OtpErlangBinary) elements[3];
      final OtpErlangList qualifiers2 = (OtpErlangList) elements[4];
      final OtpErlangList values2 = (OtpErlangList) elements[5];
      hbaseClient.put(TypeUtil.putRequest(table6, key4, family3, qualifiers2, values2))
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;
    case "set_flush_interval":
      final OtpErlangShort flushInterval2 = (OtpErlangShort) elements[1];
      try {
        final short resp1 = hbaseClient.setFlushInterval(flushInterval2.shortValue());
        reply(from, TypeUtil.tuple(new OtpErlangAtom("ok"), new OtpErlangShort(resp1)));
      } catch (final OtpErlangRangeException e) {
        reply(from, TypeUtil.tuple(new OtpErlangAtom("error"), new OtpErlangString(e.getClass().getName()), new OtpErlangString(e.getLocalizedMessage())));
      }
      break;
    case "set_increment_buffer_size":
      final OtpErlangInt incrementBufferSize2 = (OtpErlangInt) elements[1];
      try {
        final int resp2 = hbaseClient.setIncrementBufferSize(incrementBufferSize2.intValue());
        reply(from, TypeUtil.tuple(new OtpErlangAtom("ok"), new OtpErlangInt(resp2)));
      } catch (final OtpErlangRangeException e) {
        reply(from, TypeUtil.tuple(new OtpErlangAtom("error"), new OtpErlangString(e.getClass().getName()), new OtpErlangString(e.getLocalizedMessage())));
      }
      break;
    default:
      final String message = String.format("Invalid request: \"%s\"", req);
      throw new OtpErlangDecodeException(message);
    }
  }

  private void reply(final OtpErlangTuple from, OtpErlangObject reply) {
    final OtpErlangTuple resp = TypeUtil.tuple(from.elementAt(1), reply);
    mbox.send((OtpErlangPid) from.elementAt(0), resp);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      final OtpErlangObject msg = mbox.receive();
      try {
        handle((OtpErlangTuple) msg);
      } catch (final OtpErlangDecodeException | ClassCastException | ArrayIndexOutOfBoundsException e) {
        log.error(e.getMessage());
        log.info("Unrecognised message, ignored.");
      }
    }
  }

  @Override
  protected void startUp() throws Exception {
    mbox = otpNode.createMbox(registeredProcName);
  }

  @Override
  protected void shutDown() throws Exception {
    mbox.close();
    otpNode.close();
    // wait for hbaseClient to flush pending requests
    hbaseClient.shutdown().joinUninterruptibly(15000); // in ms
  }

}
