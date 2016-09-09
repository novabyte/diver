package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.name.Named;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.hbase.async.*;

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

  public static final OtpErlangAtom ATOM_OK = new OtpErlangAtom("ok");
  public static final OtpErlangAtom ATOM_ERROR = new OtpErlangAtom("error");

  public static final OtpErlangAtom ATOM_TRUE = new OtpErlangAtom("true");
  public static final OtpErlangAtom ATOM_FALSE = new OtpErlangAtom("false");

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

    case "pid":
      reply(from, TypeUtil.tuple(ATOM_OK, mbox.self()));
      break;

    case "prefetch_meta":
      final OtpErlangBinary table6 = (OtpErlangBinary) elements[1];
      hbaseClient.prefetchMeta(table6.binaryValue())
              .addCallback(new GenServerOkCallback(from, mbox))
              .addErrback(new GenServerErrback(from, mbox));
      break;

    case "get_conf":
      reply(from, handleGetConf((OtpErlangAtom)elements[1]));
      break;
    case "set_conf":
      reply(from, handleSetConf(elements));
      break;

    case "get":
      final OtpErlangBinary table4 = (OtpErlangBinary) elements[1];
      final OtpErlangBinary key3 = (OtpErlangBinary) elements[2];
      OtpErlangBinary family2 = null;
      OtpErlangBinary qualifier = null;
      if(elements.length > 3) {
        family2 = (OtpErlangBinary) elements[3];
      }
      if(elements.length > 4) {
        qualifier = (OtpErlangBinary) elements[4];
      }
      hbaseClient.get(TypeUtil.getRequest(table4, key3, family2, qualifier))
          .addCallback(new GenServerGetCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;

    case "scan":
      final OtpErlangBinary table5 = (OtpErlangBinary) elements[1];
      final OtpErlangList options = (OtpErlangList) elements[2];
      final OtpErlangRef ref = (OtpErlangRef) elements[3];
      final Scanner scanner = hbaseClient.newScanner(table5.binaryValue());
      final AsyncScanner asyncScanner = new AsyncScanner(from, mbox, ref, scanner, options);
      asyncScanner.start();
      reply(from, ATOM_OK);
      break;

    case "put":
      hbaseClient.put(parsePut((OtpErlangTuple)elements[1]))
          .addCallback(new GenServerOkCallback(from, mbox))
          .addErrback(new GenServerErrback(from, mbox));
      break;

    case "compare_and_set":
      OtpErlangBinary expected = (OtpErlangBinary)elements[2];
      hbaseClient.compareAndSet(parsePut((OtpErlangTuple)elements[1]), expected.binaryValue())
          .addCallback(new GenServerCallback<Object, Boolean>(from, mbox) {
            @Override
            protected OtpErlangObject handle(Boolean bool) {
              return TypeUtil.tuple(ATOM_OK, bool ? ATOM_TRUE : ATOM_FALSE);
            }
          })
          .addErrback(new GenServerErrback(from, mbox));
      break;

    case "increment":
      final AtomicIncrementRequest incrReq = new AtomicIncrementRequest(
              ((OtpErlangBinary) elements[1]).binaryValue(),
              ((OtpErlangBinary) elements[2]).binaryValue(),
              ((OtpErlangBinary) elements[3]).binaryValue(),
              ((OtpErlangBinary) elements[4]).binaryValue()
      );
      hbaseClient.atomicIncrement(incrReq)
          .addCallback(new GenServerCallback<Object, Long>(from, mbox) {
            @Override
            protected OtpErlangObject handle(Long value) {
              return TypeUtil.tuple(ATOM_OK, new OtpErlangLong(value));
            }
          })
          .addErrback(new GenServerErrback(from, mbox));
      break;

    case "delete":
      final OtpErlangBinary table1 = (OtpErlangBinary) elements[1];
      final OtpErlangBinary key1 = (OtpErlangBinary) elements[2];
      OtpErlangBinary family1 = null;
      OtpErlangList qualifiers1 = null;
      if(elements.length > 3) {
        family1 = (OtpErlangBinary) elements[3];
      }
      if(elements.length > 4) {
        qualifiers1 = (OtpErlangList) elements[4];
      }
      hbaseClient.delete(TypeUtil.deleteRequest(table1, key1, family1, qualifiers1))
              .addCallback(new GenServerOkCallback(from, mbox))
              .addErrback(new GenServerErrback(from, mbox));
      break;

    default:
      final String message = String.format("Invalid request: \"%s\"", req);
      throw new OtpErlangDecodeException(message);
    }
  }

  private PutRequest parsePut(OtpErlangTuple tuple) {
    final OtpErlangObject[] elements = tuple.elements();
    final OtpErlangBinary table = (OtpErlangBinary) elements[0];
    final OtpErlangBinary key = (OtpErlangBinary) elements[1];
    final OtpErlangBinary family = (OtpErlangBinary) elements[2];
    final OtpErlangList qualifiers = (OtpErlangList) elements[3];
    final OtpErlangList values = (OtpErlangList) elements[4];
    return TypeUtil.putRequest(table, key, family, qualifiers, values);
  }

  private OtpErlangObject handleGetConf(OtpErlangAtom confType) {
    switch(confType.atomValue()) {
      case "flush_interval":
        return TypeUtil.tuple(ATOM_OK, new OtpErlangShort(hbaseClient.getFlushInterval()));
      case "increment_buffer_size":
        return TypeUtil.tuple(ATOM_OK, new OtpErlangInt(hbaseClient.getIncrementBufferSize()));
      default:
        return TypeUtil.tuple(new OtpErlangAtom("error"), new OtpErlangAtom("unknown_conf_type"));
    }
  }

  private OtpErlangObject handleSetConf(OtpErlangObject[] elements) {
    final OtpErlangAtom confType = (OtpErlangAtom) elements[1];
    try {
      switch(confType.atomValue()) {
        case "flush_interval":
          final OtpErlangLong flushInterval = (OtpErlangLong) elements[2];
          final short nextInterval = hbaseClient.setFlushInterval(flushInterval.shortValue());
          return TypeUtil.tuple(ATOM_OK, new OtpErlangShort(nextInterval));
        case "increment_buffer_size":
          final OtpErlangLong bufferSize = (OtpErlangLong) elements[2];
          final int nextBufferSize = hbaseClient.setIncrementBufferSize(bufferSize.intValue());
          return TypeUtil.tuple(ATOM_OK, new OtpErlangInt(nextBufferSize));
        default:
          return TypeUtil.tuple(new OtpErlangAtom("error"), new OtpErlangAtom("unknown_conf_type"));
      }
    } catch (final OtpErlangRangeException e) {
      return TypeUtil.tuple(new OtpErlangAtom("error"), new OtpErlangString(e.getClass().getName()), new OtpErlangString(e.getLocalizedMessage()));
    }
  }

  private void reply(final OtpErlangTuple from, OtpErlangObject reply) {
    OtpErlangPid pid = (OtpErlangPid)from.elementAt(0);
    OtpErlangObject ref = from.elementAt(1);
    final OtpErlangTuple resp = TypeUtil.tuple(ref, reply);
    mbox.send(pid, resp);
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
