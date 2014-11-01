package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.inject.name.Named;
import com.google.inject.Inject;
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
    case "pid":
      final OtpErlangPid caller = (OtpErlangPid) elements[1];
      final OtpErlangObject[] payload = new OtpErlangObject[] {
        opType,
        mbox.self()
      };
      mbox.send(caller, new OtpErlangTuple(payload));
      break;
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
    case "stats":
      reply(from, TypeUtil.clientStats(hbaseClient.stats()));
      break;
    default:
      final String message = String.format("Invalid request: \"%s\"", req);
      throw new OtpErlangDecodeException(message);
    }
  }

  private void reply(final OtpErlangTuple from, OtpErlangObject reply) {
    final OtpErlangTuple resp = new OtpErlangTuple(new OtpErlangObject[]{
        from.elementAt(1),
        reply
    });
    mbox.send((OtpErlangPid) from.elementAt(0), resp);
  }

  @Override
  protected void run() throws Exception {
    while (isRunning()) {
      final OtpErlangObject msg = mbox.receive();
      try {
        handle((OtpErlangTuple) msg);
      } catch (final OtpErlangDecodeException | ClassCastException e) {
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
