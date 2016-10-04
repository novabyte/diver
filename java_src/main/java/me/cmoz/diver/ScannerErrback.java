package me.cmoz.diver;

import com.ericsson.otp.erlang.*;

class ScannerErrback extends GenServerCallback<Object,Exception> {
  private final OtpErlangRef ref;

  public ScannerErrback(OtpErlangTuple from, OtpMbox mbox, OtpErlangRef ref) {
    super(from, mbox);
    this.ref = ref;
  }

  @Override
  protected OtpErlangObject handle(Exception e) {
    final OtpErlangObject[] body = new OtpErlangObject[] {
            ref,
            JavaServer.ATOM_ERROR,
            new OtpErlangString(e.getClass().getSimpleName()),
            new OtpErlangString(e.getLocalizedMessage())
    };
    return new OtpErlangTuple(body);
  }

}
