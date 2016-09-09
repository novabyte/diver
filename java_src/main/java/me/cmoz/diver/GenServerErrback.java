package me.cmoz.diver;

import com.ericsson.otp.erlang.*;

class GenServerErrback extends GenServerCallback<Object, Exception> {
  public GenServerErrback(OtpErlangTuple from, OtpMbox mbox) {
    super(from, mbox);
  }

  @Override
  protected OtpErlangObject handle(Exception e) {
    final OtpErlangObject[] body = new OtpErlangObject[]{
            JavaServer.ATOM_ERROR,
            new OtpErlangString(e.getClass().getSimpleName()),
            new OtpErlangString(e.getLocalizedMessage())
    };
    return new OtpErlangTuple(body);
  }
}
