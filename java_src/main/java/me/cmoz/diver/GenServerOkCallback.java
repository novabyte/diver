package me.cmoz.diver;

import com.ericsson.otp.erlang.*;

class GenServerOkCallback extends GenServerCallback<Object,Object> {
  public GenServerOkCallback(OtpErlangTuple from, OtpMbox mbox) {
    super(from, mbox);
  }

  @Override
  protected OtpErlangObject handle(Object o) {
    return JavaServer.ATOM_OK;
  }
}