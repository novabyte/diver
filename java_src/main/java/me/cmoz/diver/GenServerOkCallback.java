package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.stumbleupon.async.Callback;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class GenServerOkCallback implements Callback<Object, Object> {

  private static final OtpErlangAtom OK_ATOM = new OtpErlangAtom("ok");

  private final OtpErlangTuple from;

  private final OtpMbox mbox;

  @Override
  public Object call(final Object object) throws Exception {
    final OtpErlangObject[] resp = new OtpErlangObject[] {
        from.elementAt(1),  // Ref
        new OtpErlangTuple(OK_ATOM)
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(resp));
    return null;
  }

}
