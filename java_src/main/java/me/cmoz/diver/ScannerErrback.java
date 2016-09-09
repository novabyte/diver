package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.stumbleupon.async.Callback;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
class ScannerErrback implements Callback<Object, Exception> {

  private static final OtpErlangAtom ERROR_ATOM = new OtpErlangAtom("error");

  private final OtpErlangTuple from;

  private final OtpErlangRef ref;

  private final OtpMbox mbox;

  @Override
  public Object call(final Exception e) throws Exception {
    final OtpErlangObject[] body = new OtpErlangObject[] {
        ref,
        ERROR_ATOM,
        new OtpErlangString(e.getClass().getSimpleName()),
        new OtpErlangString(e.getLocalizedMessage())
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(body));
    return null;
  }

}
