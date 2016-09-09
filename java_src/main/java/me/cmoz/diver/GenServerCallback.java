package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.stumbleupon.async.Callback;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class GenServerCallback<R, T> implements Callback<R, T> {
  private final OtpErlangTuple from;
  private final OtpMbox mbox;

  @Override
  public R call(final T data) throws Exception {
    final OtpErlangPid pid = (OtpErlangPid) from.elementAt(0);
    final OtpErlangRef ref = (OtpErlangRef) from.elementAt(1);

    mbox.send(pid, TypeUtil.tuple(ref, handle(data)));
    return null;
  }

  protected abstract OtpErlangObject handle(T t);
}