package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.stumbleupon.async.Callback;
import lombok.RequiredArgsConstructor;
import org.hbase.async.KeyValue;

import java.util.ArrayList;

@RequiredArgsConstructor
class GenServerGetCallback implements Callback<Object, ArrayList<KeyValue>> {

  private static final OtpErlangAtom OK_ATOM = new OtpErlangAtom("ok");

  private final OtpErlangTuple from;

  private final OtpMbox mbox;

  @Override
  public Object call(final ArrayList<KeyValue> data) throws Exception {
    final OtpErlangObject[] items = new OtpErlangObject[data.size()];
    int i = 0;
    for (final KeyValue keyValue : data) {
      final OtpErlangObject[] erldata = new OtpErlangObject[] {
          new OtpErlangBinary(keyValue.key()),
          new OtpErlangBinary(keyValue.family()),
          new OtpErlangBinary(keyValue.qualifier()),
          new OtpErlangBinary(keyValue.value()),
          new OtpErlangLong(keyValue.timestamp())
      };
      items[i] = new OtpErlangTuple(erldata);
      i++;
    }

    final OtpErlangObject[] body = new OtpErlangObject[] {
        OK_ATOM,
        new OtpErlangList(items)
    };

    final OtpErlangObject[] resp = new OtpErlangObject[] {
        from.elementAt(1),  // Ref
        new OtpErlangTuple(body)
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(resp));
    return null;
  }

}
