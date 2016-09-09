package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import org.hbase.async.KeyValue;
import java.util.ArrayList;

class GenServerGetCallback extends GenServerCallback<Object,ArrayList<KeyValue>> {
  public GenServerGetCallback(OtpErlangTuple from, OtpMbox mbox) {
    super(from, mbox);
  }

  @Override
  protected OtpErlangObject handle(ArrayList<KeyValue> data) {
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
            JavaServer.ATOM_OK,
            new OtpErlangList(items)
    };
    return new OtpErlangTuple(body);
  }

}
