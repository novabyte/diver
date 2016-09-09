package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import com.stumbleupon.async.Callback;
import org.hbase.async.*;

import java.util.ArrayList;

class AsyncScanner implements Callback<Object, ArrayList<ArrayList<KeyValue>>> {
  private static final OtpErlangAtom ROW_ATOM = new OtpErlangAtom("row");
  private static final OtpErlangAtom DONE_ATOM = new OtpErlangAtom("done");

  private final OtpErlangTuple from;
  private final OtpMbox mbox;
  private final OtpErlangRef ref;
  private final Scanner scanner;
  int numRows = Integer.MAX_VALUE;

  public AsyncScanner(OtpErlangTuple from, OtpMbox mbox, OtpErlangRef ref, Scanner scanner, OtpErlangList options)
        throws OtpErlangDecodeException {
    this.from = from;
    this.mbox = mbox;
    this.ref = ref;
    this.scanner = scanner;

    // prevent returning partial row by default
    scanner.setMaxNumKeyValues(-1);

    for (final OtpErlangObject option : options) {
      final OtpErlangTuple tuple = (OtpErlangTuple) option;
      final OtpErlangObject[] tupleElements = tuple.elements();
      final String optionName = ((OtpErlangAtom) tupleElements[0]).atomValue();
      final OtpErlangObject optionValue = tupleElements[1];

      switch(optionName) {
      case "num_rows":
        numRows = (int)((OtpErlangLong) optionValue).longValue();
        scanner.setMaxNumRows(numRows);
        break;

      // TODO: setFamilies
      case "family":
        scanner.setFamily(((OtpErlangBinary) optionValue).binaryValue());
        break;
      // TODO: setFilter
      case "key_regexp":
        scanner.setKeyRegexp(new String(((OtpErlangBinary) optionValue).binaryValue()));
        break;
      // TODO: setKeyRegexp(regesp, charset)
      case "max_num_bytes":
        scanner.setMaxTimestamp(((OtpErlangLong) optionValue).longValue());
        break;
      case "max_num_keyvalues":
        scanner.setMaxNumKeyValues((int)((OtpErlangLong) optionValue).longValue());
        break;
      case "max_num_rows":
        scanner.setMaxNumRows((int)((OtpErlangLong) optionValue).longValue());
        break;
      case "max_timestamp":
        scanner.setMaxTimestamp(((OtpErlangLong) optionValue).longValue());
        break;
      case "max_versions":
        scanner.setMaxVersions((int)((OtpErlangLong) optionValue).longValue());
        break;
      case "qualifier":
        scanner.setQualifier(((OtpErlangBinary) optionValue).binaryValue());
        break;
      // TODO: setQualifiers
      case "server_block_cache":
        scanner.setServerBlockCache(((OtpErlangLong) optionValue).longValue() != 0);
        break;
      case "start_key":
        scanner.setStartKey(((OtpErlangBinary) optionValue).binaryValue());
        break;
      case "stop_key":
        scanner.setStopKey(((OtpErlangBinary) optionValue).binaryValue());
        break;
      case "time_range":
        final OtpErlangObject[] timeRangeElems = ((OtpErlangTuple)optionValue).elements();
        scanner.setTimeRange(
          ((OtpErlangLong) timeRangeElems[0]).longValue(),
          ((OtpErlangLong) timeRangeElems[1]).longValue());
        break;
      default:
        final String message = String.format("Invalid scan option: \"%s\"", tuple);
        throw new OtpErlangDecodeException(message);
      }
    }
  }

  public void start() {
    scanner.nextRows()
      .addCallback(this)
      .addErrback(new ScannerErrback(from, mbox, ref));
  }

  @Override
  public Object call(ArrayList<ArrayList<KeyValue>> rows) throws Exception {
    if (rows == null) {
      sendDone();
      return null;
    }

    for(final ArrayList<KeyValue> row : rows) {
      sendRow(row);

      numRows -= 1;
      if(numRows == 0) {
        sendDone();
        return null;
      }
    }

    scanner.nextRows()
      .addCallback(this)
      .addErrback(new ScannerErrback(from, mbox, ref));
    return null;
  }

  public void sendDone() {
    final OtpErlangObject[] body = new OtpErlangObject[] {
        ref,
        DONE_ATOM,
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(body));
  }

  public void sendRow(final ArrayList<KeyValue> data) throws Exception {
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
        ref,
        ROW_ATOM,
        new OtpErlangList(items)
    };

    mbox.send((OtpErlangPid) from.elementAt(0), new OtpErlangTuple(body));
  }
}
