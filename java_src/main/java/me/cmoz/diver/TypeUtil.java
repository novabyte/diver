package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import org.hbase.async.ClientStats;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.PutRequest;

import java.util.HashMap;
import java.util.Map;

class TypeUtil {

  static OtpErlangTuple clientStats(final ClientStats stats) {
    final Map<String, Object> map = new HashMap<>();
    map.put("atomic_increments", stats.atomicIncrements());
    map.put("connections_created", stats.connectionsCreated());
    map.put("contended_meta_lookups", stats.contendedMetaLookups());
    map.put("deletes", stats.deletes());
    map.put("flushes", stats.flushes());
    map.put("gets", stats.gets());
    map.put("no_such_region_exceptions", stats.noSuchRegionExceptions());
    map.put("num_batched_rpc_sent", stats.numBatchedRpcSent());
    map.put("num_rpc_delayed_due_to_nsre", stats.numRpcDelayedDueToNSRE());
    map.put("puts", stats.puts());
    map.put("root_lookups", stats.rootLookups());
    map.put("row_locks", stats.rowLocks());
    map.put("scanners_opened", stats.scannersOpened());
    map.put("scans", stats.scans());
    map.put("uncontended_meta_lookups", stats.uncontendedMetaLookups());
    return tuple(new OtpErlangAtom("ok"), proplist(map));
  }

  static OtpErlangList proplist(final Map<String, Object> map) {
    final OtpErlangObject[] elements = new OtpErlangObject[map.keySet().size()];
    int i = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      elements[i] = tuple(new OtpErlangAtom(entry.getKey()), objectToType(entry.getValue()));
      i++;
    }
    return new OtpErlangList(elements);
  }

  static OtpErlangTuple tuple(final OtpErlangObject... objects) {
    return new OtpErlangTuple(objects);
  }

  static OtpErlangObject objectToType(final Object object) {
    if (object instanceof Long) {
      return new OtpErlangLong((Long) object);
    } else {
      throw new IllegalArgumentException("Unrecognised object type.");
    }
  }

  static PutRequest putRequest(
      final OtpErlangBinary table,
      final OtpErlangBinary key,
      final OtpErlangBinary family,
      final OtpErlangList qualifiers,
      final OtpErlangList values) {
    final byte[][] byteQualifiers = new byte[qualifiers.arity()][];
    int i = 0;
    for (final OtpErlangObject qualifier : qualifiers) {
      byteQualifiers[i] = ((OtpErlangBinary) qualifier).binaryValue();
      i++;
    }
    final byte[][] byteValues = new byte[values.arity()][];
    i = 0;
    for (final OtpErlangObject value : values) {
      byteValues[i] = ((OtpErlangBinary) value).binaryValue();
      i++;
    }
    return new PutRequest(table.binaryValue(), key.binaryValue(), family.binaryValue(), byteQualifiers, byteValues);
  }

  static DeleteRequest deleteRequest(
      final OtpErlangBinary table,
      final OtpErlangBinary key,
      final OtpErlangBinary family,
      final OtpErlangList qualifiers) {
    final byte[][] byteQualifiers = new byte[qualifiers.arity()][];
    int i = 0;
    for (final OtpErlangObject qualifier : qualifiers) {
      byteQualifiers[i] = ((OtpErlangBinary) qualifier).binaryValue();
      i++;
    }
    return new DeleteRequest(table.binaryValue(), key.binaryValue(), family.binaryValue(), byteQualifiers);
  }

  static GetRequest getRequest(
      final OtpErlangBinary table,
      final OtpErlangBinary key,
      final OtpErlangBinary family,
      final OtpErlangBinary qualifier) {
    if(qualifier == null) {
      return new GetRequest(table.binaryValue(), key.binaryValue(), family.binaryValue());
    } else {
      return new GetRequest(table.binaryValue(), key.binaryValue(), family.binaryValue(), qualifier.binaryValue());
    }
  }
}
