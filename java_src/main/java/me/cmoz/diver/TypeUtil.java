package me.cmoz.diver;

import com.ericsson.otp.erlang.*;
import org.hbase.async.ClientStats;

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
    return new OtpErlangTuple(new OtpErlangObject[]{
        new OtpErlangAtom("client_stats"),
        proplist(map)
    });
  }

  static OtpErlangList proplist(final Map<String, Object> map) {
    final OtpErlangObject[] elements = new OtpErlangObject[map.keySet().size()];
    int i = 0;
    for (final Map.Entry<String, Object> entry : map.entrySet()) {
      elements[i] = new OtpErlangTuple(new OtpErlangObject[] {
          new OtpErlangAtom(entry.getKey()), objectToType(entry.getValue())
      });
      i++;
    }
    return new OtpErlangList(elements);
  }

  static OtpErlangObject objectToType(final Object object) {
    if (object instanceof Long) {
      return new OtpErlangLong((Long) object);
    } else {
      throw new IllegalArgumentException("Unrecognised object type.");
    }
  }

}
