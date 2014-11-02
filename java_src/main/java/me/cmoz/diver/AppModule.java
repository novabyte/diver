package me.cmoz.diver;

import com.ericsson.otp.erlang.OtpNode;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.util.Properties;
import org.hbase.async.HBaseClient;

@Slf4j
@RequiredArgsConstructor
class AppModule extends AbstractModule {

  /** Configuration properties for the application. */
  private final Properties properties;

  @Override
  protected void configure() {
    Names.bindProperties(binder(), properties);
  }

  @Provides
  @Singleton
  private HBaseClient providesHBaseClient(
      @Named("zk.quorum_spec") final String quorumSpec,
      @Named("zk.base_path")   final String basePath) {
    return new HBaseClient(quorumSpec, basePath);
  }

  @Provides
  @Singleton
  private OtpNode providesOtpNode(
      @Named("erlang.self")   final String self,
      @Named("erlang.cookie") final String cookie) {
    try {
      return new OtpNode(self, cookie);
    } catch (final IOException e) {
      log.error("Could not start OTP node.", e);
      log.error("'epmd' must be running, try 'epmd -daemon'.");
      System.exit(-1);
      return null;
    }
  }

}
