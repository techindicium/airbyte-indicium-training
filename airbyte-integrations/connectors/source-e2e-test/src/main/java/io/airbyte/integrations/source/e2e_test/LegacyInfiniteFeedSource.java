/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.e2e_test;

import static java.lang.Thread.sleep;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.BaseConnector;
import io.airbyte.integrations.base.Source;
import io.airbyte.protocol.models.AirbyteCatalog;
import io.airbyte.protocol.models.AirbyteConnectionStatus;
import io.airbyte.protocol.models.AirbyteConnectionStatus.Status;
import io.airbyte.protocol.models.AirbyteMessage;
import io.airbyte.protocol.models.AirbyteMessage.Type;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteCatalog;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LegacyInfiniteFeedSource extends BaseConnector implements Source {

  private static final Logger LOGGER = LoggerFactory.getLogger(LegacyInfiniteFeedSource.class);

  @Override
  public AirbyteConnectionStatus check(final JsonNode config) {
    return new AirbyteConnectionStatus().withStatus(Status.SUCCEEDED);
  }

  @Override
  public AirbyteCatalog discover(final JsonNode config) {
    return Jsons.clone(LegacyConstants.DEFAULT_CATALOG);
  }

  @Override
  public AutoCloseableIterator<AirbyteMessage> read(final JsonNode config, final ConfiguredAirbyteCatalog catalog, final JsonNode state) {
    final LongPredicate anotherRecordPredicate = config.has("max_records")
        ? recordNumber -> recordNumber < config.get("max_records").asLong()
        : recordNumber -> true;

    final Optional<Long> sleepTime = Optional.ofNullable(config.get("message_interval")).map(JsonNode::asLong);

    final AtomicLong i = new AtomicLong();

    return AutoCloseableIterators.fromIterator(new AbstractIterator<>() {

      @Override
      protected AirbyteMessage computeNext() {
        if (!anotherRecordPredicate.test(i.get())) {
          return endOfData();
        }

        if (sleepTime.isPresent() && i.get() != 0) {
          try {
            LOGGER.info("sleeping for {} ms", sleepTime.get());
            sleep(sleepTime.get());
          } catch (final InterruptedException e) {
            throw new RuntimeException(e);
          }
        }

        i.incrementAndGet();
        LOGGER.info("source emitting record {}:", i.get());
        return new AirbyteMessage()
            .withType(Type.RECORD)
            .withRecord(new AirbyteRecordMessage()
                .withStream("data")
                .withEmittedAt(Instant.now().toEpochMilli())
                .withData(Jsons.jsonNode(ImmutableMap.of("column1", i))));
      }

    });
  }

}
