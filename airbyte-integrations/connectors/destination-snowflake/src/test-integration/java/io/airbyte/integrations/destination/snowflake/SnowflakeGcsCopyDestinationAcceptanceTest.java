/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import io.airbyte.commons.io.IOs;
import io.airbyte.commons.json.Jsons;
import java.nio.file.Path;

public class SnowflakeGcsCopyDestinationAcceptanceTest extends SnowflakeInsertDestinationAcceptanceTest {

  @Override
  public JsonNode getStaticConfig() {
    final JsonNode copyConfig = Jsons.deserialize(IOs.readFile(Path.of("secrets/copy_gcs_config.json")));
    Preconditions.checkArgument(SnowflakeDestinationResolver.isGcsCopy(copyConfig));
    Preconditions.checkArgument(!SnowflakeDestinationResolver.isS3Copy(copyConfig));
    return copyConfig;
  }

}
