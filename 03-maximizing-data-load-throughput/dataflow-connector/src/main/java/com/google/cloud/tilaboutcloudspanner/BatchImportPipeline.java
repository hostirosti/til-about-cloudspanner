/*
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.tilaboutcloudspanner;

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.codec.binary.Hex;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 *
 * <p>To run this pipeline example using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<project>
 *   --stagingLocation=gs://<project>/df-staging
 *   --output=gs://<project>/df-output
 *   --runner=DataflowPipelineRunner
 */
@SuppressWarnings("serial")
public class BatchImportPipeline implements Serializable {

    public static void main(String[] args) {
        PipelineOptionsFactory.register(CustomPipelineOptions.class);
        CustomPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomPipelineOptions.class);
        Pipeline p = Pipeline.create(pipelineOptions);

        // Read lines from input files
        PCollection<String> lines =
            p.apply("Read CSV files", TextIO.read()
                .from(pipelineOptions.getBucketDataPath()));

        PCollection<Mutation> mutations = lines.apply("Parse CSV to Spanner Mutations", ParDo.of(new DoFn<String, Mutation>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
                String line = c.element();
                if (line == null || line.contains("OrderID")) {
                    return;
                }
                String[] elems = line.split(",");
                if (elems.length != 4) {
                    throw new IllegalStateException("Not enough columns in csv line to create OrderLineItem");
                }

                c.output(Mutation.newInsertOrUpdateBuilder("OrderLineItem")
                    .set("OrderID").to(getBase64EncodedUUID())
                    .set("ProductID").to(getBase64EncodedUUID())
                    .set("Quantity").to(Long.parseLong(elems[2]))
                    .set("Discount").to(Long.parseLong(elems[3]))
                    .build());
            }
        }));

        mutations.apply("Write to Cloud Spanner",
            SpannerIO.write()
                .withInstanceId("til-about-cloudspanner")
                .withDatabaseId("dataflow-connector")
                .withProjectId("cloud-spanner-til")
                .withBatchSizeBytes(200000)
        );

        p.run();
    }

    public static String getBase64EncodedUUID() {
        UUID uuid = UUID.randomUUID();
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return new String(Hex.encodeHex(bb.array()));
    }
}