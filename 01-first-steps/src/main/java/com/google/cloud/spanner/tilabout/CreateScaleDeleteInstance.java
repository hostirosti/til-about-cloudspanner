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
package com.google.cloud.spanner.tilabout;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.*;

import java.io.IOException;


public class CreateScaleDeleteInstance {
    public static void main(String[] args){
        try {
            GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }

        // Instantiates a client
        SpannerOptions options = SpannerOptions.newBuilder().build();
        Spanner spanner = options.getService();
        String instanceId = "til-about-cloud-spanner";

        try {
            InstanceAdminClient adminClient = spanner.getInstanceAdminClient();

            // Create Instance
            InstanceInfo instanceInfo = InstanceInfo.newBuilder(InstanceId.of(options.getProjectId(), instanceId))
                .setDisplayName("TIL about Cloud Spanner")
                .setInstanceConfigId(InstanceConfigId.of(options.getProjectId(), "regional-europe-west1"))
                .setNodeCount(3)
                .build();
            Operation createOp = adminClient.createInstance(instanceInfo).waitFor();
            if (createOp.isSuccessful()) {
                System.out.println("Instance created successfully :)");
            } else {
                System.err.println("Something went wrong :(");
                return;
            }

            // Scale Instance
            instanceInfo = InstanceInfo.newBuilder(InstanceId.of(options.getProjectId(), instanceId))
                .setNodeCount(5)
                .build();
            Operation scaleOp = adminClient.updateInstance(instanceInfo, InstanceInfo.InstanceField.NODE_COUNT).waitFor();
            if (scaleOp.isSuccessful()) {
                System.out.println("Instance scaled successfully :)");
            } else {
                System.err.println("Something went wrong :(");
                return;
            }

            //Delete Instance
            adminClient.deleteInstance(instanceId);

        } catch (SpannerException e) {
            System.err.println("Something went wrong: " + e.getMessage());
        } finally {
            // Closes the client which will free up the resources used
            spanner.close();
        }
    }
}
