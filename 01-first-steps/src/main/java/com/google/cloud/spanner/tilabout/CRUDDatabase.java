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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class CRUDDatabase {
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
        String databaseId = "helloworld";

        try {

            // Create database with schema
            System.out.printf("Creating database and schema... ");
            DatabaseAdminClient adminClient = spanner.getDatabaseAdminClient();
            Operation createDBOp = adminClient.createDatabase(instanceId,
                databaseId,
                Arrays.asList(
                    "CREATE TABLE helloworld (\n" +
                        "    hello       STRING(32) NOT NULL,\n" +
                        "    world       STRING(32) NOT NULL,\n" +
                        "    lang        STRING(3)  NOT NULL,\n" +
                        ") PRIMARY KEY (hello, world)")
            ).waitFor();

            if (createDBOp.isSuccessful()) {
                System.out.println("Database created successfully :)");
            } else {
                System.err.println("Something went wrong :(");
                return;
            }

            // Create DB client for CRUD operations
            System.out.printf("Loading data... ");
            DatabaseClient dbClient = spanner.getDatabaseClient(DatabaseId.of(
                options.getProjectId(), instanceId, databaseId));

            // Load data into database table
            List<Mutation> mutations = new ArrayList<>();
            mutations.add(Mutation.newInsertBuilder("helloworld")
                .set("hello").to("Hallo")
                .set("world").to("Welt")
                .set("lang").to("DE")
                .build());
            mutations.add(Mutation.newInsertBuilder("helloworld")
                .set("hello").to("Hola")
                .set("world").to("Mundo")
                .set("lang").to("ES")
                .build());
            mutations.add(Mutation.newInsertBuilder("helloworld")
                .set("hello").to("Hello")
                .set("world").to("World")
                .set("lang").to("EN")
                .build());
            dbClient.write(mutations);
            System.out.println(" data load finished");

            // Read data from database
            ReadOnlyTransaction txn = dbClient.singleUseReadOnlyTransaction();
            System.out.printf("Read row from table 'helloworld': ");
            ResultSet resultSet = txn.executeQuery(
                Statement.newBuilder(
                    "SELECT * FROM helloworld WHERE hello=@hello"
                )
                    .bind("hello").to("Hallo")
                    .build());

            while (resultSet.next()) {
                System.out.printf(
                    "%s %s %s\n",
                    resultSet.getString(0),
                    resultSet.getString(1),
                    resultSet.getString("lang"));
            }

            // Update table row(s) in database
            System.out.println("Updating row in database table... ");
            dbClient.readWriteTransaction().run(
                // enables rerun of transaction in case of failure - managed in client lib
                new TransactionRunner.TransactionCallable<Void>() {
                    @Nullable
                    @Override
                    public Void run(TransactionContext txn) throws Exception {
                        ResultSet resultSet = txn.executeQuery(
                            Statement.newBuilder("SELECT hello, world FROM helloworld WHERE hello=@hello AND world=@world")
                                .bind("hello").to("Hallo")
                                .bind("world").to("Welt")
                                .build()
                        );
                        List<Mutation> mutations = new ArrayList<>();
                        while (resultSet.next()) {
                            mutations.add(
                                Mutation.newUpdateBuilder("helloworld")
                                    .set("hello")
                                    .to(resultSet.getString("hello"))
                                    .set("world")
                                    .to(resultSet.getString("world"))
                                    .set("lang")
                                    .to("DEU")
                                    .build()
                            );
                        }
                        txn.buffer(mutations);
                        return null;
                    }
                }
            );

            // Read updated data from database
            txn = dbClient.singleUseReadOnlyTransaction();
            System.out.printf("Read updated row from table 'helloworld': ");
            resultSet = txn.executeQuery(
                Statement.newBuilder(
                    "SELECT * FROM helloworld WHERE hello=@hello"
                )
                    .bind("hello").to("Hallo")
                    .build());

            while (resultSet.next()) {
                System.out.printf(
                    "%s %s %s\n",
                    resultSet.getString(0),
                    resultSet.getString(1),
                    resultSet.getString("lang"));
            }

            // Delete database
            System.out.println("Deleting database...");
            adminClient.dropDatabase(instanceId,databaseId);

        } catch (SpannerException e) {
            System.err.println("Something went wrong: " + e.getMessage());
        } finally {
            // Closes the client which will free up the resources used
            spanner.close();
        }
    }
}
