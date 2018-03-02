/*
Copyright 2018 Google Inc. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/gocarina/gocsv"
	"github.com/namsral/flag"
)

const (
	serviceAccountJSONFile = "service-account.json"
	gcsScope               = storage.ScopeReadWrite
)

var (
	version string // set by linker -X

	project  = flag.String("project", "", "Your cloud project ID.")
	instance = flag.String("instance", "", "The Cloud Spanner Instance within your project.")
	dbName   = flag.String("database", "", "The database name in your Cloud Spanner Instance.")

	bucketName  = flag.String("bucket", "", "The name of the bucket within your project.")
	folderPath  = flag.String("folderPath", "", "Bucket Folder path for data to store or load.")
	scaleInit   = flag.Int("scaleInit", 1, "for scaleout, start with n-th file")
	scaleOffset = flag.Int("scaleOffset", 1, "for scaleout, process every n-th file")

	numAccounts    = flag.Int("accounts", 10000, "Number of accounts to generate / load.")
	numOrders      = flag.Int("orders", 100000, "Number of orders to generate / load.")
	numProducts    = flag.Int("products", 100, "Number of products to generate / load.")
	numWarehouses  = flag.Int("warehouses", 10, "Number of warehouses to generate / load.")
	infoDataSize   = flag.Int("infoDataSize", 1000, "Bytes of random data to add per product")
	schemaFile     = flag.String("schema", "schema.sql", "Path to schema file for 'create' command")
	parallelTxn    = flag.Int("parallelTxn", 10, "Max number of parallel to Cloud Spanner transactions.")
	timeout        = flag.Int("timeout", 5, "Timeout value (in seconds) for inserts.")
	recordsPerFile = 100000

	debugLog debugging
)

func main() {

	// Initialize rand
	rand.Seed(time.Now().UnixNano())

	if len(os.Args) > 1 && os.Args[1] == "version" {
		log.Printf("version:    %v", version)
		os.Exit(0)
	}

	// print debug log always
	debugLog = debugging(true)

	validCommands := []string{"create", "generate", "load", "reset"}
	flag.Parse()

	log.Printf("Running version %v", version)

	switch flag.Arg(0) {
	case "create":
		cssvc, err := NewCloudSpannerService(serviceAccountJSONFile, *project, *instance, *dbName)
		if err != nil {
			log.Fatalf("Couldn't connect to Google Cloud Spanner: %v", err)
		}
		defer cssvc.cleanup()

		s, err := ioutil.ReadFile(*schemaFile)
		if err != nil {
			log.Fatal(err)
		}

		err = cssvc.createDB(*dbName, string(s))
		if err != nil {
			log.Fatal(err)
		}
		break

	case "generate":
		gcs, err := NewGCSService(serviceAccountJSONFile, gcsScope)
		if err != nil {
			log.Fatalf("Couldn't connect to Google Cloud Storage: %v", err)
		}
		wg := &sync.WaitGroup{}
		generateAccounts(gcs, wg)
		generateWarehouses(gcs, wg)
		generateProducts(gcs, wg)
		generateOrders(gcs, wg)
		debugLog.Println("Waiting for write to GCS to finish....")
		wg.Wait()
		break

	case "load":
		if *project == "" {
			log.Fatalf("project argument is required. See --help.")
		}
		if *instance == "" {
			log.Fatalf("instance argument is required. See --help.")
		}
		if *dbName == "" {
			log.Fatalf("dbName argument is required. See --help.")
		}

		if *bucketName == "" {
			log.Fatalf("bucket argument is required. See --help.")
		}

		if *folderPath == "" {
			log.Fatalf("folderPath argument is required. See --help.")
		}

		gcs, err := NewGCSService(serviceAccountJSONFile, gcsScope)
		if err != nil {
			log.Fatalf("Couldn't connect to Google Cloud Storage: %v", err)
		}
		defer gcs.Close()

		debugLog.Printf("Reading files from bucket %v:\n", *bucketName+"/"+*folderPath)
		inputFiles, err := gcs.list(*bucketName, *folderPath)
		if err != nil {
			log.Fatalf("Unable to get list of files for bucket: %v/%v: %v", *bucketName, *folderPath, err)
		}

		cssvc, err := NewCloudSpannerService(serviceAccountJSONFile, *project, *instance, *dbName)
		if err != nil {
			log.Fatalf("Couldn't connect to Google Cloud Spanner: %v", err)
		}
		defer cssvc.cleanup()

		err = persist(gcs, cssvc, inputFiles)
		if err != nil {
			log.Fatalf("Couldn't persist objects in Google Cloud Spanner: %v", err)
		}
		break

	case "reset":
		cssvc, err := NewCloudSpannerService(serviceAccountJSONFile, *project, *instance, *dbName)
		if err != nil {
			log.Fatalf("Couldn't connect to Google Cloud Spanner: %v", err)
		}

		for _, t := range tables {
			debugLog.Printf("Deleting all rows in %v table...", t)
			err = cssvc.clearTable(t)
			if err != nil {
				log.Fatalf("Couldn't delete rows from %v table: %v", t, err)
			}
		}
		break

	default:
		log.Fatalf("'%v' is not a valid command! Supported cmds are: %v", flag.Arg(0), validCommands)
	}
}

func persist(gcs *GCSclient, cssvc *CloudSpannerService, files []string) error {
	// decouple file loading from storage and persisting to Cloud Spanner
	// with a channel (for scalability), buffer size 100k Mutations
	// This needs to be increased if runnning against more than 5 nodes instance
	ch := make(chan *spanner.Mutation, 100000)
	ijr := &insertJobsRunner{
		svc:         cssvc,
		timeout:     *timeout,
		parallelism: *parallelTxn,
	}
	ijr.run(ch)

	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		debugLog.Println("Cleaning up resources before exiting!")
		ijr.stop()
		ijr.workersWG.Wait()
		gcs.Close()
		cssvc.cleanup()
		os.Exit(1)
	}()

	err := persistObjects(gcs, "accounts-", "Account", ch, files)
	if err != nil {
		return err
	}

	err = persistObjects(gcs, "warehouses-", "Warehouse", ch, files)
	if err != nil {
		return err
	}

	err = persistObjects(gcs, "products-", "Product", ch, files)
	if err != nil {
		return err
	}

	// wait so that interleaved rows can be persisted without errors
	ijr.jobsWG.Wait()

	err = persistObjects(gcs, "productinfos-", "ProductInfo", ch, files)
	if err != nil {
		return err
	}

	err = persistObjects(gcs, "productprices-", "ProductPrice", ch, files)
	if err != nil {
		return err
	}

	err = persistObjects(gcs, "productstocks-", "ProductStock", ch, files)
	if err != nil {
		return err
	}

	err = persistObjects(gcs, "orders-", "Order", ch, files)
	if err != nil {
		return err
	}

	// wait so that interleaved rows can be persisted without errors
	ijr.jobsWG.Wait()

	err = persistObjects(gcs, "orderlineitems-", "OrderLineItem", ch, files)
	if err != nil {
		return err
	}

	err = persistObjects(gcs, "payments-", "Payment", ch, files)
	if err != nil {
		return err
	}

	close(ch)
	<-ijr.done()
	return nil
}

func persistObjects(gcs *GCSclient, filePrefix, table string, ch chan<- *spanner.Mutation, files []string) error {
	// persist objects read from CSV files
	for i, f := range filterStrings(files,
		func(s string) bool { return strings.Contains(s, filePrefix) }) {

		process := false
		if (i+*scaleInit)%*scaleOffset == 0 {
			process = true
		}
		var c []byte
		var err error
		if !(process || table == "Account" || table == "Warehouse" || table == "Product") {
			continue
		}

		debugLog.Printf("Reading %vs from file %v", table, f)
		c, err = gcs.read(*bucketName, f)
		if err != nil {
			return err
		}

		switch table {
		case "Account":
			err = gocsv.UnmarshalBytesToCallback(c, func(a *dbAccount) error {
				if process {
					return appendMutation(ch, table, a)
				}
				return nil
			})
			break

		case "Order":
			err = gocsv.UnmarshalBytesToCallback(c, func(o *dbOrder) error {
				return appendMutation(ch, table, o)
			})
			break

		case "OrderLineItem":
			err = gocsv.UnmarshalBytesToCallback(c, func(ol *dbOrderLineItem) error {
				return appendMutation(ch, table, ol)
			})
			break

		case "Payment":
			err = gocsv.UnmarshalBytesToCallback(c, func(p *dbPayment) error {
				return appendMutation(ch, table, p)
			})
			break

		case "Product":
			err = gocsv.UnmarshalBytesToCallback(c, func(p *dbProduct) error {
				if process {
					return appendMutation(ch, table, p)
				}
				return nil
			})
			break

		case "ProductPrice":
			err = gocsv.UnmarshalBytesToCallback(c, func(pp *dbProductPrice) error {
				return appendMutation(ch, table, pp)
			})
			break

		case "ProductStock":
			err = gocsv.UnmarshalBytesToCallback(c, func(ps *dbProductStock) error {
				return appendMutation(ch, table, ps)
			})
			break

		case "ProductInfo":
			err = gocsv.UnmarshalBytesToCallback(c, func(pi *dbProductInfo) error {
				return appendMutation(ch, table, pi)
			})
			break

		case "Warehouse":
			err = gocsv.UnmarshalBytesToCallback(c, func(w *dbWarehouse) error {
				if process {
					return appendMutation(ch, table, w)
				}
				return nil
			})
			break

		default:
			log.Fatalf("Table type '%v' not supported in persistObjects!", table)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
