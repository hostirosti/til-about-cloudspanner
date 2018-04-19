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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/gocarina/gocsv"
	"github.com/google/uuid"
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

	numAccounts   = flag.Int("accounts", 10000, "Number of accounts to generate / load.")
	numOrders     = flag.Int("orders", 100000, "Number of orders to generate / load.")
	numProducts   = flag.Int("products", 100, "Number of products to generate / load.")
	numWarehouses = flag.Int("warehouses", 10, "Number of warehouses to generate / load.")
	infoDataSize  = flag.Int("infoDataSize", 1000, "Bytes of random data to add per product")
	schemaFile    = flag.String("schema", "schema.sql", "Path to schema file for 'create' command")
	batchSize     = flag.Int("batchSize", 1, "Max number of mutations to send in one Cloud Spanner request.")
	parallelTxn   = flag.Int("parallelTxn", 10, "Max number of parallel to Cloud Spanner transactions.")
	timeout       = flag.Int("timeout", 5, "Timeout value (in seconds) for inserts.")

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

		debugLog.Printf("Reading files from bucket %v ...\n", *bucketName+"/"+*folderPath)
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
	// with channels (for scalability), buffer size 20k Mutations per channel
	// This needs to be increased if runnning against more than 5 nodes instance
	chs := []chan map[string]*spanner.Mutation{}
	for i := 0; i < 4; i++ {
		chs = append(chs, make(chan map[string]*spanner.Mutation, 20000))
	}

	ijr := &insertJobsRunner{
		svc:          cssvc,
		timeout:      *timeout,
		parallelism:  *parallelTxn,
		maxBatchSize: *batchSize,
	}
	ijr.run(chs)

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

	readersCh := make(chan *processJob, 10)
	processorsCh := make(chan *processJob, 10)

	for i := 0; i < 10; i++ {
		go readWorker(i, gcs, readersCh, processorsCh)
		go processWorker(i, processorsCh, chs)
	}

	lookup := map[string]map[int64]*uuid.UUID{
		"accounts":   make(map[int64]*uuid.UUID),
		"products":   make(map[int64]*uuid.UUID),
		"warehouses": make(map[int64]*uuid.UUID),
	}

	wgs := make(map[string]*sync.WaitGroup)
	for _, g := range []string{"Account", "Warehouse", "Product", "Order"} {
		wgs[g] = &sync.WaitGroup{}
		err := persistObjects(wgs, g, readersCh, files, lookup)
		if err != nil {
			return err
		}
	}
	for _, wg := range wgs {
		wg.Wait()
	}

	for _, ch := range chs {
		close(ch)
	}
	ijr.jobsWG.Wait()
	return nil
}

func persistObjects(wgs map[string]*sync.WaitGroup, group string, readersCh chan<- *processJob, files []string, lookup map[string]map[int64]*uuid.UUID) error {
	switch group {
	case "Account":
		// persist Accounts
		for _, f := range filterStrings(files,
			func(s string) bool { return strings.Contains(s, "accounts-") }) {
			wgs[group].Add(1)
			readersCh <- &processJob{
				files: map[string][]byte{
					f: nil,
				},
				mapping: map[string]string{
					"accounts": f,
				},
				lookup: lookup,
				processFunction: func(j *processJob, chs []chan map[string]*spanner.Mutation) error {
					gocsv.UnmarshalBytesToCallback(j.files[j.mapping["accounts"]], func(a *dbAccount) error {
						id := uuid.New()
						a.AccountID = string(encodeHexUUID(&id))
						j.lookup["accounts"][a.OldID] = &id
						return appendMutation(chs[0], "Account", a.AccountID, a)
					})
					j.wgs["Account"].Done()
					return nil
				},
				wgs: wgs,
			}
		}
		break
	case "Order":
		// persist Orders and OrderLineItems + Payments in groups together
		ofs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "orders-") })
		olfs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "orderlineitems-") })
		pfs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "payments-") })

		if len(ofs) != len(olfs) || len(ofs) != len(pfs) {
			log.Fatal("Available data files for Orders, OrderLineItems an Payments don't match!")
		}

		// wait for products and accounts to be persistet
		wgs["Product"].Wait()
		wgs["Account"].Wait()

		for i := 0; i < len(ofs); i++ {
			wgs[group].Add(1)
			readersCh <- &processJob{
				files: map[string][]byte{
					ofs[i]:  nil,
					olfs[i]: nil,
					pfs[i]:  nil,
				},
				mapping: map[string]string{
					"orders":         ofs[i],
					"orderlineitems": olfs[i],
					"payments":       pfs[i],
				},
				lookup: lookup,
				processFunction: func(j *processJob, chs []chan map[string]*spanner.Mutation) error {
					var orders []*dbOrder
					err := gocsv.UnmarshalBytes(j.files[j.mapping["orders"]], &orders)
					if err != nil {
						return err
					}

					orderIDsCache := make(map[int64]*uuid.UUID)
					for _, o := range orders {
						aid, err := strconv.ParseInt(o.AccountID, 10, 64)
						if err != nil {
							log.Printf("Couldn't parse Order AccountID: %v", err)
							return err
						}
						auuid, ok := j.lookup["accounts"][aid]
						if !ok {
							log.Print("Error in lookup of Order AccountID. Discarding")
							return err
						}
						o.AccountID = string(encodeHexUUID(auuid))
						id := uuid.New()
						o.OrderID = string(encodeHexUUID(&id))
						orderIDsCache[o.OldID] = &id
						err = appendMutation(chs[0], "Order", o.OrderID, o)
						if err != nil {
							return err
						}
					}

					var ols []*dbOrderLineItem
					err = gocsv.UnmarshalBytes(j.files[j.mapping["orderlineitems"]], &ols)
					if err != nil {
						return err
					}

					for _, ol := range ols {
						oid, err := strconv.ParseInt(ol.OrderID, 10, 64)
						if err != nil {
							log.Printf("Couldn't parse OrderLineItem OrderID: %v", err)
							continue
						}
						ouuid, ok := orderIDsCache[oid]
						if !ok {
							log.Print("Error in lookup of OrderLineItem OrderID. Discarding")
							continue
						}

						pid, err := strconv.ParseInt(ol.ProductID, 10, 64)
						if err != nil {
							log.Printf("Couldn't parse OrderLineItem ProductID: %v", err)
							continue
						}
						puuid, ok := j.lookup["products"][pid]
						if !ok {
							log.Print("Error in lookup of OrderLineItem ProductID. Discarding")
							continue
						}

						ol.OrderID = string(encodeHexUUID(ouuid))
						ol.ProductID = string(encodeHexUUID(puuid))
						err = appendMutation(chs[1], "OrderLineItem", ol.OrderID, ol)
						if err != nil {
							return err
						}
					}

					var ps []*dbPayment
					err = gocsv.UnmarshalBytes(j.files[j.mapping["payments"]], &ps)
					if err != nil {
						return err
					}

					for _, p := range ps {
						oid, err := strconv.ParseInt(p.OrderID, 10, 64)
						if err != nil {
							log.Printf("Couldn't parse Payment OrderID: %v", err)
							continue
						}
						ouuid, ok := orderIDsCache[oid]
						if !ok {
							log.Print("Error in lookup of Payment OrderID. Discarding")
							continue
						}

						p.OrderID = string(encodeHexUUID(ouuid))
						err = appendMutation(chs[2], "Payment", p.OrderID, p)
						if err != nil {
							return err
						}
					}
					j.wgs["Order"].Done()
					return nil
				},
				wgs: wgs,
			}
		}
		break

	case "Product":
		// persist Products and ProductInfo, ProductPrices, ProductStocks in groups together
		pfs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "products-") })
		pifs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "productinfos-") })
		psfs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "productstocks-") })
		ppfs := filterStrings(files,
			func(s string) bool { return strings.Contains(s, "productprices-") })

		if len(pfs) != len(pifs) || len(pfs) != len(psfs) || len(pfs) != len(ppfs) {
			log.Fatal("Available data files for Products, ProductInfos, ProductStocks and ProductPrices don't match!")
		}

		// wait for warehouses to be persistet
		wgs["Warehouse"].Wait()

		for i := 0; i < len(pfs); i++ {
			wgs[group].Add(1)
			readersCh <- &processJob{
				files: map[string][]byte{
					pfs[i]:  nil,
					pifs[i]: nil,
					psfs[i]: nil,
					ppfs[i]: nil,
				},
				mapping: map[string]string{
					"products":      pfs[i],
					"productinfos":  pifs[i],
					"productstocks": psfs[i],
					"productprices": ppfs[i],
				},
				lookup: lookup,
				processFunction: func(j *processJob, chs []chan map[string]*spanner.Mutation) error {
					var ps []*dbProduct
					err := gocsv.UnmarshalBytes(j.files[j.mapping["products"]], &ps)
					if err != nil {
						return err
					}

					var pis []*dbProductInfo
					err = gocsv.UnmarshalBytes(j.files[j.mapping["productinfos"]], &pis)
					if err != nil {
						return err
					}

					var pps []*dbProductPrice
					err = gocsv.UnmarshalBytes(j.files[j.mapping["productprices"]], &pps)
					if err != nil {
						return err
					}

					var pss []*dbProductStock
					err = gocsv.UnmarshalBytes(j.files[j.mapping["productstocks"]], &pss)
					if err != nil {
						return err
					}

					pisidx, ppsidx, pssidx := 0, 0, 0

					for _, p := range ps {
						id := uuid.New()
						p.ProductID = string(encodeHexUUID(&id))
						j.lookup["products"][p.OldID] = &id

						err = appendMutation(chs[0], "Product", p.ProductID, p)
						if err != nil {
							return err
						}
						for i := pisidx; i < len(pis); i++ {
							if pis[i].ProductID != strconv.FormatInt(p.OldID, 10) {
								break
							}
							pisidx = i + 1
							pis[i].ProductID = p.ProductID
							err = appendMutation(chs[1], "ProductInfo", pis[i].ProductID, pis[i])
							if err != nil {
								return err
							}
						}

						for i := ppsidx; i < len(pps); i++ {
							if pps[i].ProductID != strconv.FormatInt(p.OldID, 10) {
								break
							}
							ppsidx = i + 1
							pps[i].ProductID = p.ProductID
							err = appendMutation(chs[2], "ProductPrice", pps[i].ProductID, pps[i])
							if err != nil {
								return err
							}
						}

						for i := pssidx; i < len(pss); i++ {
							if pss[i].ProductID != strconv.FormatInt(p.OldID, 10) {
								break
							}
							pssidx = i + 1
							lid, err := strconv.ParseInt(pss[i].LocationID, 10, 64)
							if err != nil {
								log.Printf("Couldn't parse ProductStock LocationID: %v", err)
								continue
							}
							luuid, ok := j.lookup["warehouses"][lid]
							if !ok {
								log.Print("Error in lookup of ProductStock LocationID. Discarding")
								continue
							}
							pss[i].LocationID = string(encodeHexUUID(luuid))

							pss[i].ProductID = p.ProductID
							err = appendMutation(chs[3], "ProductStock", pss[i].ProductID, pss[i])
							if err != nil {
								return err
							}
						}
					}
					j.wgs["Product"].Done()
					return nil
				},
				wgs: wgs,
			}
		}
		break

	case "Warehouse":
		// persist Warehouses
		for _, f := range filterStrings(files,
			func(s string) bool { return strings.Contains(s, "warehouses-") }) {
			wgs[group].Add(1)
			readersCh <- &processJob{
				files: map[string][]byte{
					f: nil,
				},
				mapping: map[string]string{
					"warehouses": f,
				},
				lookup: lookup,
				processFunction: func(j *processJob, chs []chan map[string]*spanner.Mutation) error {
					gocsv.UnmarshalBytesToCallback(j.files[j.mapping["warehouses"]], func(w *dbWarehouse) error {
						id := uuid.New()
						w.LocationID = string(encodeHexUUID(&id))
						j.lookup["warehouses"][w.OldID] = &id
						return appendMutation(chs[0], "Warehouse", w.LocationID, w)
					})
					j.wgs["Warehouse"].Done()
					return nil
				},
				wgs: wgs,
			}
		}
		break
	default:
		log.Fatalf("Group type '%v' not supported in persistObjects!", group)
	}
	return nil
}
