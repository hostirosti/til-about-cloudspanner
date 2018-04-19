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
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gocarina/gocsv"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSclient is an authenticated Cloud Storage client
type GCSclient struct {
	*storage.Client
}

// NewGCSService creates new authenticated Cloud Storage client with the given
// scope and service account. If no service account is provided, the default
// auth context is used.
func NewGCSService(svcAccJSON string, scopes ...string) (*GCSclient, error) {
	log.Println("Connecting to Google Cloud Storage ...")
	var opts []option.ClientOption
	if _, err := os.Stat(svcAccJSON); err == nil {
		opts = append(opts, option.WithServiceAccountFile(svcAccJSON))
	}

	opts = append(opts, option.WithScopes(scopes...))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := storage.NewClient(ctx, opts...)

	if err != nil {
		return nil, err
	}
	return &GCSclient{c}, nil
}

// list for passed bucketName filtered by passed FilePrefix
func (svc *GCSclient) list(bucketName string, filePrefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	// List all objects in a bucket using pagination
	var files []string
	it := svc.Bucket(bucketName).Objects(ctx, &storage.Query{Prefix: filePrefix})
	for {
		obj, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		files = append(files, obj.Name)
	}
	sort.Strings(files)
	return files, nil
}

func (svc *GCSclient) read(bucketName, filepath string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	fr, err := svc.Bucket(bucketName).Object(filepath).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer fr.Close()
	c, err := ioutil.ReadAll(fr)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (svc *GCSclient) write(bucket, filepath string, fileContent []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()
	wc := svc.Bucket(bucket).Object(filepath).NewWriter(ctx)
	_, err := wc.Write(fileContent)
	if err != nil {
		return err
	}
	if err := wc.Close(); err != nil {
		return err
	}
	return nil
}

func (svc *GCSclient) writeCSV(wg *sync.WaitGroup, bucket, folderPath, filePrefix string, idx int, objs interface{}) error {
	wg.Add(1)
	defer wg.Done()
	k := reflect.TypeOf(objs).Kind()
	switch k {
	case reflect.Slice:
		fp := fmt.Sprintf("%v/%v-%04d.csv", folderPath, filePrefix, idx)
		if reflect.ValueOf(objs).Len() > 0 {
			csv, err := gocsv.MarshalString(objs)
			if err != nil {
				log.Print(err)
				return err
			}
			err = svc.write(bucket, fp, []byte(csv))
			if err != nil {
				log.Print(err)
				return err
			}
		}
	default:
		log.Fatalf("Type '%v' in toCSV as objs not supported!", k)
	}
	return nil
}
