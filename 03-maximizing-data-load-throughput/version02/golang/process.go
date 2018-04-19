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
	"log"
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

type processJob struct {
	wgs             map[string]*sync.WaitGroup
	files           map[string][]byte
	mapping         map[string]string
	lookup          map[string]map[int64]*uuid.UUID
	processFunction func(j *processJob, chs []chan map[string]*spanner.Mutation) error
}

func readWorker(id int, gcs *GCSclient, in <-chan *processJob, out chan<- *processJob) {
	for j := range in {
		for f := range j.files {
			var err error
			log.Printf("Reading file %v", f)
			j.files[f], err = gcs.read(*bucketName, f)
			if err != nil {
				log.Printf("ReadWorker %v -- Couldn't read file %v: %v", id, f, err)
			}
		}
		out <- j
	}
}

func processWorker(id int, in <-chan *processJob, chs []chan map[string]*spanner.Mutation) {
	for j := range in {
		err := j.processFunction(j, chs)
		if err != nil {
			log.Printf("ProcessWorker %v -- Couldn't process job: %v", id, err)
		}
	}
}
