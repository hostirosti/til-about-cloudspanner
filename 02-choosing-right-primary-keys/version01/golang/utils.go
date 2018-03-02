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
	"encoding/hex"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

type uuidHex string

func decodeHexUUIDStr(h uuidHex) (string, error) {
	id, err := decodeHexUUID(h)
	if err != nil {
		return "", err
	}
	return id.String(), nil
}

func decodeHexUUID(h uuidHex) (*uuid.UUID, error) {
	b, err := hex.DecodeString(string(h))
	if err != nil {
		return new(uuid.UUID), err
	}
	var id = new(uuid.UUID)
	if err := id.UnmarshalBinary(b); err != nil {
		return id, err
	}
	return id, nil
}

func encodeHexUUIDStr(id string) (uuidHex, error) {
	u, err := uuid.Parse(id)
	if err != nil {
		debugLog.Printf("Failed UUID string: %v", id)
		return "", err
	}

	b, err := u.MarshalBinary()
	if err != nil {
		return "", err
	}
	return uuidHex(hex.EncodeToString(b)), nil
}

func encodeHexUUID(id *uuid.UUID) uuidHex {
	b, err := id.MarshalBinary()
	if err != nil {
		log.Fatal(err)
	}

	return uuidHex(hex.EncodeToString(b))
}

type debugging bool

// debugging log
func (d debugging) Printf(format string, args ...interface{}) {
	if d {
		log.Printf(format, args...)
	}
}

func (d debugging) Print(args ...interface{}) {
	if d {
		log.Print(args...)
	}
}

func (d debugging) Println(args ...interface{}) {
	if d {
		log.Println(args...)
	}
}

// taken from https://stackoverflow.com/questions/43495745/how-to-generate-random-date-in-go-lang
func randomDate() time.Time {
	min := time.Date(2001, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2018, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	d := max - min

	sec := rand.Int63n(d) + min
	return time.Unix(sec, 0)
}

func filterStrings(sa []string, f func(string) bool) (res []string) {
	for _, s := range sa {
		if f(s) {
			res = append(res, s)
		}
	}
	return
}
