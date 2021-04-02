// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

// Package mongotop provides a method to track the amount of time a MongoDB instance spends reading and writing data.
package mongotop

import (
	"context"
	"fmt"
	"time"

	"github.com/mongodb/mongo-tools-common/db"
	"github.com/mongodb/mongo-tools-common/log"
	"github.com/mongodb/mongo-tools-common/options"
	"github.com/mongodb/mongo-tools-common/util"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

// MongoTop is a container for the user-specified options and
// internal state used for running mongotop.
type MongoTop struct {
	// For busy %
	NumCores int

	// Generic mongo tool options
	Options *options.ToolOptions

	// Mongotop-specific output options
	OutputOptions *Output

	// for connecting to the db
	SessionProvider *db.SessionProvider

	// Length of time to sleep between each polling.
	Sleeptime time.Duration

	previousOperationMetrics *OperationMetrics
	previousServerStatus     *ServerStatus
	previousTop              *Top
}

func (mt *MongoTop) runDiff() (outDiff FormattableDiff, err error) {
	if mt.OutputOptions.Locks {
		return mt.runServerStatusDiff()
	} else if mt.OutputOptions.OperationMetrics {
		return mt.runOperationMetricsDiff()
	}
	return mt.runTopDiff()
}

func (mt *MongoTop) runTopDiff() (outDiff FormattableDiff, err error) {
	now := time.Now()
	commandName := "top"
	dest := &bsonx.Doc{}
	err = mt.SessionProvider.RunString(commandName, dest, "admin")
	if err != nil {
		mt.previousTop = nil
		return nil, err
	}
	// Remove 'note' field that prevents easy decoding, then round-trip
	// again to simplify unpacking into the nested data structure
	totals, err := dest.LookupErr("totals")
	if err != nil {
		return nil, err
	}
	recoded, err := totals.Document().Delete("note").MarshalBSON()
	if err != nil {
		return nil, err
	}
	topinfo := make(map[string]NSTopInfo)
	err = bson.Unmarshal(recoded, &topinfo)
	if err != nil {
		return nil, err
	}
	currentTop := Top{Totals: topinfo}
	currentTop.time = now
	currentTop.numCores = mt.NumCores
	if mt.previousTop != nil {
		topDiff := currentTop.Diff(*mt.previousTop, mt.OutputOptions.ListCount, mt.OutputOptions.SortLatency)
		outDiff = topDiff
	}
	mt.previousTop = &currentTop
	return outDiff, nil
}

func (mt *MongoTop) runServerStatusDiff() (outDiff FormattableDiff, err error) {
	now := time.Now()
	var currentServerStatus ServerStatus
	commandName := "serverStatus"
	var dest interface{} = &currentServerStatus
	err = mt.SessionProvider.RunString(commandName, dest, "admin")
	if err != nil {
		mt.previousServerStatus = nil
		return nil, err
	}
	if currentServerStatus.Locks == nil {
		return nil, fmt.Errorf("server does not support reporting lock information")
	}
	for _, ns := range currentServerStatus.Locks {
		if ns.AcquireCount != nil {
			return nil, fmt.Errorf("server does not support reporting lock information")
		}
	}
	currentServerStatus.time = now
	if mt.previousServerStatus != nil {
		serverStatusDiff := currentServerStatus.Diff(*mt.previousServerStatus, mt.OutputOptions.ListCount)
		outDiff = serverStatusDiff
	}
	mt.previousServerStatus = &currentServerStatus
	return outDiff, nil
}

func (mt *MongoTop) runOperationMetricsDiff() (outDiff FormattableDiff, err error) {
	now := time.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pipeline := []bson.M{{"$operationMetrics": bson.M{}}}
	cursor, err := mt.SessionProvider.DB("admin").Aggregate(ctx, pipeline)
	if err != nil {
		mt.previousOperationMetrics = nil
		return nil, err
	}
	defer cursor.Close(ctx)

	currentOperationMetrics := OperationMetrics{
		numCores: mt.NumCores,
		time:     now,
		Entries:  make(map[string]OperationMetricsEntry),
	}

	for cursor.Next(ctx) {
		var entry OperationMetricsEntry
		err := cursor.Decode(&entry)
		if err != nil {
			return nil, fmt.Errorf("failure decoding from cursor, err: %v", err)
		}

		currentOperationMetrics.Entries[entry.DBName] = entry
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("failure reading from cursor, err: %v", err)
	}

	if mt.previousOperationMetrics != nil {
		diff := currentOperationMetrics.Diff(*mt.previousOperationMetrics, mt.OutputOptions.ListCount, mt.OutputOptions.SortLatency)
		outDiff = diff
	}
	mt.previousOperationMetrics = &currentOperationMetrics
	return outDiff, nil
}

// Run executes the mongotop program.
func (mt *MongoTop) Run() error {
	hasData := false
	numPrinted := 0

	for {
		if mt.OutputOptions.RowCount > 0 && numPrinted > mt.OutputOptions.RowCount {
			return nil
		}
		numPrinted++
		diff, err := mt.runDiff()
		if err != nil {
			// If this is the first time trying to poll the server and it fails,
			// just stop now instead of trying over and over.
			if !hasData {
				return err
			}

			log.Logvf(log.Always, "Error: %v\n", err)
			time.Sleep(mt.Sleeptime)
		}

		// if this is the first time and the connection is successful, print
		// the connection message
		if !hasData && !mt.OutputOptions.Json {
			log.Logvf(log.Always, "connected to: %v\n", util.SanitizeURI(mt.Options.URI.ConnectionString))
		}

		hasData = true

		if diff != nil {
			if mt.OutputOptions.Json {
				fmt.Println(diff.JSON())
			} else {
				fmt.Println(diff.Grid())
			}
		}
		time.Sleep(mt.Sleeptime)
	}
}
