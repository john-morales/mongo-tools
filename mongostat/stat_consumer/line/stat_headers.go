// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package line

import (
	"github.com/mongodb/mongo-tools/mongostat/status"
)

// Flags to determine cases when to activate/deactivate columns for output.
const (
	FlagAlways   = 1 << iota // always activate the column
	FlagHosts                // only active if we may have multiple hosts
	FlagDiscover             // only active when mongostat is in discover mode
	FlagMetrics              // only active if node supports metrics
	FlagRepl                 // only active if one of the nodes being monitored is in a replset
	FlagLocks                // only active if node is capable of calculating lock info
	FlagCollectionLocks      // only active if node is capable of calculating collection lock info
	FlagOpLatencies          // only active if node is capable of calculating op latencies
	FlagAll                  // only active if mongostat was run with --all option
	FlagMMAP                 // only active if node has mmap-specific fields
	FlagWT                   // only active if node has wiredtiger-specific fields
)

// StatHeader describes a single column for mongostat's terminal output,
// its formatting, and in which modes it should be displayed.
type StatHeader struct {
	// ReadField produces a particular field according to the StatHeader instance.
	// Some fields are based on a diff, so both latest ServerStatuses are taken.
	ReadField func(c *status.ReaderConfig, newStat, oldStat *status.ServerStatus) string
}

// StatHeaders are the complete set of data metrics supported by mongostat.
var (
	keyNames = map[string][]string{ // short, long, deprecated
		"host":           {"host", "Host", "host"},
		"storage_engine": {"storage_engine", "Storage engine", "engine"},
		"insert":         {"insert", "Insert opcounter (diff)", "insert"},
		"query":          {"query", "Query opcounter (diff)", "query"},
		"update":         {"update", "Update opcounter (diff)", "update"},
		"delete":         {"delete", "Delete opcounter (diff)", "delete"},
		"getmore":        {"getmore", "GetMore opcounter (diff)", "getmore"},
		"command":        {"command", "Command opcounter (diff)", "command"},
		"dirty":          {"dirty", "Cache dirty (percentage)", "% dirty"},
		"used":           {"used", "Cache used (percentage)", "% used"},
		"read":           {"read", "Cache bytes read into (diff)", "read"},
		"written":        {"written", "Cache bytes written from (diff)", "written"},
		"pread":          {"pread", "Cache pages read into (diff)", "pread"},
		"preq":           {"preq", "Cache pages requested (diff)", "preq"},
		"pwritten":       {"pwritten", "Cache pages written from (diff)", "pwritten"},
		"pagehit%":       {"pagehit%", "Cache page hit ratio (percentage)", "pagehit%"},
		"evict-um":       {"evict-um", "Cache unmodified pages evicted (diff)", "evict-um"},
		"evict-m":        {"evict-m", "Cache modified pages evicted (diff)", "evict-m"},
		"evict-i":        {"evict-i", "Cache internal pages evicted (diff)", "evict-i"},
		"r%|w%|em%|eum%": {"r%|w%|em%|eum%", "Cache page stats (percentage)", "r%|w%|em%|eum%"},
		"flushes":        {"flushes", "Number of flushes (diff)", "flushes"},
		"mapped":         {"mapped", "Mapped (size)", "mapped"},
		"vsize":          {"vsize", "Virtual (size)", "vsize"},
		"res":            {"res", "Resident (size)", "res"},
		"nonmapped":      {"nonmapped", "Non-mapped (size)", "non-mapped"},
		"faults":         {"faults", "Page faults (diff)", "faults"},
		"lrw":            {"lrw", "Lock acquire count, read|write (diff percentage)", "lr|lw %"},
		"lrwt":           {"lrwt", "Lock acquire time, read|write (diff percentage)", "lrt|lwt"},
		"locked_db":      {"locked_db", "Locked db info, '(db):(percentage)'", "locked"},

		"sao":            {"sao", "Scan and Order (diff)", "sao"},
		"wc":             {"wc", "Write Conflicts (diff)", "wc"},
		"ns":             {"ns", "NScanned (diff)", "ns"},
		"nso":            {"nso", "NScanned Objects (diff)", "nso"},
		"effic":          {"effic", "Query Efficiency: max(nscanned, nscannedObjects)/nreturned (ratio)", "effic"},
		"r|i|u|d":        {"r|i|u|d", "Document metrics Returned|Inserted|Updated|Deleted (diff)", "r|i|u|d"},
		"moves":          {"moves", "Document moves (diff)", "moves"},
		"gleto":          {"gleto", "Get Last Error timeouts (diff)", "gleto"},
		"glems":          {"glems", "Average time waiting for GLE (millis)", "glems"},
		"r|w|c":          {"r|w|c", "Average execution time per read/write/command (millis)", "r|w|c"},
		"r%|w%|c%":       {"r%|w%|c%", "Average utilization percent per read/write/command (diff percentage)", "r%|w%|c%"},
		"appr%|appw%":    {"appr%|appw%", "Average utilization percent application threads page read from disk to cache time (usecs)", "appr%|appw%"},

		"qrw":            {"qrw", "Queued accesses, read|write", "qr|qw"},
		"arw":            {"arw", "Active accesses, read|write", "ar|aw"},
		"net_in":         {"net_in", "Network input (size)", "netIn"},
		"net_out":        {"net_out", "Network output (size)", "netOut"},
		"conn":           {"conn", "Current connection count", "conn"},
		"set":            {"set", "FlagReplica set name", "set"},
		"repl":           {"repl", "FlagReplica set type", "repl"},
		"time":           {"time", "Time of sample", "time"},
	}
	StatHeaders = map[string]StatHeader{
		"host":           {status.ReadHost},
		"storage_engine": {status.ReadStorageEngine},
		"insert":         {status.ReadInsert},
		"query":          {status.ReadQuery},
		"update":         {status.ReadUpdate},
		"delete":         {status.ReadDelete},
		"getmore":        {status.ReadGetMore},
		"command":        {status.ReadCommand},
		"dirty":          {status.ReadDirty},
		"used":           {status.ReadUsed},
		"read":           {status.ReadCacheBytesReadInto},
		"written":        {status.ReadCacheBytesWrittenFrom},
		"pread":          {status.ReadCachePagesReadInto},
		"preq":           {status.ReadCachePagesRequested},
		"pwritten":       {status.ReadCachePagesWrittenFrom},
		"pagehit%":       {status.ReadCachePageHitRatio},
		"evict-um":       {status.ReadEvictedUnmodified},
		"evict-m":        {status.ReadEvictedModified},
		"evict-i":        {status.ReadEvictedInternal},
		"r%|w%|em%|eum%": {status.ReadCachePercentages},
		"flushes":        {status.ReadFlushes},
		"mapped":         {status.ReadMapped},
		"vsize":          {status.ReadVSize},
		"res":            {status.ReadRes},
		"nonmapped":      {status.ReadNonMapped},
		"faults":         {status.ReadFaults},
		"lrw":            {status.ReadLRW},
		"lrwt":           {status.ReadLRWT},
		"locked_db":      {status.ReadLockedDB},

		"sao":       	  {status.ReadScanAndOrders},
		"wc":       	  {status.ReadWriteConflicts},
		"ns":       	  {status.ReadNScanned},
		"nso":       	  {status.ReadNScannedObjects},
		"effic":       	  {status.ReadQueryEfficiency},
		"r|i|u|d":        {status.ReadDocumentStats},
		"moves":          {status.ReadMoves},
		"gleto":          {status.ReadGLETimeouts},
		"glems":          {status.ReadGLEMillis},
		"r|w|c":          {status.ReadOpLatencies},
		"r%|w%|c%":       {status.ReadOpLatencyUtilPercent},
		"appr%|appw%":    {status.ReadApplicationThreadPageToCachePercent},

		"qrw":            {status.ReadQRW},
		"arw":            {status.ReadARW},
		"net_in":         {status.ReadNetIn},
		"net_out":        {status.ReadNetOut},
		"conn":           {status.ReadConn},
		"set":            {status.ReadSet},
		"repl":           {status.ReadRepl},
		"time":           {status.ReadTime},
	}
	CondHeaders = []struct {
		Key  string
		Flag int
	}{
		{"host", FlagHosts},
		{"insert", FlagAlways},
		{"query", FlagAlways},
		{"update", FlagAlways},
		{"delete", FlagAlways},
		{"getmore", FlagAlways},
		{"command", FlagAlways},
		{"dirty", FlagWT},
		{"used", FlagWT},
		{"read", FlagWT},
		{"written", FlagWT},
		{"pread", FlagWT},
		{"preq", FlagWT},
		{"pwritten", FlagWT},
		{"pagehit%", FlagWT},
		{"evict-um", FlagWT},
		{"evict-m", FlagWT},
		{"evict-i", FlagWT},
		{"r%|w%|em%|eum%", FlagWT},
		{"flushes", FlagAlways},
		{"mapped", FlagMMAP},
		{"vsize", FlagAlways},
		{"res", FlagAlways},
		{"nonmapped", FlagMMAP | FlagAll},
		{"faults", FlagMMAP},
		{"lrw", FlagMMAP | FlagCollectionLocks | FlagAll},
		{"lrwt", FlagMMAP | FlagCollectionLocks | FlagAll},
		{"locked_db", FlagLocks},

		{"sao", FlagMetrics | FlagAll},
		{"wc", FlagMetrics | FlagAll},
		{"ns", FlagMetrics | FlagAll},
		{"nso", FlagMetrics | FlagAll},
		{"effic", FlagMetrics | FlagAll},
		{"r|i|u|d", FlagMetrics | FlagAll},
		{"moves", FlagMetrics | FlagMMAP | FlagAll},
		{"gleto", FlagMetrics | FlagAll},
		{"glems", FlagMetrics | FlagAll},
		{"r|w|c", FlagOpLatencies},
		{"r%|w%|c%", FlagOpLatencies},
		{"appr%|appw%", FlagWT},

		{"qrw", FlagAlways},
		{"arw", FlagAlways},
		{"net_in", FlagAlways},
		{"net_out", FlagAlways},
		{"conn", FlagAlways},
		{"set", FlagRepl},
		{"repl", FlagRepl},
		{"time", FlagAlways},
	}
)

func defaultKeyMap(index int) map[string]string {
	names := make(map[string]string)
	for k, v := range keyNames {
		names[k] = v[index]
	}
	return names
}

func DefaultKeyMap() map[string]string {
	return defaultKeyMap(0)
}

func LongKeyMap() map[string]string {
	return defaultKeyMap(1)
}

func DeprecatedKeyMap() map[string]string {
	return defaultKeyMap(2)
}
