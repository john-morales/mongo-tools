// Copyright (C) MongoDB, Inc. 2014-present.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

package mongotop

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/mongodb/mongo-tools-common/text"
)

// FormattableDiff represents a diff of two samples taken by mongotop,
// which can be printed to output in various formats.
type FormattableDiff interface {
	// Generate a JSON representation of the diff
	JSON() string
	// Generate a table-like representation which can be printed to a terminal
	Grid() string
}

type HostInfo struct {
	System HostInfoSystem `bson:"system"`
}

type HostInfoSystem struct {
	NumCores int `bson:"numCores"`
}

// ServerStatus represents the results of the "serverStatus" command.
type ServerStatus struct {
	time  time.Time
	Locks map[string]LockStats `bson:"locks,omitempty"`
}

// LockStats contains information on time spent acquiring and holding a lock.
type LockStats struct {
	AcquireCount        *ReadWriteLockTimes `bson:"acquireCount"`
	TimeLockedMicros    ReadWriteLockTimes  `bson:"timeLockedMicros"`
	TimeAcquiringMicros ReadWriteLockTimes  `bson:"timeAcquiringMicros"`
}

// ReadWriteLockTimes contains read/write lock times on a database.
type ReadWriteLockTimes struct {
	Read       int64 `bson:"R"`
	Write      int64 `bson:"W"`
	ReadLower  int64 `bson:"r"`
	WriteLower int64 `bson:"w"`
}

// ServerStatusDiff contains a map of the lock time differences for each database.
type ServerStatusDiff struct {
	currentServerStatus *ServerStatus
	listCount           int
	// namespace -> lock times
	Totals map[string]LockDelta `json:"totals"`
	Time   time.Time            `json:"time"`
}

// LockDelta represents the differences in read/write lock times between two samples.
type LockDelta struct {
	Read  int64 `json:"read"`
	Write int64 `json:"write"`
}

type OperationMetricsDiff struct {
	numCores         int
	elapsed          time.Duration
	currentOperation *OperationMetrics
	listCount        int
	sortTotal        bool
	// namespace -> deltas
	Totals map[string]OperationMetricsEntryDelta `json:"totals"`
	Time   time.Time                             `json:"time"`
}

type OperationMetrics struct {
	numCores int
	time     time.Time

	Entries map[string]OperationMetricsEntry
}

type OperationMetricsEntry struct {
	DBName string `bson:"db"`

	PrimaryMetrics   OperationMetricsMemberInfo `bson:"primaryMetrics"`
	SecondaryMetrics OperationMetricsMemberInfo `bson:"secondaryMetrics"`

	DocBytesWritten        int64 `bson:"docBytesWritten"`
	DocUnitsWritten        int64 `bson:"docUnitsWritten"`
	IndexEntryBytesWritten int64 `bson:"idxEntryBytesWritten"`
	IndexEntryUnitsWritten int64 `bson:"idxEntryUnitsWritten"`
	CpuNanos               int64 `bson:"cpuNanos"`
}

func (e OperationMetricsEntry) TotalDocUnits() int64 {
	return e.PrimaryMetrics.DocUnitsRead + e.SecondaryMetrics.DocUnitsRead + e.DocUnitsWritten
}

type OperationMetricsMemberInfo struct {
	DocBytesRead        int64 `bson:"docBytesRead"`
	DocUnitsRead        int64 `bson:"docUnitsRead"`
	IndexEntryBytesRead int64 `bson:"idxEntryBytesRead"`
	IndexEntryUnitsRead int64 `bson:"idxEntryUnitsRead"`
	KeysSorted          int64 `bson:"keysSorted"`
	SorterSpills        int64 `bson:"sorterSpills"`
	DocUnitsReturned    int64 `bson:"docUnitsReturned"`
	CursorSeeks         int64 `bson:"cursorSeeks"`
}

type OperationMetricsEntryDelta struct {
	PrimaryMetrics   OperationMetricsMemberInfoDelta
	SecondaryMetrics OperationMetricsMemberInfoDelta

	DocBytesWritten        int64
	DocUnitsWritten        int64
	IndexEntryBytesWritten int64
	IndexEntryUnitsWritten int64
	CpuNanos               int64
}

func (d OperationMetricsEntryDelta) TotalDocUnits() int64 {
	return d.PrimaryMetrics.DocUnitsRead + d.SecondaryMetrics.DocUnitsRead + d.DocUnitsWritten
}

type OperationMetricsMemberInfoDelta struct {
	DocBytesRead        int64
	DocUnitsRead        int64
	IndexEntryBytesRead int64
	IndexEntryUnitsRead int64
	KeysSorted          int64
	SorterSpills        int64
	DocUnitsReturned    int64
	CursorSeeks         int64
}

// Diff takes an older sample, and produces a OperationMetricsDiff
// representing the deltas of each metric between the two samples.
func (metrics OperationMetrics) Diff(previous OperationMetrics, listCount int, sortTotal bool) OperationMetricsDiff {
	// The diff to eventually return
	diff := OperationMetricsDiff{
		numCores:         previous.numCores,
		elapsed:          metrics.time.Sub(previous.time),
		currentOperation: &metrics,
		listCount:        listCount,
		sortTotal:        sortTotal,
		Totals:           map[string]OperationMetricsEntryDelta{},
		Time:             time.Now(),
	}

	// For each namespace we are tracking, subtract the times and counts
	// for total/read/write and build a new map containing the diffs.
	prevEntries := previous.Entries
	curEntries := metrics.Entries
	for ns, prevInfo := range prevEntries {
		if curInfo, ok := curEntries[ns]; ok {
			diff.Totals[ns] = OperationMetricsEntryDelta{
				PrimaryMetrics: OperationMetricsMemberInfoDelta{
					DocBytesRead:        curInfo.PrimaryMetrics.DocBytesRead - prevInfo.PrimaryMetrics.DocBytesRead,
					DocUnitsRead:        curInfo.PrimaryMetrics.DocUnitsRead - prevInfo.PrimaryMetrics.DocUnitsRead,
					IndexEntryBytesRead: curInfo.PrimaryMetrics.IndexEntryBytesRead - prevInfo.PrimaryMetrics.IndexEntryBytesRead,
					IndexEntryUnitsRead: curInfo.PrimaryMetrics.IndexEntryUnitsRead - prevInfo.PrimaryMetrics.IndexEntryUnitsRead,
					KeysSorted:          curInfo.PrimaryMetrics.KeysSorted - prevInfo.PrimaryMetrics.KeysSorted,
					SorterSpills:        curInfo.PrimaryMetrics.SorterSpills - prevInfo.PrimaryMetrics.SorterSpills,
					DocUnitsReturned:    curInfo.PrimaryMetrics.DocUnitsReturned - prevInfo.PrimaryMetrics.DocUnitsReturned,
					CursorSeeks:         curInfo.PrimaryMetrics.CursorSeeks - prevInfo.PrimaryMetrics.CursorSeeks,
				},
				SecondaryMetrics: OperationMetricsMemberInfoDelta{
					DocBytesRead:        curInfo.SecondaryMetrics.DocBytesRead - prevInfo.SecondaryMetrics.DocBytesRead,
					DocUnitsRead:        curInfo.SecondaryMetrics.DocUnitsRead - prevInfo.SecondaryMetrics.DocUnitsRead,
					IndexEntryBytesRead: curInfo.SecondaryMetrics.IndexEntryBytesRead - prevInfo.SecondaryMetrics.IndexEntryBytesRead,
					IndexEntryUnitsRead: curInfo.SecondaryMetrics.IndexEntryUnitsRead - prevInfo.SecondaryMetrics.IndexEntryUnitsRead,
					KeysSorted:          curInfo.SecondaryMetrics.KeysSorted - prevInfo.SecondaryMetrics.KeysSorted,
					SorterSpills:        curInfo.SecondaryMetrics.SorterSpills - prevInfo.SecondaryMetrics.SorterSpills,
					DocUnitsReturned:    curInfo.SecondaryMetrics.DocUnitsReturned - prevInfo.SecondaryMetrics.DocUnitsReturned,
					CursorSeeks:         curInfo.SecondaryMetrics.CursorSeeks - prevInfo.SecondaryMetrics.CursorSeeks,
				},
				DocBytesWritten:        curInfo.DocBytesWritten - prevInfo.DocBytesWritten,
				DocUnitsWritten:        curInfo.DocUnitsWritten - prevInfo.DocUnitsWritten,
				IndexEntryBytesWritten: curInfo.IndexEntryBytesWritten - prevInfo.IndexEntryBytesWritten,
				IndexEntryUnitsWritten: curInfo.IndexEntryUnitsWritten - prevInfo.IndexEntryUnitsWritten,
				CpuNanos:               curInfo.CpuNanos - prevInfo.CpuNanos,
			}
		}
	}
	return diff
}

func (od OperationMetricsDiff) Grid() string {
	listCount := od.listCount
	if listCount == 0 {
		listCount = 9
	}

	buf := &bytes.Buffer{}
	out := &text.GridWriter{ColumnPadding: 4}
	out.WriteCells("                                              ns", "||TOTAL||", "total Units/s", "total RUnits/s", "total WUnits/s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	out.EndRow()

	totals := make(sortableTotals, 0, len(od.Totals))
	for ns, diff := range od.Totals {
		if od.sortTotal {
			//Sort by total doc units
			totals = append(totals, sortableTotal{ns, float64(diff.TotalDocUnits()), int64(od.currentOperation.Entries[ns].TotalDocUnits())})
		} else {
			//Sort by cpuNanos
			totals = append(totals, sortableTotal{ns, float64(diff.CpuNanos), int64(od.currentOperation.Entries[ns].CpuNanos)})
		}
	}

	elapsedSeconds := float64(int64(od.elapsed) / 1e9)
	sort.Sort(sort.Reverse(totals))
	for i, st := range totals {
		diff := od.Totals[st.Name]
		out.WriteCells(st.Name,
			"",
			fmt.Sprintf("%0.1fUnits/s", float64(diff.TotalDocUnits())/elapsedSeconds),
			fmt.Sprintf("%0.1fRUnits/s", float64(diff.PrimaryMetrics.DocUnitsRead+diff.SecondaryMetrics.DocUnitsRead)/elapsedSeconds),
			fmt.Sprintf("%0.1fWUnits/s", float64(diff.DocUnitsWritten)/elapsedSeconds),
			"")
		out.EndRow()
		if i >= listCount-1 {
			break
		}
	}
	out.Flush(buf)
	return buf.String()
}

func (od OperationMetricsDiff) JSON() string {
	return "{\"unsupported\": true}"
}

// TopDiff contains a map of the differences between top samples for each namespace.
type TopDiff struct {
	numCores    int
	elapsed     time.Duration
	currentTop  *Top
	listCount   int
	sortLatency bool
	// namespace -> totals
	Totals map[string]NSTopInfo `json:"totals"`
	Time   time.Time            `json:"time"`
}

// Top holds raw output of the "top" command.
type Top struct {
	numCores int
	time     time.Time
	Totals   map[string]NSTopInfo `bson:"totals"`
}

// NSTopInfo holds information about a single namespace.
type NSTopInfo struct {
	Total TopField `bson:"total" json:"total"`
	Read  TopField `bson:"readLock" json:"read"`
	Write TopField `bson:"writeLock" json:"write"`
}

// TopField contains the timing and counts for a single lock statistic within the "top" command.
type TopField struct {
	Time  int `bson:"time" json:"time"`
	Count int `bson:"count" json:"count"`
}

// struct to enable sorting of namespaces by lock time with the sort package
type sortableTotal struct {
	Name    string
	Total   float64
	Current int64
}

type sortableTotals []sortableTotal

func (a sortableTotals) Less(i, j int) bool {
	if !math.IsNaN(a[i].Total) && math.IsNaN(a[j].Total) {
		return false
	} else if math.IsNaN(a[i].Total) && !math.IsNaN(a[j].Total) {
		return true
	} else if math.IsNaN(a[i].Total) || math.IsNaN(a[j].Total) {
		return true
	}

	if a[i].Total == a[j].Total {
		if a[i].Current == a[j].Current {
			return a[i].Name > a[j].Name
		} else {
			return a[i].Current < a[j].Current
		}
	}
	return a[i].Total < a[j].Total
}
func (a sortableTotals) Len() int      { return len(a) }
func (a sortableTotals) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Diff takes an older Top sample, and produces a TopDiff
// representing the deltas of each metric between the two samples.
func (top Top) Diff(previous Top, listCount int, sortLatency bool) TopDiff {
	// The diff to eventually return
	diff := TopDiff{
		numCores:    previous.numCores,
		elapsed:     top.time.Sub(previous.time),
		currentTop:  &top,
		listCount:   listCount,
		sortLatency: sortLatency,
		Totals:      map[string]NSTopInfo{},
		Time:        time.Now(),
	}

	// For each namespace we are tracking, subtract the times and counts
	// for total/read/write and build a new map containing the diffs.
	prevTotals := previous.Totals
	curTotals := top.Totals
	for ns, prevNSInfo := range prevTotals {
		if curNSInfo, ok := curTotals[ns]; ok {
			diff.Totals[ns] = NSTopInfo{
				Total: TopField{
					Time:  (curNSInfo.Total.Time - prevNSInfo.Total.Time) / 1000,
					Count: curNSInfo.Total.Count - prevNSInfo.Total.Count,
				},
				Read: TopField{
					Time:  (curNSInfo.Read.Time - prevNSInfo.Read.Time) / 1000,
					Count: curNSInfo.Read.Count - prevNSInfo.Read.Count,
				},
				Write: TopField{
					Time:  (curNSInfo.Write.Time - prevNSInfo.Write.Time) / 1000,
					Count: curNSInfo.Write.Count - prevNSInfo.Write.Count,
				},
			}
		}
	}
	return diff
}

// Grid returns a tabular representation of the TopDiff.
func (td TopDiff) Grid() string {
	listCount := td.listCount
	if listCount == 0 {
		listCount = 9
	}

	buf := &bytes.Buffer{}
	out := &text.GridWriter{ColumnPadding: 4}
	out.WriteCells("                                              ns", "||TOTAL||", "total %", "total %/core", "time/op", "op/s", "||READ||", "read %", "time/op", "op/s", "||WRITE||", "write %", "time/op", "op/s", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	out.EndRow()

	totals := make(sortableTotals, 0, len(td.Totals))
	for ns, diff := range td.Totals {
		if td.sortLatency {
			//Sort by total latency (ms/op)
			totals = append(totals, sortableTotal{ns, float64(diff.Total.Time) / float64(diff.Total.Count), int64(td.currentTop.Totals[ns].Total.Time)})
		} else {
			//Sort by total time
			totals = append(totals, sortableTotal{ns, float64(diff.Total.Time), int64(td.currentTop.Totals[ns].Total.Time)})
		}
	}

	elapsedMillis := float64(int64(td.elapsed) / 1e6)
	elapsedSeconds := float64(int64(td.elapsed) / 1e9)
	sort.Sort(sort.Reverse(totals))
	for i, st := range totals {
		diff := td.Totals[st.Name]
		out.WriteCells(st.Name,
			fmt.Sprintf("%vms", diff.Total.Time),
			fmt.Sprintf("%0.1f%%", float64(diff.Total.Time)/elapsedMillis*100),
			fmt.Sprintf("%0.2f%%", float64(diff.Total.Time)/elapsedMillis*100/float64(td.numCores)),
			fmt.Sprintf("%0.1fms/op", float64(diff.Total.Time)/float64(diff.Total.Count)),
			fmt.Sprintf("%0.1fop/s", float64(diff.Total.Count)/elapsedSeconds),
			fmt.Sprintf("%vms", diff.Read.Time),
			fmt.Sprintf("%0.1f%%", float64(diff.Read.Time)/elapsedMillis*100),
			fmt.Sprintf("%0.1fms/op", float64(diff.Read.Time)/float64(diff.Read.Count)),
			fmt.Sprintf("%0.1fop/s", float64(diff.Read.Count)/elapsedSeconds),
			fmt.Sprintf("%vms", diff.Write.Time),
			fmt.Sprintf("%0.1f%%", float64(diff.Write.Time)/elapsedMillis*100),
			fmt.Sprintf("%0.1fms/op", float64(diff.Write.Time)/float64(diff.Write.Count)),
			fmt.Sprintf("%0.1fop/s", float64(diff.Write.Count)/elapsedSeconds),
			"")
		out.EndRow()
		if i >= listCount-1 {
			break
		}
	}
	out.Flush(buf)
	return buf.String()
}

// JSON returns a JSON representation of the TopDiff.
func (td TopDiff) JSON() string {
	bytes, err := json.Marshal(td)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// JSON returns a JSON representation of the ServerStatusDiff.
func (ssd ServerStatusDiff) JSON() string {
	bytes, err := json.Marshal(ssd)
	if err != nil {
		panic(err)
	}
	return string(bytes)
}

// Grid returns a tabular representation of the ServerStatusDiff.
func (ssd ServerStatusDiff) Grid() string {
	listCount := ssd.listCount
	if listCount == 0 {
		listCount = 9
	}

	buf := &bytes.Buffer{}
	out := &text.GridWriter{ColumnPadding: 4}
	out.WriteCells("db", "total", "read", "write", time.Now().Format("2006-01-02T15:04:05Z07:00"))
	out.EndRow()

	//Sort by total time
	totals := make(sortableTotals, 0, len(ssd.Totals))
	for ns, diff := range ssd.Totals {
		lockStats := ssd.currentServerStatus.Locks[ns]
		currentTotal := lockStats.TimeLockedMicros.ReadLower + lockStats.TimeLockedMicros.WriteLower
		totals = append(totals, sortableTotal{ns, float64(diff.Read + diff.Write), currentTotal})
	}

	sort.Sort(sort.Reverse(totals))
	for i, st := range totals {
		diff := ssd.Totals[st.Name]
		out.WriteCells(st.Name,
			fmt.Sprintf("%vms", diff.Read+diff.Write),
			fmt.Sprintf("%vms", diff.Read),
			fmt.Sprintf("%vms", diff.Write),
			"")
		out.EndRow()
		if i >= listCount-1 {
			break
		}
	}

	out.Flush(buf)
	return buf.String()
}

// Diff takes an older ServerStatus sample, and produces a ServerStatusDiff
// representing the deltas of each metric between the two samples.
func (ss ServerStatus) Diff(previous ServerStatus, listCount int) ServerStatusDiff {
	// the diff to eventually return
	diff := ServerStatusDiff{
		currentServerStatus: &ss,
		listCount:           listCount,
		Totals:              map[string]LockDelta{},
		Time:                time.Now(),
	}

	prevLocks := previous.Locks
	curLocks := ss.Locks
	for ns, prevNSInfo := range prevLocks {
		if curNSInfo, ok := curLocks[ns]; ok {
			prevTimeLocked := prevNSInfo.TimeLockedMicros
			curTimeLocked := curNSInfo.TimeLockedMicros

			diff.Totals[ns] = LockDelta{
				Read: (curTimeLocked.Read + curTimeLocked.ReadLower -
					(prevTimeLocked.Read + prevTimeLocked.ReadLower)) / 1000,
				Write: (curTimeLocked.Write + curTimeLocked.WriteLower -
					(prevTimeLocked.Write + prevTimeLocked.WriteLower)) / 1000,
			}
		}
	}

	return diff
}
