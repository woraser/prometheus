// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tsdb

import (
	"context"
	"sync"
	"time"
	"unsafe"

	"github.com/alecthomas/units"
	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	tsdbLabels "github.com/prometheus/prometheus/tsdb/labels"
)

// ErrNotReady is returned if the underlying storage is not ready yet.
var ErrNotReady = errors.New("TSDB not ready")

// ReadyStorage implements the Storage interface while allowing to set the actual
// storage at a later point in time.
// ReadyStorage实现了Storage接口，同时允许在以后的某个时间点设置实际存储。
type ReadyStorage struct {
	mtx sync.RWMutex
	a   *adapter	//adapter 是tsdb对interface定义接口对实现
}

// Set the storage.
// 为ReadyStorage设置实际存储器(tsdb)
func (s *ReadyStorage) Set(db *tsdb.DB, startTimeMargin int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.a = &adapter{db: db, startTimeMargin: startTimeMargin}
}

// Get the storage.
// 获取实际存储器
func (s *ReadyStorage) Get() *tsdb.DB {
	if x := s.get(); x != nil {
		return x.db
	}
	return nil
}
// 获取存储适配器
func (s *ReadyStorage) get() *adapter {
	s.mtx.RLock()
	x := s.a
	s.mtx.RUnlock()
	return x
}

// StartTime implements the Storage interface.
func (s *ReadyStorage) StartTime() (int64, error) {
	if x := s.get(); x != nil {
		return x.StartTime()
	}
	return int64(model.Latest), ErrNotReady
}

// Querier implements the Storage interface.
func (s *ReadyStorage) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	if x := s.get(); x != nil {
		return x.Querier(ctx, mint, maxt)
	}
	return nil, ErrNotReady
}

// Appender implements the Storage interface.
func (s *ReadyStorage) Appender() (storage.Appender, error) {
	if x := s.get(); x != nil {
		return x.Appender()
	}
	return nil, ErrNotReady
}

// Close implements the Storage interface.
func (s *ReadyStorage) Close() error {
	if x := s.Get(); x != nil {
		return x.Close()
	}
	return nil
}

// Adapter return an adapter as storage.Storage.
// 适配器作为storage.Storage返回
func Adapter(db *tsdb.DB, startTimeMargin int64) storage.Storage {
	return &adapter{db: db, startTimeMargin: startTimeMargin}
}

// adapter implements a storage.Storage around TSDB.
// tsdb对适配器，大小写注意
type adapter struct {
	db              *tsdb.DB
	startTimeMargin int64	// 起始时间的冗余时间
}

// Options of the DB storage.
// db配置项，在此特指tsdb
type Options struct {
	// The timestamp range of head blocks after which they get persisted.
	// It's the minimum duration of any persisted block.
	// 数据在内存中保留时间， 默认是2h
	MinBlockDuration model.Duration

	// The maximum timestamp range of compacted blocks.
	// 数据在内存中保留时间 默认36h
	MaxBlockDuration model.Duration

	// The maximum size of each WAL segment file.
	// 每个日志追加文件的最大值
	WALSegmentSize units.Base2Bytes

	// Duration for how long to retain data.
	// 数据在存储块的生存时间 最大是100y
	RetentionDuration model.Duration

	// Maximum number of bytes to be retained.
	// 要保留的最大字节数。
	MaxBytes units.Base2Bytes

	// Disable creation and consideration of lockfile.
	// 是否禁用lockfile。
	NoLockfile bool

	// When true it disables the overlapping blocks check.
	// This in-turn enables vertical compaction and vertical query merge.
	// 如果为true，则禁用重叠块check.This反过来启用垂直压缩和垂直查询合并。
	AllowOverlappingBlocks bool

	// When true records in the WAL will be compressed.
	// wal文件是否需要压缩
	WALCompression bool
}

var (
	startTime   prometheus.GaugeFunc
	headMaxTime prometheus.GaugeFunc
	headMinTime prometheus.GaugeFunc
)
// 注册tsdb内置metric
func registerMetrics(db *tsdb.DB, r prometheus.Registerer) {

	startTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_lowest_timestamp_seconds",
		Help: "Lowest timestamp value stored in the database.",
	}, func() float64 {
		bb := db.Blocks()
		if len(bb) == 0 {
			return float64(db.Head().MinTime()) / 1000
		}
		return float64(db.Blocks()[0].Meta().MinTime) / 1000
	})
	headMinTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_min_time_seconds",
		Help: "Minimum time bound of the head block.",
	}, func() float64 {
		return float64(db.Head().MinTime()) / 1000
	})
	headMaxTime = prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "prometheus_tsdb_head_max_time_seconds",
		Help: "Maximum timestamp of the head block.",
	}, func() float64 {
		return float64(db.Head().MaxTime()) / 1000
	})

	if r != nil {
		r.MustRegister(
			startTime,
			headMaxTime,
			headMinTime,
		)
	}
}

// Open returns a new storage backed by a TSDB database that is configured for Prometheus.
// Open返回一个新对由为Prometheus配置的TSDB数据库支持的存储对象。
func Open(path string, l log.Logger, r prometheus.Registerer, opts *Options) (*tsdb.DB, error) {
	if opts.MinBlockDuration > opts.MaxBlockDuration {
		opts.MaxBlockDuration = opts.MinBlockDuration
	}
	// Start with smallest block duration and create exponential buckets until the exceed the
	// configured maximum block duration.
	// 以最小的块持续时间开始并创建存储块，直到超过配置的最大持续时间。
	// rngs 10个数据块的时间区间 time ranges
	// rngs = [1, 1*3, 1*3*3....]
	rngs := tsdb.ExponentialBlockRanges(int64(time.Duration(opts.MinBlockDuration).Seconds()*1000), 10, 3)

	for i, v := range rngs {
		// 遍历数据块，数据块的时间范围不能超过最大值
		if v > int64(time.Duration(opts.MaxBlockDuration).Seconds()*1000) {
			rngs = rngs[:i]
			break
		}
	}
	// 打开tsdb存储引擎
	db, err := tsdb.Open(path, l, r, &tsdb.Options{
		WALSegmentSize:         int(opts.WALSegmentSize),
		RetentionDuration:      uint64(time.Duration(opts.RetentionDuration).Seconds() * 1000),
		MaxBytes:               int64(opts.MaxBytes), // 数据的最大字节数
		BlockRanges:            rngs,	// 存储块大小范围[a, a*3, a*3*3...]
		NoLockfile:             opts.NoLockfile,
		AllowOverlappingBlocks: opts.AllowOverlappingBlocks,
		WALCompression:         opts.WALCompression,
	})
	if err != nil {
		return nil, err
	}
	registerMetrics(db, r)

	return db, nil
}

// StartTime implements the Storage interface.
// 存储器中最早对数据时间
func (a adapter) StartTime() (int64, error) {
	var startTime int64

	if len(a.db.Blocks()) > 0 {
		startTime = a.db.Blocks()[0].Meta().MinTime
	} else {
		startTime = time.Now().Unix() * 1000
	}

	// Add a safety margin as it may take a few minutes for everything to spin up.
	// 添加安全边距，因为可能需要几分钟才能完成所有操作。
	return startTime + a.startTimeMargin, nil
}

func (a adapter) Querier(_ context.Context, mint, maxt int64) (storage.Querier, error) {
	q, err := a.db.Querier(mint, maxt)
	if err != nil {
		return nil, err
	}
	return querier{q: q}, nil
}

// Appender returns a new appender against the storage.
func (a adapter) Appender() (storage.Appender, error) {
	return appender{a: a.db.Appender()}, nil
}

// Close closes the storage and all its underlying resources.
func (a adapter) Close() error {
	return a.db.Close()
}

type querier struct {
	q tsdb.Querier
}

func (q querier) Select(_ *storage.SelectParams, oms ...*labels.Matcher) (storage.SeriesSet, storage.Warnings, error) {
	ms := make([]tsdbLabels.Matcher, 0, len(oms))

	for _, om := range oms {
		ms = append(ms, convertMatcher(om))
	}
	set, err := q.q.Select(ms...)
	if err != nil {
		return nil, nil, err
	}
	return seriesSet{set: set}, nil, nil
}

func (q querier) LabelValues(name string) ([]string, storage.Warnings, error) {
	v, err := q.q.LabelValues(name)
	return v, nil, err
}
func (q querier) LabelNames() ([]string, storage.Warnings, error) {
	v, err := q.q.LabelNames()
	return v, nil, err
}
func (q querier) Close() error { return q.q.Close() }

type seriesSet struct {
	set tsdb.SeriesSet
}

func (s seriesSet) Next() bool         { return s.set.Next() }
func (s seriesSet) Err() error         { return s.set.Err() }
func (s seriesSet) At() storage.Series { return series{s: s.set.At()} }

type series struct {
	s tsdb.Series
}

func (s series) Labels() labels.Labels            { return toLabels(s.s.Labels()) }
func (s series) Iterator() storage.SeriesIterator { return storage.SeriesIterator(s.s.Iterator()) }

type appender struct {
	a tsdb.Appender
}

func (a appender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	ref, err := a.a.Add(toTSDBLabels(lset), t, v)

	switch errors.Cause(err) {
	case tsdb.ErrNotFound:
		return 0, storage.ErrNotFound
	case tsdb.ErrOutOfOrderSample:
		return 0, storage.ErrOutOfOrderSample
	case tsdb.ErrAmendSample:
		return 0, storage.ErrDuplicateSampleForTimestamp
	case tsdb.ErrOutOfBounds:
		return 0, storage.ErrOutOfBounds
	}
	return ref, err
}

func (a appender) AddFast(_ labels.Labels, ref uint64, t int64, v float64) error {
	err := a.a.AddFast(ref, t, v)

	switch errors.Cause(err) {
	case tsdb.ErrNotFound:
		return storage.ErrNotFound
	case tsdb.ErrOutOfOrderSample:
		return storage.ErrOutOfOrderSample
	case tsdb.ErrAmendSample:
		return storage.ErrDuplicateSampleForTimestamp
	case tsdb.ErrOutOfBounds:
		return storage.ErrOutOfBounds
	}
	return err
}

func (a appender) Commit() error   { return a.a.Commit() }
func (a appender) Rollback() error { return a.a.Rollback() }

func convertMatcher(m *labels.Matcher) tsdbLabels.Matcher {
	switch m.Type {
	case labels.MatchEqual:
		return tsdbLabels.NewEqualMatcher(m.Name, m.Value)

	case labels.MatchNotEqual:
		return tsdbLabels.Not(tsdbLabels.NewEqualMatcher(m.Name, m.Value))

	case labels.MatchRegexp:
		res, err := tsdbLabels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return res

	case labels.MatchNotRegexp:
		res, err := tsdbLabels.NewRegexpMatcher(m.Name, "^(?:"+m.Value+")$")
		if err != nil {
			panic(err)
		}
		return tsdbLabels.Not(res)
	}
	panic("storage.convertMatcher: invalid matcher type")
}

func toTSDBLabels(l labels.Labels) tsdbLabels.Labels {
	return *(*tsdbLabels.Labels)(unsafe.Pointer(&l))
}

func toLabels(l tsdbLabels.Labels) labels.Labels {
	return *(*labels.Labels)(unsafe.Pointer(&l))
}
