// Copyright 2014 The Prometheus Authors
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

package storage

import (
	"context"
	"errors"

	"github.com/prometheus/prometheus/pkg/labels"
)

// The errors exposed.
var (
	ErrNotFound                    = errors.New("not found")
	ErrOutOfOrderSample            = errors.New("out of order sample")
	ErrDuplicateSampleForTimestamp = errors.New("duplicate sample for timestamp")
	ErrOutOfBounds                 = errors.New("out of bounds")
)

// Storage ingests and manages samples, along with various indexes. All methods
// are goroutine-safe. Storage implements storage.SampleAppender.
// 存储接口，所有方法都是协程安全
type Storage interface {
	Queryable

	// StartTime returns the oldest timestamp stored in the storage.
	// 存储中最早的时间戳
	StartTime() (int64, error)

	// Appender returns a new appender against the storage.
	// 批量追加器
	Appender() (Appender, error)

	// Close closes the storage and all its underlying resources.
	Close() error
}

// A Queryable handles queries against a storage.
// 存储的查询方法
type Queryable interface {
	// Querier returns a new Querier on the storage.
	Querier(ctx context.Context, mint, maxt int64) (Querier, error)
}

// Querier provides reading access to time series data.
// 查询器提供对时间序列数据的读取访问。
type Querier interface {
	// Select returns a set of series that matches the given label matchers.
	// 返回符合查询要求的数据
	Select(*SelectParams, ...*labels.Matcher) (SeriesSet, Warnings, error)

	// LabelValues returns all potential values for a label name.
	LabelValues(name string) ([]string, Warnings, error)

	// LabelNames returns all the unique label names present in the block in sorted order.
	LabelNames() ([]string, Warnings, error)

	// Close releases the resources of the Querier.
	Close() error
}

// SelectParams specifies parameters passed to data selections.
// 查询参数结构体
type SelectParams struct {
	Start int64 // Start time in milliseconds for this select.
	End   int64 // End time in milliseconds for this select.

	Step int64  // Query step size in milliseconds.
	Func string // String representation of surrounding function or aggregation.
}

// QueryableFunc is an adapter to allow the use of ordinary functions as
// Queryables. It follows the idea of http.HandlerFunc.
// QueryableFunc是一个允许使用普通函数asQueryables的适配器。 它遵循http.HandlerFunc规则
type QueryableFunc func(ctx context.Context, mint, maxt int64) (Querier, error)

// Querier calls f() with the given parameters.
// 根据指定参数执行查询
func (f QueryableFunc) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	return f(ctx, mint, maxt)
}

// Appender provides batched appends against a storage.
// 批量写入器，用于实现批量写入到存储器中
type Appender interface {
	Add(l labels.Labels, t int64, v float64) (uint64, error)

	AddFast(l labels.Labels, ref uint64, t int64, v float64) error

	// Commit submits the collected samples and purges the batch.
	Commit() error

	Rollback() error
}

// SeriesSet contains a set of series.
// 序列集合
type SeriesSet interface {
	Next() bool
	At() Series
	Err() error
}

// Series represents a single time series.
// 单个时间序列
type Series interface {
	// Labels returns the complete set of labels identifying the series.
	Labels() labels.Labels

	// Iterator returns a new iterator of the data of the series.
	// 返回数据集
	Iterator() SeriesIterator
}

// SeriesIterator iterates over the data of a time series.
// SeriesIterator迭代时间序列的数据
type SeriesIterator interface {
	// Seek advances the iterator forward to the value at or after
	// the given timestamp.
	// 给定时间戳进行定位
	Seek(t int64) bool
	// At returns the current timestamp/value pair.
	// 当前的键值对
	At() (t int64, v float64)
	// Next advances the iterator by one.
	// 迭代器迭代到下一个
	Next() bool
	// Err returns the current error.
	// 返回当前到错误信息
	Err() error
}

type Warnings []error
