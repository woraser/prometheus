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

package storage

import (
	"container/heap"
	"context"
	"sort"
	"strings"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
)

// fanout storage存储的代理抽象层，屏蔽底层local storage和remote storage细节，samples向下双写，合并读取。
// Remote Storage创建了一个Queue管理器，基于负载轮流发送，读取客户端merge来自远端的数据。
// Local Storage基于本地磁盘的轻量级时序数据库
type fanout struct {
	logger log.Logger

	primary     Storage	// 主
	secondaries []Storage	// 二级
}

// NewFanout returns a new fan-out Storage, which proxies reads and writes
// through to multiple underlying storages.
// 新的存储抽象
func NewFanout(logger log.Logger, primary Storage, secondaries ...Storage) Storage {
	return &fanout{
		logger:      logger,
		primary:     primary,
		secondaries: secondaries,	// 次级存储器，可能有多个
	}
}

// StartTime implements the Storage interface.
// 获取fanout中最早的时间戳(比较primary和secondaries获得)，fanout的记录开始时间
func (f *fanout) StartTime() (int64, error) {
	// StartTime of a fanout should be the earliest StartTime of all its storages,
	// both primary and secondaries.
	firstTime, err := f.primary.StartTime()
	if err != nil {
		return int64(model.Latest), err
	}

	for _, storage := range f.secondaries {
		t, err := storage.StartTime()
		if err != nil {
			return int64(model.Latest), err
		}
		if t < firstTime {
			firstTime = t
		}
	}
	return firstTime, nil
}
// 查询接口
// 同时查询primary和secondary，返回查询函数集合
func (f *fanout) Querier(ctx context.Context, mint, maxt int64) (Querier, error) {
	queriers := make([]Querier, 0, 1+len(f.secondaries))

	// Add primary querier
	// primary查询
	primaryQuerier, err := f.primary.Querier(ctx, mint, maxt)
	if err != nil {
		return nil, err
	}
	queriers = append(queriers, primaryQuerier)

	// Add secondary queriers
	// secondary查询
	for _, storage := range f.secondaries {
		querier, err := storage.Querier(ctx, mint, maxt)
		if err != nil {
			NewMergeQuerier(primaryQuerier, queriers).Close()
			return nil, err
		}
		queriers = append(queriers, querier)
	}
	// 返回查询器组合
	return NewMergeQuerier(primaryQuerier, queriers), nil
}
// 获取fanout的appender集合
func (f *fanout) Appender() (Appender, error) {
	primary, err := f.primary.Appender()
	if err != nil {
		return nil, err
	}

	secondaries := make([]Appender, 0, len(f.secondaries))
	for _, storage := range f.secondaries {
		appender, err := storage.Appender()
		if err != nil {
			return nil, err
		}
		secondaries = append(secondaries, appender)
	}
	return &fanoutAppender{
		logger:      f.logger,
		primary:     primary,
		secondaries: secondaries,
	}, nil
}

// Close closes the storage and all its underlying resources.
// 关闭存储和关联的资源
func (f *fanout) Close() error {
	if err := f.primary.Close(); err != nil {
		return err
	}

	// TODO return multiple errors?
	// 只返回最后一个错误，有问题无法正确的定位错误
	var lastErr error
	for _, storage := range f.secondaries {
		if err := storage.Close(); err != nil {
			level.Error(f.logger).Log("msg", "Secondary storage Close error", "err", err)
			lastErr = err
		}
	}
	return lastErr
}

// fanoutAppender implements Appender.
// fanout写入器，一个合并写入器
type fanoutAppender struct {
	logger log.Logger

	primary     Appender
	secondaries []Appender
}
// 以下是fanoutAppender对appender接口对实现类,add,addFast,commit,Rollback
func (f *fanoutAppender) Add(l labels.Labels, t int64, v float64) (uint64, error) {
	ref, err := f.primary.Add(l, t, v)
	if err != nil {
		return ref, err
	}

	for _, appender := range f.secondaries {
		if _, err := appender.Add(l, t, v); err != nil {
			return 0, err
		}
	}
	return ref, nil
}

func (f *fanoutAppender) AddFast(l labels.Labels, ref uint64, t int64, v float64) error {
	if err := f.primary.AddFast(l, ref, t, v); err != nil {
		return err
	}

	for _, appender := range f.secondaries {
		if _, err := appender.Add(l, t, v); err != nil {
			return err
		}
	}
	return nil
}

func (f *fanoutAppender) Commit() (err error) {
	err = f.primary.Commit()

	for _, appender := range f.secondaries {
		if err == nil {
			err = appender.Commit()
		} else {
			if rollbackErr := appender.Rollback(); rollbackErr != nil {
				level.Error(f.logger).Log("msg", "Squashed rollback error on commit", "err", rollbackErr)
			}
		}
	}
	return
}

func (f *fanoutAppender) Rollback() (err error) {
	err = f.primary.Rollback()

	for _, appender := range f.secondaries {
		rollbackErr := appender.Rollback()
		if err == nil {
			err = rollbackErr
		} else if rollbackErr != nil {
			level.Error(f.logger).Log("msg", "Squashed rollback error on rollback", "err", rollbackErr)
		}
	}
	return nil
}

// mergeQuerier implements Querier.
// 合并的查询器
type mergeQuerier struct {
	primaryQuerier Querier
	queriers       []Querier

	failedQueriers map[Querier]struct{}
	setQuerierMap  map[SeriesSet]Querier
}

// NewMergeQuerier returns a new Querier that merges results of input queriers.
// NB NewMergeQuerier will return NoopQuerier if no queriers are passed to it,
// and will filter NoopQueriers from its arguments, in order to reduce overhead
// when only one querier is passed.
// NewMergeQuerier返回一个新的查询器，用于合并输入的查询结果。
// 如果没有向其传递任何查询器，则NewMergeQuerier将返回NoopQuerier，
// 并将从其参数中过滤NoopQueriers，以便在仅传递一个查询器时减少开销。
func NewMergeQuerier(primaryQuerier Querier, queriers []Querier) Querier {
	filtered := make([]Querier, 0, len(queriers))
	// 过滤queriers中的空查询器
	for _, querier := range queriers {
		// NoopQuerier 空查询器
		// 判断是否是空查询器
		if querier != NoopQuerier() {
			filtered = append(filtered, querier)
		}
	}

	setQuerierMap := make(map[SeriesSet]Querier)
	failedQueriers := make(map[Querier]struct{})
	// 根据过滤结果返回
	switch len(filtered) {
	case 0:
		return NoopQuerier()
	case 1:
		return filtered[0]
	default:
		return &mergeQuerier{
			primaryQuerier: primaryQuerier,
			queriers:       filtered,
			failedQueriers: failedQueriers,
			setQuerierMap:  setQuerierMap,
		}
	}
}

// Select returns a set of series that matches the given label matchers.
// 返回符合指定label的数据集合
// Core function
func (q *mergeQuerier) Select(params *SelectParams, matchers ...*labels.Matcher) (SeriesSet, Warnings, error) {
	seriesSets := make([]SeriesSet, 0, len(q.queriers))
	var warnings Warnings
	for _, querier := range q.queriers {
		// 遍历每一个查询器执行查询操作
		set, wrn, err := querier.Select(params, matchers...)
		// {query set:querier}
		q.setQuerierMap[set] = querier
		if wrn != nil {
			// 错误合并
			warnings = append(warnings, wrn...)
		}
		if err != nil {
			// 记录失败的querier
			q.failedQueriers[querier] = struct{}{}
			// If the error source isn't the primary querier, return the error as a warning and continue.
			if querier != q.primaryQuerier {
				warnings = append(warnings, err)
				continue
			} else {
				return nil, nil, err
			}
		}
		// 结果集合并
		seriesSets = append(seriesSets, set)
	}
	return NewMergeSeriesSet(seriesSets, q), warnings, nil
}

// LabelValues returns all potential values for a label name.
// 返回指定label的数据
func (q *mergeQuerier) LabelValues(name string) ([]string, Warnings, error) {
	var results [][]string
	var warnings Warnings
	for _, querier := range q.queriers {
		// 获取查询结果
		values, wrn, err := querier.LabelValues(name)

		if wrn != nil {
			warnings = append(warnings, wrn...)
		}
		if err != nil {
			q.failedQueriers[querier] = struct{}{}
			// If the error source isn't the primary querier, return the error as a warning and continue.
			if querier != q.primaryQuerier {
				warnings = append(warnings, err)
				continue
			} else {
				return nil, nil, err
			}
		}
		results = append(results, values)
	}
	return mergeStringSlices(results), warnings, nil
}
// 判断是否是失败的查询器
func (q *mergeQuerier) IsFailedSet(set SeriesSet) bool {
	_, isFailedQuerier := q.failedQueriers[q.setQuerierMap[set]]
	return isFailedQuerier
}
// 合并数组
func mergeStringSlices(ss [][]string) []string {
	switch len(ss) {
	case 0:
		return nil
	case 1:
		return ss[0]
	case 2:
		return mergeTwoStringSlices(ss[0], ss[1])
	default:
		halfway := len(ss) / 2
		return mergeTwoStringSlices(
			mergeStringSlices(ss[:halfway]),
			mergeStringSlices(ss[halfway:]),
		)
	}
}
// 合并两个slices
func mergeTwoStringSlices(a, b []string) []string {
	i, j := 0, 0
	result := make([]string, 0, len(a)+len(b))
	for i < len(a) && j < len(b) {
		switch strings.Compare(a[i], b[j]) {
		case 0:
			result = append(result, a[i])
			i++
			j++
		case -1:
			result = append(result, a[i])
			i++
		case 1:
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// LabelNames returns all the unique label names present in the block in sorted order.
// 返回所有唯一label
func (q *mergeQuerier) LabelNames() ([]string, Warnings, error) {
	labelNamesMap := make(map[string]struct{})
	var warnings Warnings
	for _, b := range q.queriers {
		names, wrn, err := b.LabelNames()
		if wrn != nil {
			warnings = append(warnings, wrn...)
		}

		if err != nil {
			// If the error source isn't the primary querier, return the error as a warning and continue.
			if b != q.primaryQuerier {
				warnings = append(warnings, err)
				continue
			} else {
				return nil, nil, errors.Wrap(err, "LabelNames() from Querier")
			}
		}

		for _, name := range names {
			labelNamesMap[name] = struct{}{}
		}
	}

	labelNames := make([]string, 0, len(labelNamesMap))
	for name := range labelNamesMap {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	return labelNames, warnings, nil
}

// Close releases the resources of the Querier.
func (q *mergeQuerier) Close() error {
	// TODO return multiple errors?
	var lastErr error
	for _, querier := range q.queriers {
		if err := querier.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// mergeSeriesSet implements SeriesSet
// 合并的数据集合
type mergeSeriesSet struct {
	currentLabels labels.Labels
	currentSets   []SeriesSet
	heap          seriesSetHeap
	sets          []SeriesSet

	querier *mergeQuerier
}

// NewMergeSeriesSet returns a new series set that merges (deduplicates)
// series returned by the input series sets when iterating.
func NewMergeSeriesSet(sets []SeriesSet, querier *mergeQuerier) SeriesSet {
	if len(sets) == 1 {
		return sets[0]
	}

	// Sets need to be pre-advanced, so we can introspect the label of the
	// series under the cursor.
	var h seriesSetHeap
	for _, set := range sets {
		// 判断set是否有数据，将有效的set存入heap中
		if set == nil {
			continue
		}
		// set有多个数据
		if set.Next() {
			heap.Push(&h, set)
		}
	}
	return &mergeSeriesSet{
		heap:    h,
		sets:    sets,
		querier: querier,
	}
}
//	循环迭代
func (c *mergeSeriesSet) Next() bool {
	// Run in a loop because the "next" series sets may not be valid anymore.
	// If a remote querier fails, we discard all series sets from that querier.
	// If, for the current label set, all the next series sets come from
	// failed remote storage sources, we want to keep trying with the next label set.
	// 当远程查询器失败时，我们把这个查询器中当所有series都丢弃。
	// 如果，对于当前标签集，所有都series都来自失败的远程存储源，我们想继续尝试下一个标签集。
	for {
		// Firstly advance all the current series sets.  If any of them have run out
		// we can drop them, otherwise they should be inserted back into the heap.
		// 首先推进所有当前的系列集。 如果他们中的任何一个用完了
		// 我们可以删除它们，否则它们应该插回到堆中。
		for _, set := range c.currentSets {
			// 如果当前series有下一个，将下一个值存入heap中
			if set.Next() {
				// heap 堆
				heap.Push(&c.heap, set)
			}
		}
		// len(c.heap) == 0 代表当前已经是最后一个，无法迭代了
		if len(c.heap) == 0 {
			return false
		}

		// Now, pop items of the heap that have equal label sets.
		c.currentSets = nil
		c.currentLabels = c.heap[0].At().Labels()
		// 把heap中非失败set全部存入当前set中
		for len(c.heap) > 0 && labels.Equal(c.currentLabels, c.heap[0].At().Labels()) {
			set := heap.Pop(&c.heap).(SeriesSet)
			if c.querier != nil && c.querier.IsFailedSet(set) {
				continue
			}
			// 将no failed的set转存到当前sets中
			c.currentSets = append(c.currentSets, set)
		}

		// As long as the current set contains at least 1 set,
		// then it should return true.
		// 只要当前set集合包含一个set，就返回true
		// 如果=0，代表没有no failed的set，无法进行下次迭代，直接推出循环
		if len(c.currentSets) != 0 {
			break
		}
	}
	return true
}
// 返回当前的时序键值对，点位点
func (c *mergeSeriesSet) At() Series {
	if len(c.currentSets) == 1 {
		return c.currentSets[0].At()
	}
	series := []Series{}
	for _, seriesSet := range c.currentSets {
		series = append(series, seriesSet.At())
	}
	return &mergeSeries{
		labels: c.currentLabels,
		series: series,
	}
}

func (c *mergeSeriesSet) Err() error {
	for _, set := range c.sets {
		if err := set.Err(); err != nil {
			return err
		}
	}
	return nil
}
// seriesSetHeap 有序集合，可排序
type seriesSetHeap []SeriesSet

func (h seriesSetHeap) Len() int      { return len(h) }
func (h seriesSetHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesSetHeap) Less(i, j int) bool {
	a, b := h[i].At().Labels(), h[j].At().Labels()
	return labels.Compare(a, b) < 0
}

func (h *seriesSetHeap) Push(x interface{}) {
	*h = append(*h, x.(SeriesSet))
}

func (h *seriesSetHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
// 对键值对结果集
type mergeSeries struct {
	labels labels.Labels
	series []Series
}

func (m *mergeSeries) Labels() labels.Labels {
	return m.labels
}

func (m *mergeSeries) Iterator() SeriesIterator {
	iterators := make([]SeriesIterator, 0, len(m.series))
	// Series to iterators
	for _, s := range m.series {
		iterators = append(iterators, s.Iterator())
	}
	return newMergeIterator(iterators)
}

type mergeIterator struct {
	iterators []SeriesIterator
	h         seriesIteratorHeap
}

func newMergeIterator(iterators []SeriesIterator) SeriesIterator {
	return &mergeIterator{
		iterators: iterators,
		h:         nil,
	}
}

func (c *mergeIterator) Seek(t int64) bool {
	c.h = seriesIteratorHeap{}
	for _, iter := range c.iterators {
		if iter.Seek(t) {
			heap.Push(&c.h, iter)
		}
	}
	return len(c.h) > 0
}
// 返回键值对
func (c *mergeIterator) At() (t int64, v float64) {
	if len(c.h) == 0 {
		panic("mergeIterator.At() called after .Next() returned false.")
	}

	return c.h[0].At()
}
// 迭代
func (c *mergeIterator) Next() bool {
	if c.h == nil {
		for _, iter := range c.iterators {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}

		return len(c.h) > 0
	}

	if len(c.h) == 0 {
		return false
	}

	currt, _ := c.At()
	for len(c.h) > 0 {
		nextt, _ := c.h[0].At()
		if nextt != currt {
			break
		}

		iter := heap.Pop(&c.h).(SeriesIterator)
		if iter.Next() {
			heap.Push(&c.h, iter)
		}
	}

	return len(c.h) > 0
}

func (c *mergeIterator) Err() error {
	for _, iter := range c.iterators {
		if err := iter.Err(); err != nil {
			return err
		}
	}
	return nil
}
// series迭代用heap
type seriesIteratorHeap []SeriesIterator

func (h seriesIteratorHeap) Len() int      { return len(h) }
func (h seriesIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h seriesIteratorHeap) Less(i, j int) bool {
	at, _ := h[i].At()
	bt, _ := h[j].At()
	return at < bt
}

func (h *seriesIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(SeriesIterator))
}

func (h *seriesIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
