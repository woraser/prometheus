// Copyright 2013 The Prometheus Authors
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

package scrape

import (
	"fmt"
	"hash/fnv"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
)

// TargetHealth describes the health state of a target.
// 目标健康状态 HealthUnknown | HealthGood | HealthBad
type TargetHealth string

// The possible health states of a target based on the last performed scrape.
const (
	HealthUnknown TargetHealth = "unknown" // 未知
	HealthGood    TargetHealth = "up"	// 运行
	HealthBad     TargetHealth = "down"	//down机
)

// Target refers to a singular HTTP or HTTPS endpoint.
// Target指的是单个HTTP或HTTPS端点。
type Target struct {
	// Labels before any processing.
	discoveredLabels labels.Labels	//初始标签
	// Any labels that are added to this target and its metrics.
	labels labels.Labels	//自定义标签
	// Additional URL parameters that are part of the target URL.
	params url.Values	//url参数

	mtx                sync.RWMutex
	lastError          error
	lastScrape         time.Time	//上一次采集时间
	lastScrapeDuration time.Duration	//上一次采集的持续时间
	health             TargetHealth
	metadata           metricMetadataStore	//元数据
}

// NewTarget creates a reasonably configured target for querying.
func NewTarget(labels, discoveredLabels labels.Labels, params url.Values) *Target {
	return &Target{
		labels:           labels,
		discoveredLabels: discoveredLabels,
		params:           params,
		health:           HealthUnknown,
	}
}

func (t *Target) String() string {
	return t.URL().String()
}

type metricMetadataStore interface {
	listMetadata() []MetricMetadata
	getMetadata(metric string) (MetricMetadata, bool)
}

// MetricMetadata is a piece of metadata for a metric.
type MetricMetadata struct {
	Metric string
	Type   textparse.MetricType
	Help   string
	Unit   string
}

func (t *Target) MetadataList() []MetricMetadata {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.metadata == nil {
		return nil
	}
	return t.metadata.listMetadata()
}

// Metadata returns type and help metadata for the given metric.
func (t *Target) Metadata(metric string) (MetricMetadata, bool) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	if t.metadata == nil {
		return MetricMetadata{}, false
	}
	return t.metadata.getMetadata(metric)
}

func (t *Target) setMetadataStore(s metricMetadataStore) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.metadata = s
}

// hash returns an identifying hash for the target.
// 采集目标的hash值 作为唯一标识符用
func (t *Target) hash() uint64 {
	h := fnv.New64a()
	//nolint: errcheck
	h.Write([]byte(fmt.Sprintf("%016d", t.labels.Hash())))
	//nolint: errcheck
	h.Write([]byte(t.URL().String()))

	return h.Sum64()
}

// offset returns the time until the next scrape cycle for the target.
// It includes the global server jitterSeed for scrapes from multiple Prometheus to try to be at different times.
func (t *Target) offset(interval time.Duration, jitterSeed uint64) time.Duration {
	now := time.Now().UnixNano()

	// Base is a pinned to absolute time, no matter how often offset is called.
	var (
		base   = int64(interval) - now%int64(interval)
		offset = (t.hash() ^ jitterSeed) % uint64(interval)
		next   = base + int64(offset)
	)

	if next > int64(interval) {
		next -= int64(interval)
	}
	return time.Duration(next)
}

// Labels returns a copy of the set of all public labels of the target.
// 获得所有的labels
func (t *Target) Labels() labels.Labels {
	lset := make(labels.Labels, 0, len(t.labels))
	for _, l := range t.labels {
		if !strings.HasPrefix(l.Name, model.ReservedLabelPrefix) {
			lset = append(lset, l)
		}
	}
	return lset
}

// DiscoveredLabels returns a copy of the target's labels before any processing.
// 获得所有的初始化labels
func (t *Target) DiscoveredLabels() labels.Labels {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	lset := make(labels.Labels, len(t.discoveredLabels))
	copy(lset, t.discoveredLabels)
	return lset
}

// SetDiscoveredLabels sets new DiscoveredLabels
// 设置初始化labels
func (t *Target) SetDiscoveredLabels(l labels.Labels) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.discoveredLabels = l
}

// URL returns a copy of the target's URL.
// 获取目标url
func (t *Target) URL() *url.URL {
	params := url.Values{}

	for k, v := range t.params {
		params[k] = make([]string, len(v))
		copy(params[k], v)
	}
	for _, l := range t.labels {
		if !strings.HasPrefix(l.Name, model.ParamLabelPrefix) {
			continue
		}
		ks := l.Name[len(model.ParamLabelPrefix):]

		if len(params[ks]) > 0 {
			params[ks][0] = l.Value
		} else {
			params[ks] = []string{l.Value}
		}
	}

	return &url.URL{
		Scheme:   t.labels.Get(model.SchemeLabel),
		Host:     t.labels.Get(model.AddressLabel),
		Path:     t.labels.Get(model.MetricsPathLabel),
		RawQuery: params.Encode(),
	}
}

// 状态报告
func (t *Target) report(start time.Time, dur time.Duration, err error) {
	t.mtx.Lock()
	defer t.mtx.Unlock()

	if err == nil {
		t.health = HealthGood
	} else {
		t.health = HealthBad
	}

	t.lastError = err
	t.lastScrape = start
	t.lastScrapeDuration = dur
}

// LastError returns the error encountered during the last scrape.
// 获取上次采集中的错误
func (t *Target) LastError() error {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.lastError
}

// LastScrape returns the time of the last scrape.
// 获取上次采集时间
func (t *Target) LastScrape() time.Time {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.lastScrape
}

// LastScrapeDuration returns how long the last scrape of the target took.
// 获取上次的采集持续时间
func (t *Target) LastScrapeDuration() time.Duration {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.lastScrapeDuration
}

// Health returns the last known health state of the target.
// 获取健康状态
func (t *Target) Health() TargetHealth {
	t.mtx.RLock()
	defer t.mtx.RUnlock()

	return t.health
}

// Targets is a sortable list of targets.
// 可排序的目标集合
type Targets []*Target

func (ts Targets) Len() int           { return len(ts) }
func (ts Targets) Less(i, j int) bool { return ts[i].URL().String() < ts[j].URL().String() }
func (ts Targets) Swap(i, j int)      { ts[i], ts[j] = ts[j], ts[i] }

var errSampleLimit = errors.New("sample limit exceeded")

// limitAppender limits the number of total appended samples in a batch.
// 数量限制
type limitAppender struct {
	storage.Appender

	limit int
	i     int
}

// 追加起添加label数据
func (app *limitAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return 0, errSampleLimit
		}
	}
	ref, err := app.Appender.Add(lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

func (app *limitAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	if !value.IsStaleNaN(v) {
		app.i++
		if app.i > app.limit {
			return errSampleLimit
		}
	}
	err := app.Appender.AddFast(lset, ref, t, v)
	return err
}

// appender数据的时间限制
type timeLimitAppender struct {
	storage.Appender

	maxTime int64
}

func (app *timeLimitAppender) Add(lset labels.Labels, t int64, v float64) (uint64, error) {
	if t > app.maxTime {
		return 0, storage.ErrOutOfBounds
	}

	ref, err := app.Appender.Add(lset, t, v)
	if err != nil {
		return 0, err
	}
	return ref, nil
}

func (app *timeLimitAppender) AddFast(lset labels.Labels, ref uint64, t int64, v float64) error {
	if t > app.maxTime {
		return storage.ErrOutOfBounds
	}
	err := app.Appender.AddFast(lset, ref, t, v)
	return err
}

// populateLabels builds a label set from the given label set and scrape configuration.
// It returns a label set before relabeling was applied as the second return value.
// Returns the original discovered label set found before relabelling was applied if the target is dropped during relabeling.
// 根据配置文件填充lables
func populateLabels(lset labels.Labels, cfg *config.ScrapeConfig) (res, orig labels.Labels, err error) {
	// Copy labels into the labelset for the target if they are not set already.
	scrapeLabels := []labels.Label{
		{Name: model.JobLabel, Value: cfg.JobName},
		{Name: model.MetricsPathLabel, Value: cfg.MetricsPath},
		{Name: model.SchemeLabel, Value: cfg.Scheme},
	}
	// 合并元数据label和自定义label
	lb := labels.NewBuilder(lset)

	for _, l := range scrapeLabels {
		// 保证唯一性
		if lv := lset.Get(l.Name); lv == "" {
			lb.Set(l.Name, l.Value)
		}
	}
	// Encode scrape query parameters as labels.
	for k, v := range cfg.Params {
		if len(v) > 0 {
			lb.Set(model.ParamLabelPrefix+k, v[0])
		}
	}
	// 提取等待relabeling的labels
	preRelabelLabels := lb.Labels()
	// 根据配置文件进行relabel操作（对label进行一些基础操作，drop,keep,replace）
	// https://yunlzheng.gitbook.io/prometheus-book/part-ii-prometheus-jin-jie/sd/service-discovery-with-relabel
	lset = relabel.Process(preRelabelLabels, cfg.RelabelConfigs...)

	// Check if the target was dropped.
	// 如果target已被抛弃，直接返回未处理labels
	if lset == nil {
		return nil, preRelabelLabels, nil
	}
	// 如果没有AddressLabel，直接抛错
	if v := lset.Get(model.AddressLabel); v == "" {
		return nil, nil, errors.New("no address")
	}

	lb = labels.NewBuilder(lset)

	// addPort checks whether we should add a default port to the address.
	// If the address is not valid, we don't append a port either.
	addPort := func(s string) bool {
		// If we can split, a port exists and we don't have to add one.
		if _, _, err := net.SplitHostPort(s); err == nil {
			return false
		}
		// If adding a port makes it valid, the previous error
		// was not due to an invalid address and we can append a port.
		_, _, err := net.SplitHostPort(s + ":1234")
		return err == nil
	}
	addr := lset.Get(model.AddressLabel)
	// If it's an address with no trailing port, infer it based on the used scheme.
	if addPort(addr) {
		// Addresses reaching this point are already wrapped in [] if necessary.
		switch lset.Get(model.SchemeLabel) {
		case "http", "":
			addr = addr + ":80"
		case "https":
			addr = addr + ":443"
		default:
			return nil, nil, errors.Errorf("invalid scheme: %q", cfg.Scheme)
		}
		lb.Set(model.AddressLabel, addr)
	}

	if err := config.CheckTargetAddress(model.LabelValue(addr)); err != nil {
		return nil, nil, err
	}

	// Meta labels are deleted after relabelling. Other internal labels propagate to
	// the target which decides whether they will be part of their label set.
	// 删除元数据_meta_xxx
	for _, l := range lset {
		if strings.HasPrefix(l.Name, model.MetaLabelPrefix) {
			lb.Del(l.Name)
		}
	}

	// Default the instance label to the target address.
	// 追加实例label
	if v := lset.Get(model.InstanceLabel); v == "" {
		lb.Set(model.InstanceLabel, addr)
	}

	res = lb.Labels()
	// label校验（string is a valid UTF8？）
	for _, l := range res {
		// Check label values are valid, drop the target if not.
		if !model.LabelValue(l.Value).IsValid() {
			return nil, nil, errors.Errorf("invalid label value for %q: %q", l.Name, l.Value)
		}
	}
	return res, preRelabelLabels, nil
}

// targetsFromGroup builds targets based on the given TargetGroup and config.
// 从config和group中构建目标组
// 从discovery中获取targetgroup
func targetsFromGroup(tg *targetgroup.Group, cfg *config.ScrapeConfig) ([]*Target, error) {
	// targets数组
	targets := make([]*Target, 0, len(tg.Targets))

	for i, tlset := range tg.Targets {
		// 当前target的labels数组
		lbls := make([]labels.Label, 0, len(tlset)+len(tg.Labels))
		// 合并labels
		for ln, lv := range tlset {
			lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
		}
		for ln, lv := range tg.Labels {
			if _, ok := tlset[ln]; !ok {
				lbls = append(lbls, labels.Label{Name: string(ln), Value: string(lv)})
			}
		}
		// 构建label，lbls中的label必须唯一
		lset := labels.New(lbls...)

		lbls, origLabels, err := populateLabels(lset, cfg)
		if err != nil {
			return nil, errors.Wrapf(err, "instance %d in group %s", i, tg)
		}
		if lbls != nil || origLabels != nil {
			targets = append(targets, NewTarget(lbls, origLabels, cfg.Params))
		}
	}
	return targets, nil
}
