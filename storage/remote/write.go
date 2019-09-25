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

package remote

import (
	"crypto/md5"
	"encoding/json"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

var (
	samplesIn = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Subsystem: subsystem,
		Name:      "samples_in_total",
		Help:      "Samples in to remote storage, compare to samples out for queue managers.",
	})
	highestTimestamp = maxGauge{
		Gauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "highest_timestamp_in_seconds",
			Help:      "Highest timestamp that has come into the remote storage via the Appender interface, in seconds since epoch.",
		}),
	}
)

// WriteStorage represents all the remote write storage.
// 远程存储帮助类
type WriteStorage struct {
	logger log.Logger
	mtx    sync.Mutex

	configHash        [16]byte
	externalLabelHash [16]byte
	walDir            string
	queues            []*QueueManager
	hashes            [][16]byte
	samplesIn         *ewmaRate
	flushDeadline     time.Duration
}

// NewWriteStorage creates and runs a WriteStorage.
func NewWriteStorage(logger log.Logger, walDir string, flushDeadline time.Duration) *WriteStorage {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	rws := &WriteStorage{
		logger:        logger,
		flushDeadline: flushDeadline,
		samplesIn:     newEWMARate(ewmaWeight, shardUpdateDuration),
		walDir:        walDir,
	}
	// 定时修改速率
	go rws.run()
	return rws
}

func (rws *WriteStorage) run() {
	ticker := time.NewTicker(shardUpdateDuration)
	defer ticker.Stop()
	for range ticker.C {
		rws.samplesIn.tick()
	}
}

// ApplyConfig updates the state as the new config requires.
// Only stop & create queues which have changes.
// 应用配置文件，启动或者停止写入队列
func (rws *WriteStorage) ApplyConfig(conf *config.Config) error {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()

	// Remote write queues only need to change if the remote write config or
	// external labels change. Hash these together and only reload if the hash
	// changes.
	// Remote write只会在配置文件或者label修改的时候发生变化，用hash值来做判断。
	cfgBytes, err := json.Marshal(conf.RemoteWriteConfigs)
	if err != nil {
		return err
	}
	externalLabelBytes, err := json.Marshal(conf.GlobalConfig.ExternalLabels)
	if err != nil {
		return err
	}

	configHash := md5.Sum(cfgBytes)
	externalLabelHash := md5.Sum(externalLabelBytes)
	externalLabelUnchanged := externalLabelHash == rws.externalLabelHash
	// 没发生变化直接返回
	if configHash == rws.configHash && externalLabelUnchanged {
		level.Debug(rws.logger).Log("msg", "remote write config has not changed, no need to restart QueueManagers")
		return nil
	}

	rws.configHash = configHash
	rws.externalLabelHash = externalLabelHash

	// Update write queues
	// 创建新的存储队列管理器
	newQueues := []*QueueManager{}
	newHashes := [][16]byte{}
	newClientIndexes := []int{}
	// 每个远程存储都要单独存储
	for i, rwConf := range conf.RemoteWriteConfigs {
		b, err := json.Marshal(rwConf)
		if err != nil {
			return err
		}

		// Use RemoteWriteConfigs and its index to get hash. So if its index changed,
		// the correspoinding queue should also be restarted.
		// 判断配置文件信息是否修改，若修改，需要重启队列
		hash := md5.Sum(b)
		if i < len(rws.queues) && rws.hashes[i] == hash && externalLabelUnchanged {
			// The RemoteWriteConfig and index both not changed, keep the queue.
			newQueues = append(newQueues, rws.queues[i])
			newHashes = append(newHashes, hash)
			rws.queues[i] = nil
			continue
		}
		// Otherwise create a new queue.
		// 创建一个队列
		c, err := NewClient(i, &ClientConfig{
			URL:              rwConf.URL,
			Timeout:          rwConf.RemoteTimeout,
			HTTPClientConfig: rwConf.HTTPClientConfig,
		})
		if err != nil {
			return err
		}
		newQueues = append(newQueues, NewQueueManager(
			rws.logger,
			rws.walDir, // wal文件地址,写入日志地址
			rws.samplesIn,
			rwConf.QueueConfig,	// 队列配置文件
			conf.GlobalConfig.ExternalLabels,
			rwConf.WriteRelabelConfigs,
			c,
			rws.flushDeadline, // 刷新时间
		))
		newHashes = append(newHashes, hash)
		newClientIndexes = append(newClientIndexes, i)
	}

	for _, q := range rws.queues {
		// A nil queue means that queue has been reused.
		// 空queue代表这个queue被重用
		if q != nil {
			q.Stop()
		}
	}

	for _, index := range newClientIndexes {
		// 启动新的队列
		newQueues[index].Start()
	}

	rws.queues = newQueues
	rws.hashes = newHashes

	return nil
}

// Appender implements storage.Storage.
func (rws *WriteStorage) Appender() (storage.Appender, error) {
	return &timestampTracker{
		writeStorage: rws,
	}, nil
}

// Close closes the WriteStorage.
func (rws *WriteStorage) Close() error {
	rws.mtx.Lock()
	defer rws.mtx.Unlock()
	for _, q := range rws.queues {
		q.Stop()
	}
	return nil
}

type timestampTracker struct {
	writeStorage     *WriteStorage
	samples          int64
	highestTimestamp int64
}

// Add implements storage.Appender.
func (t *timestampTracker) Add(_ labels.Labels, ts int64, v float64) (uint64, error) {
	t.samples++
	if ts > t.highestTimestamp {
		t.highestTimestamp = ts
	}
	return 0, nil
}

// AddFast implements storage.Appender.
func (t *timestampTracker) AddFast(l labels.Labels, _ uint64, ts int64, v float64) error {
	_, err := t.Add(l, ts, v)
	return err
}

// Commit implements storage.Appender.
func (t *timestampTracker) Commit() error {
	t.writeStorage.samplesIn.incr(t.samples)

	samplesIn.Add(float64(t.samples))
	highestTimestamp.Set(float64(t.highestTimestamp / 1000))
	return nil
}

// Rollback implements storage.Appender.
func (*timestampTracker) Rollback() error {
	return nil
}
