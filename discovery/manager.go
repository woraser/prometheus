// Copyright 2016 The Prometheus Authors
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

package discovery

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	sd_config "github.com/prometheus/prometheus/discovery/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"

	"github.com/prometheus/prometheus/discovery/azure"
	"github.com/prometheus/prometheus/discovery/consul"
	"github.com/prometheus/prometheus/discovery/dns"
	"github.com/prometheus/prometheus/discovery/ec2"
	"github.com/prometheus/prometheus/discovery/file"
	"github.com/prometheus/prometheus/discovery/gce"
	"github.com/prometheus/prometheus/discovery/kubernetes"
	"github.com/prometheus/prometheus/discovery/marathon"
	"github.com/prometheus/prometheus/discovery/openstack"
	"github.com/prometheus/prometheus/discovery/triton"
	"github.com/prometheus/prometheus/discovery/zookeeper"
)

var (
	//无法加载的服务发现配置总数
	failedConfigs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_configs_failed_total",
			Help: "Total number of service discovery configurations that failed to load.",
		},
		[]string{"name"},
	)
	//当前已发现目标的数量。
	discoveredTargets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_discovered_targets",
			Help: "Current number of discovered targets.",
		},
		[]string{"name", "config"},
	)
	//从发现的服务中收到的更新事件总数
	receivedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_received_updates_total",
			Help: "Total number of update events received from the SD providers.",
		},
		[]string{"name"},
	)
	//无法立即发送的更新事件总数。
	delayedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_delayed_total",
			Help: "Total number of update events that couldn't be sent immediately.",
		},
		[]string{"name"},
	)
	//发送给SD消费者的更新事件总数
	sentUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_total",
			Help: "Total number of update events sent to the SD consumers.",
		},
		[]string{"name"},
	)
)

func init() {
	//监控注册
	prometheus.MustRegister(failedConfigs, discoveredTargets, receivedUpdates, delayedUpdates, sentUpdates)
}

// Discoverer provides information about target groups. It maintains a set
// of sources from which TargetGroups can originate. Whenever a discovery provider
// detects a potential change, it sends the TargetGroup through its channel.
//
// Discoverer does not know if an actual change happened.
// It does guarantee that it sends the new TargetGroup whenever a change happens.
//
// Discoverers should initially send a full set of all discoverable TargetGroups.
type Discoverer interface {
	// Run hands a channel to the discovery provider (Consul, DNS etc) through which it can send
	// updated target groups.
	// Must returns if the context gets canceled. It should not close the update
	// channel on returning.
	//Group是一组具有通用标签集的目标。
	//Group{Targets, Labels, Source(描述)}
	Run(ctx context.Context, up chan<- []*targetgroup.Group)
}

type poolKey struct {
	setName  string
	provider string
}

// provider holds a Discoverer instance, its configuration and its subscribers.
// 服务发现实例
type provider struct {
	name   string
	d      Discoverer
	subs   []string
	config interface{}
}

// NewManager is the Discovery Manager constructor.
// 服务发现管理构造器
func NewManager(ctx context.Context, logger log.Logger, options ...func(*Manager)) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	mgr := &Manager{
		logger:         logger,
		syncCh:         make(chan map[string][]*targetgroup.Group),
		targets:        make(map[poolKey]map[string]*targetgroup.Group),
		discoverCancel: []context.CancelFunc{},
		ctx:            ctx,
		updatert:       5 * time.Second,
		triggerSend:    make(chan struct{}, 1),
	}
	for _, option := range options {
		option(mgr)
	}
	return mgr
}

// Name sets the name of the manager.
func Name(n string) func(*Manager) {
	return func(m *Manager) {
		m.mtx.Lock()
		defer m.mtx.Unlock()
		m.name = n
	}
}

// Manager maintains a set of discovery providers and sends each update to a map channel.
// Targets are grouped by the target set name.
type Manager struct {
	logger         log.Logger
	name           string
	mtx            sync.RWMutex
	ctx            context.Context
	discoverCancel []context.CancelFunc

	// Some Discoverers(eg. k8s) send only the updates for a given target group
	// so we use map[tg.Source]*targetgroup.Group to know which group to update.
	// poolKey.name = Group.Source
	targets map[poolKey]map[string]*targetgroup.Group
	// providers keeps track of SD providers.追踪
	providers []*provider
	// The sync channel sends the updates as a map where the key is the job value from the scrape config.
	// 同步通道将更新作为映射发送，其中键是来自scrape配置的作业值。
	// scrape模块使用 在main的run()方法中调用
	syncCh chan map[string][]*targetgroup.Group

	// How long to wait before sending updates to the channel. The variable
	// should only be modified in unit tests.
	// 在向channel发送更新之前等待多长时间,用于限制channel更新频率。
	// 默认是5s,只能在测试用例中修改,不能通过配置文件修改。
	updatert time.Duration

	// The triggerSend channel signals to the manager that new updates have been received from providers.
	// 服务发现更新时触发通知 更新触发器
	triggerSend chan struct{}
}

// module 入口函数
// Run starts the background processing
// 启动后台运行服务,在/cmd/prometheus main.go文件中调用
func (m *Manager) Run() error {
	go m.sender()
	for range m.ctx.Done() {
		m.cancelDiscoverers()
		return m.ctx.Err()
	}
	return nil
}

// SyncCh returns a read only channel used by all the clients to receive target updates.
// 返回一个所有目标接受更新的通道
func (m *Manager) SyncCh() <-chan map[string][]*targetgroup.Group {
	return m.syncCh
}

// ApplyConfig removes all running discovery providers and starts new ones using the provided config.
// 应用配置文件，重置SD，删除所有正在运行的发现提供程序，并使用提供的配置启
func (m *Manager) ApplyConfig(cfg map[string]sd_config.ServiceDiscoveryConfig) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for pk := range m.targets {
		if _, ok := cfg[pk.setName]; !ok {
			discoveredTargets.DeleteLabelValues(m.name, pk.setName)
		}
	}
	// 清空discovery
	m.cancelDiscoverers()
	for name, scfg := range cfg {
		// 注册discovery的provider，例如consule，file，etcd....
		m.registerProviders(scfg, name)
		discoveredTargets.WithLabelValues(m.name, name).Set(0)
	}
	// 启动provider
	for _, prov := range m.providers {
		m.startProvider(m.ctx, prov)
	}

	return nil
}

// StartCustomProvider is used for sdtool. Only use this if you know what you're doing.
func (m *Manager) StartCustomProvider(ctx context.Context, name string, worker Discoverer) {
	p := &provider{
		name: name,
		d:    worker,
		subs: []string{name},
	}
	m.providers = append(m.providers, p)
	m.startProvider(ctx, p)
}
// 启动provider 每个provider都实现了共同的接口
func (m *Manager) startProvider(ctx context.Context, p *provider) {
	level.Debug(m.logger).Log("msg", "Starting provider", "provider", p.name, "subs", fmt.Sprintf("%v", p.subs))
	ctx, cancel := context.WithCancel(ctx)
	updates := make(chan []*targetgroup.Group)

	m.discoverCancel = append(m.discoverCancel, cancel)
	// run provider实现了discovery的run()接口
	go p.d.Run(ctx, updates)
	// 启动更新器
	go m.updater(ctx, p, updates)
}
// 启动更新器
func (m *Manager) updater(ctx context.Context, p *provider, updates chan []*targetgroup.Group) {
	for {
		select {
		case <-ctx.Done():
			return
		// updates不为空
		case tgs, ok := <-updates:
			// 接受到的更新次数+1
			receivedUpdates.WithLabelValues(m.name).Inc()
			if !ok {
				level.Debug(m.logger).Log("msg", "discoverer channel closed", "provider", p.name)
				return
			}

			for _, s := range p.subs {
				m.updateGroup(poolKey{setName: s, provider: p.name}, tgs)
			}

			select {
			// 触发更新，provider有新的更新内容
			case m.triggerSend <- struct{}{}:
			default:
			}
		}
	}
}

func (m *Manager) sender() {
	// 断续器 限制服务发现频率
	ticker := time.NewTicker(m.updatert)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C: // Some discoverers send updates too often so we throttle these with the ticker.
			select {
			case <-m.triggerSend:
				// 更新次数+1
				sentUpdates.WithLabelValues(m.name).Inc()
				select {
				case m.syncCh <- m.allGroups():
				default:
					// 未发生的更新次数+1
					delayedUpdates.WithLabelValues(m.name).Inc()
					level.Debug(m.logger).Log("msg", "discovery receiver's channel was full so will retry the next cycle")
					select {
					case m.triggerSend <- struct{}{}:
					default:
					}
				}
			default:
			}
		}
	}
}
// 关闭所有的发现服务 清空数据
func (m *Manager) cancelDiscoverers() {
	// 执行关闭函数
	for _, c := range m.discoverCancel {
		c()
	}
	// 清空数据
	m.targets = make(map[poolKey]map[string]*targetgroup.Group)
	m.providers = nil
	m.discoverCancel = nil
}
// 根据传入参数 更新m.targets
func (m *Manager) updateGroup(poolKey poolKey, tgs []*targetgroup.Group) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	for _, tg := range tgs {
		if tg != nil { // Some Discoverers send nil target group so need to check for it to avoid panics.
			if _, ok := m.targets[poolKey]; !ok {
				m.targets[poolKey] = make(map[string]*targetgroup.Group)
			}
			m.targets[poolKey][tg.Source] = tg
		}
	}
}
// 搜集m.targets，整合为targetgroup，返回新的tSets变量
func (m *Manager) allGroups() map[string][]*targetgroup.Group {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	tSets := map[string][]*targetgroup.Group{}
	// 遍历m.targets
	for pkey, tsets := range m.targets {
		var n int	// targets总数
		for _, tg := range tsets {
			// Even if the target group 'tg' is empty we still need to send it to the 'Scrape manager'
			// to signal that it needs to stop all scrape loops for this target set.
			// 即使目标组'tg'为空，我们仍然需要将它发送到'Scrape manager'，
			// 以表示它需要停止此目标集的所有scrape循环。
			tSets[pkey.setName] = append(tSets[pkey.setName], tg)
			n += len(tg.Targets)
		}
		// 记录targets总数
		discoveredTargets.WithLabelValues(m.name, pkey.setName).Set(float64(n))
	}
	return tSets
}

// 注册不同的服务发现组件 etcd，consule等
func (m *Manager) registerProviders(cfg sd_config.ServiceDiscoveryConfig, setName string) {
	var added bool
	add := func(cfg interface{}, newDiscoverer func() (Discoverer, error)) {
		t := reflect.TypeOf(cfg).String()
		for _, p := range m.providers {
			if reflect.DeepEqual(cfg, p.config) {
				p.subs = append(p.subs, setName)
				added = true
				return
			}
		}

		d, err := newDiscoverer()
		if err != nil {
			level.Error(m.logger).Log("msg", "Cannot create service discovery", "err", err, "type", t)
			failedConfigs.WithLabelValues(m.name).Inc()
			return
		}

		provider := provider{
			name:   fmt.Sprintf("%s/%d", t, len(m.providers)),
			d:      d,
			config: cfg,
			subs:   []string{setName},
		}
		m.providers = append(m.providers, &provider)
		added = true
	}

	for _, c := range cfg.DNSSDConfigs {
		add(c, func() (Discoverer, error) {
			return dns.NewDiscovery(*c, log.With(m.logger, "discovery", "dns")), nil
		})
	}
	for _, c := range cfg.FileSDConfigs {
		add(c, func() (Discoverer, error) {
			return file.NewDiscovery(c, log.With(m.logger, "discovery", "file")), nil
		})
	}
	for _, c := range cfg.ConsulSDConfigs {
		add(c, func() (Discoverer, error) {
			return consul.NewDiscovery(c, log.With(m.logger, "discovery", "consul"))
		})
	}
	for _, c := range cfg.MarathonSDConfigs {
		add(c, func() (Discoverer, error) {
			return marathon.NewDiscovery(*c, log.With(m.logger, "discovery", "marathon"))
		})
	}
	for _, c := range cfg.KubernetesSDConfigs {
		add(c, func() (Discoverer, error) {
			return kubernetes.New(log.With(m.logger, "discovery", "k8s"), c)
		})
	}
	for _, c := range cfg.ServersetSDConfigs {
		add(c, func() (Discoverer, error) {
			return zookeeper.NewServersetDiscovery(c, log.With(m.logger, "discovery", "zookeeper"))
		})
	}
	for _, c := range cfg.NerveSDConfigs {
		add(c, func() (Discoverer, error) {
			return zookeeper.NewNerveDiscovery(c, log.With(m.logger, "discovery", "nerve"))
		})
	}
	for _, c := range cfg.EC2SDConfigs {
		add(c, func() (Discoverer, error) {
			return ec2.NewDiscovery(c, log.With(m.logger, "discovery", "ec2")), nil
		})
	}
	for _, c := range cfg.OpenstackSDConfigs {
		add(c, func() (Discoverer, error) {
			return openstack.NewDiscovery(c, log.With(m.logger, "discovery", "openstack"))
		})
	}
	for _, c := range cfg.GCESDConfigs {
		add(c, func() (Discoverer, error) {
			return gce.NewDiscovery(*c, log.With(m.logger, "discovery", "gce"))
		})
	}
	for _, c := range cfg.AzureSDConfigs {
		add(c, func() (Discoverer, error) {
			return azure.NewDiscovery(c, log.With(m.logger, "discovery", "azure")), nil
		})
	}
	for _, c := range cfg.TritonSDConfigs {
		add(c, func() (Discoverer, error) {
			return triton.New(log.With(m.logger, "discovery", "triton"), c)
		})
	}
	if len(cfg.StaticConfigs) > 0 {
		add(setName, func() (Discoverer, error) {
			return &StaticProvider{TargetGroups: cfg.StaticConfigs}, nil
		})
	}
	if !added {
		// Add an empty target group to force the refresh of the corresponding
		// scrape pool and to notify the receiver that this target set has no
		// current targets.
		// It can happen because the combined set of SD configurations is empty
		// or because we fail to instantiate all the SD configurations.
		add(setName, func() (Discoverer, error) {
			return &StaticProvider{TargetGroups: []*targetgroup.Group{{}}}, nil
		})
	}
}

// StaticProvider holds a list of target groups that never change.
// 未发送改变的目标组
type StaticProvider struct {
	TargetGroups []*targetgroup.Group
}

// Run implements the Worker interface.
func (sd *StaticProvider) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// We still have to consider that the consumer exits right away in which case
	// the context will be canceled.
	select {
	case ch <- sd.TargetGroups:
	case <-ctx.Done():
	}
	close(ch)
}
