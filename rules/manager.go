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

package rules

import (
	"context"
	html_template "html/template"
	"math"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/rulefmt"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

// RuleHealth describes the health state of a rule.
// rule状态
type RuleHealth string

// The possible health states of a rule based on the last execution.
// 基于上次执行的规则的可能健康状态。
const (
	HealthUnknown RuleHealth = "unknown"
	HealthGood    RuleHealth = "ok"
	HealthBad     RuleHealth = "err"
)

// Constants for instrumentation.
// 默认的namespace
const namespace = "prometheus"

var (
	groupInterval = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, "", "rule_group_interval_seconds"),
		"The interval of a rule group.",
		[]string{"rule_group"},
		nil,
	)
)

// Metrics for rule evaluation.
// 每次规则执行需要的metrics
type Metrics struct {
	evalDuration        prometheus.Summary
	evalFailures        prometheus.Counter
	evalTotal           prometheus.Counter
	iterationDuration   prometheus.Summary
	iterationsMissed    prometheus.Counter
	iterationsScheduled prometheus.Counter
	groupLastEvalTime   *prometheus.GaugeVec
	groupLastDuration   *prometheus.GaugeVec
	groupRules          *prometheus.GaugeVec
}

// NewGroupMetrics makes a new Metrics and registers them with the provided registerer,
// if not nil.
// 注册rule的监控指标，用户模块的自我管理和监控
func NewGroupMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		evalDuration: prometheus.NewSummary(
			prometheus.SummaryOpts{
				Namespace:  namespace,
				Name:       "rule_evaluation_duration_seconds",
				Help:       "The duration for a rule to execute.",
				Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
			}),
		evalFailures: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rule_evaluation_failures_total",
				Help:      "The total number of rule evaluation failures.",
			}),
		evalTotal: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "rule_evaluations_total",
				Help:      "The total number of rule evaluations.",
			}),
		iterationDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace:  namespace,
			Name:       "rule_group_duration_seconds",
			Help:       "The duration of rule group evaluations.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		}),
		iterationsMissed: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rule_group_iterations_missed_total",
			Help:      "The total number of rule group evaluations missed due to slow rule group evaluation.",
		}),
		iterationsScheduled: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "rule_group_iterations_total",
			Help:      "The total number of scheduled rule group evaluations, whether executed or missed.",
		}),
		groupLastEvalTime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_last_evaluation_timestamp_seconds",
				Help:      "The timestamp of the last rule group evaluation in seconds.",
			},
			[]string{"rule_group"},
		),
		groupLastDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_last_duration_seconds",
				Help:      "The duration of the last rule group evaluation.",
			},
			[]string{"rule_group"},
		),
		groupRules: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "rule_group_rules",
				Help:      "The number of rules.",
			},
			[]string{"rule_group"},
		),
	}

	if reg != nil {
		reg.MustRegister(
			m.evalDuration,
			m.evalFailures,
			m.evalTotal,
			m.iterationDuration,
			m.iterationsMissed,
			m.iterationsScheduled,
			m.groupLastEvalTime,
			m.groupLastDuration,
			m.groupRules,
		)
	}

	return m
}

// QueryFunc processes PromQL queries.
// 查询方法
type QueryFunc func(ctx context.Context, q string, t time.Time) (promql.Vector, error)

// EngineQueryFunc returns a new query function that executes instant queries against
// the given engine.
// It converts scalar into vector results.
// 存储引擎查询方法 返回查询方法
func EngineQueryFunc(engine *promql.Engine, q storage.Queryable) QueryFunc {
	return func(ctx context.Context, qs string, t time.Time) (promql.Vector, error) {
		q, err := engine.NewInstantQuery(q, qs, t)
		if err != nil {
			return nil, err
		}
		res := q.Exec(ctx)
		if res.Err != nil {
			return nil, res.Err
		}
		switch v := res.Value.(type) {
		case promql.Vector:
			return v, nil
		case promql.Scalar:
			return promql.Vector{promql.Sample{
				Point:  promql.Point(v),
				Metric: labels.Labels{},
			}}, nil
		default:
			return nil, errors.New("rule result is not a vector or scalar")
		}
	}
}

// A Rule encapsulates a vector expression which is evaluated at a specified
// interval and acted upon (currently either recorded or used for alerting).
type Rule interface {
	// 名称
	Name() string
	// Labels of the rule.
	// rule的labels 需要新增或者覆盖的labels
	Labels() labels.Labels
	// eval evaluates the rule, including any associated recording or alerting actions.
	// 表达式
	Eval(context.Context, time.Time, QueryFunc, *url.URL) (promql.Vector, error)
	// String returns a human-readable string representation of the rule.
	String() string
	// SetLastErr sets the current error experienced by the rule.
	SetLastError(error)
	// LastErr returns the last error experienced by the rule.
	LastError() error
	// SetHealth sets the current health of the rule.
	SetHealth(RuleHealth)
	// Health returns the current health of the rule.
	// 获取rule的实时状态
	Health() RuleHealth
	// 设置函数的执行时间
	SetEvaluationDuration(time.Duration)
	// GetEvaluationDuration returns last evaluation duration.
	// NOTE: Used dynamically by rules.html template.
	// 获取函数的执行时间
	GetEvaluationDuration() time.Duration
	SetEvaluationTimestamp(time.Time)
	// GetEvaluationTimestamp returns last evaluation timestamp.
	// NOTE: Used dynamically by rules.html template.
	GetEvaluationTimestamp() time.Time
	// HTMLSnippet returns a human-readable string representation of the rule,
	// decorated with HTML elements for use the web frontend.
	HTMLSnippet(pathPrefix string) html_template.HTML
}

// Group is a set of rules that have a logical relation.
// 一组有逻辑关系的规则
type Group struct {
	name                 string	// group name 唯一
	file                 string	 //文件地址
	interval             time.Duration	// 运行间隔
	rules                []Rule	// 定义的规则列表
	seriesInPreviousEval []map[string]labels.Labels // One per Rule.
	staleSeries          []labels.Labels
	opts                 *ManagerOptions
	mtx                  sync.Mutex
	evaluationDuration   time.Duration
	evaluationTimestamp  time.Time

	shouldRestore bool

	extraAdditional bool
	// 信号量
	done       chan struct{}
	terminated chan struct{}

	logger log.Logger

	metrics *Metrics
}

// NewGroup makes a new Group with the given name, options, and rules.
func NewGroup(name, file string, interval time.Duration, rules []Rule, shouldRestore bool, opts *ManagerOptions) *Group {
	metrics := opts.Metrics
	if metrics == nil {
		metrics = NewGroupMetrics(opts.Registerer)
	}

	metrics.groupLastEvalTime.WithLabelValues(groupKey(file, name))
	metrics.groupLastDuration.WithLabelValues(groupKey(file, name))
	metrics.groupRules.WithLabelValues(groupKey(file, name)).Set(float64(len(rules)))

	return &Group{
		name:                 name,
		file:                 file,
		interval:             interval,
		rules:                rules,
		shouldRestore:        shouldRestore,
		opts:                 opts,
		seriesInPreviousEval: make([]map[string]labels.Labels, len(rules)),
		done:                 make(chan struct{}),
		terminated:           make(chan struct{}),
		logger:               log.With(opts.Logger, "group", name),
		metrics:              metrics,
	}
}

// Name returns the group name.
func (g *Group) Name() string { return g.name }

// File returns the group's file.
func (g *Group) File() string { return g.file }

// Rules returns the group's rules.
func (g *Group) Rules() []Rule { return g.rules }

// Interval returns the group's interval.
func (g *Group) Interval() time.Duration { return g.interval }
// group 运行函数
func (g *Group) run(ctx context.Context) {
	defer close(g.terminated)

	// Wait an initial amount to have consistently slotted intervals.
	// 延迟开始时间
	evalTimestamp := g.evalTimestamp().Add(g.interval)
	select {
	case <-time.After(time.Until(evalTimestamp)):
	case <-g.done:
		return
	}
	// rule执行函数
	iter := func() {
		// 迭代次数+1
		g.metrics.iterationsScheduled.Inc()

		start := time.Now()
		// 执行group中的规则
		g.Eval(ctx, evalTimestamp)
		// 耗时
		timeSinceStart := time.Since(start)
		// 设置启动时间等参数
		g.metrics.iterationDuration.Observe(timeSinceStart.Seconds())
		g.setEvaluationDuration(timeSinceStart)
		g.setEvaluationTimestamp(start)
	}

	// The assumption here is that since the ticker was started after having
	// waited for `evalTimestamp` to pass, the ticks will trigger soon
	// after each `evalTimestamp + N * g.interval` occurrence.
	// 断续器
	tick := time.NewTicker(g.interval)
	defer tick.Stop()

	iter()
	if g.shouldRestore {
		// If we have to restore, we wait for another Eval to finish.
		// The reason behind this is, during first eval (or before it)
		// we might not have enough data scraped, and recording rules would not
		// have updated the latest values, on which some alerts might depend.
		select {
		case <-g.done:
			return
		case <-tick.C:
			missed := (time.Since(evalTimestamp) / g.interval) - 1
			if missed > 0 {
				g.metrics.iterationsMissed.Add(float64(missed))
				g.metrics.iterationsScheduled.Add(float64(missed))
			}
			evalTimestamp = evalTimestamp.Add((missed + 1) * g.interval)
			iter()
		}

		g.RestoreForState(time.Now())
		g.shouldRestore = false
	}

	for {
		select {
		case <-g.done:
			return
		default:
			select {
			case <-g.done:
				return
			case <-tick.C:
				missed := (time.Since(evalTimestamp) / g.interval) - 1
				if missed > 0 {
					g.metrics.iterationsMissed.Add(float64(missed))
					g.metrics.iterationsScheduled.Add(float64(missed))
				}
				evalTimestamp = evalTimestamp.Add((missed + 1) * g.interval)
				iter()
			}
		}
	}
}

func (g *Group) stop() {
	close(g.done)
	<-g.terminated
}


func (g *Group) Stop() {
	g.stop()
}



func (g *Group) hash() uint64 {
	l := labels.New(
		labels.Label{Name: "name", Value: g.name},
		labels.Label{Name: "file", Value: g.file},
	)
	return l.Hash()
}

// AlertingRules returns the list of the group's alerting rules.
// 获取group的告警规则
func (g *Group) AlertingRules() []*AlertingRule {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	var alerts []*AlertingRule
	for _, rule := range g.rules {
		if alertingRule, ok := rule.(*AlertingRule); ok {
			alerts = append(alerts, alertingRule)
		}
	}
	sort.Slice(alerts, func(i, j int) bool {
		return alerts[i].State() > alerts[j].State() ||
			(alerts[i].State() == alerts[j].State() &&
				alerts[i].Name() < alerts[j].Name())
	})
	return alerts
}

// HasAlertingRules returns true if the group contains at least one AlertingRule.
// 判断group是否有告警规则
func (g *Group) HasAlertingRules() bool {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	for _, rule := range g.rules {
		if _, ok := rule.(*AlertingRule); ok {
			return true
		}
	}
	return false
}

// GetEvaluationDuration returns the time in seconds it took to evaluate the rule group.
func (g *Group) GetEvaluationDuration() time.Duration {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.evaluationDuration
}

// setEvaluationDuration sets the time in seconds the last evaluation took.
func (g *Group) setEvaluationDuration(dur time.Duration) {
	g.metrics.groupLastDuration.WithLabelValues(groupKey(g.file, g.name)).Set(dur.Seconds())

	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.evaluationDuration = dur
}

// GetEvaluationTimestamp returns the time the last evaluation of the rule group took place.
func (g *Group) GetEvaluationTimestamp() time.Time {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return g.evaluationTimestamp
}

// setEvaluationTimestamp updates evaluationTimestamp to the timestamp of when the rule group was last evaluated.
func (g *Group) setEvaluationTimestamp(ts time.Time) {
	g.metrics.groupLastEvalTime.WithLabelValues(groupKey(g.file, g.name)).Set(float64(ts.UnixNano()) / 1e9)

	g.mtx.Lock()
	defer g.mtx.Unlock()
	g.evaluationTimestamp = ts
}

// evalTimestamp returns the immediately preceding consistently slotted evaluation time.
func (g *Group) evalTimestamp() time.Time {
	var (
		offset = int64(g.hash() % uint64(g.interval))
		now    = time.Now().UnixNano()
		adjNow = now - offset
		base   = adjNow - (adjNow % int64(g.interval))
	)

	return time.Unix(0, base+offset)
}

func nameAndLabels(rule Rule) string {
	return rule.Name() + rule.Labels().String()
}

// CopyState copies the alerting rule and staleness related state from the given group.
//
// Rules are matched based on their name and labels. If there are duplicates, the
// first is matched with the first, second with the second etc.
func (g *Group) CopyState(from *Group) {
	g.evaluationDuration = from.evaluationDuration

	ruleMap := make(map[string][]int, len(from.rules))

	for fi, fromRule := range from.rules {
		nameAndLabels := nameAndLabels(fromRule)
		l := ruleMap[nameAndLabels]
		ruleMap[nameAndLabels] = append(l, fi)
	}

	for i, rule := range g.rules {
		nameAndLabels := nameAndLabels(rule)
		indexes := ruleMap[nameAndLabels]
		if len(indexes) == 0 {
			continue
		}
		fi := indexes[0]
		g.seriesInPreviousEval[i] = from.seriesInPreviousEval[fi]
		ruleMap[nameAndLabels] = indexes[1:]

		ar, ok := rule.(*AlertingRule)
		if !ok {
			continue
		}
		far, ok := from.rules[fi].(*AlertingRule)
		if !ok {
			continue
		}

		for fp, a := range far.active {
			ar.active[fp] = a
		}
	}

	// Handle deleted and unmatched duplicate rules.
	g.staleSeries = from.staleSeries
	for fi, fromRule := range from.rules {
		nameAndLabels := nameAndLabels(fromRule)
		l := ruleMap[nameAndLabels]
		if len(l) != 0 {
			for _, series := range from.seriesInPreviousEval[fi] {
				g.staleSeries = append(g.staleSeries, series)
			}
		}
	}
}

// Eval runs a single evaluation cycle in which all rules are evaluated sequentially.
// 执行group中rules的一个执行周期
func (g *Group) Eval(ctx context.Context, ts time.Time) {
	// 依次处理每个rule
	for i, rule := range g.rules {
		select {
		case <-g.done:
			return
		default:
		}
		// 采集函数
		func(i int, rule Rule) {
			sp, ctx := opentracing.StartSpanFromContext(ctx, "rule")
			sp.SetTag("name", rule.Name())
			defer func(t time.Time) {
				sp.Finish()
				// 任务完成之后记录时间
				since := time.Since(t)
				g.metrics.evalDuration.Observe(since.Seconds())
				rule.SetEvaluationDuration(since)
				rule.SetEvaluationTimestamp(t)
			}(time.Now())
			// 执行次数+1
			g.metrics.evalTotal.Inc()
			// rule metric执行结果
			// alert rule对应 alerting.go 中的Eval()函数
			// vector：promql表达式的查询结果集和
			vector, err := rule.Eval(ctx, ts, g.opts.QueryFunc, g.opts.ExternalURL)
			if err != nil {
				// 出错返回
				// Canceled queries are intentional termination of queries. This normally
				// happens on shutdown and thus we skip logging of any errors here.
				if _, ok := err.(promql.ErrQueryCanceled); !ok {
					level.Warn(g.logger).Log("msg", "Evaluating rule failed", "rule", rule, "err", err)
				}
				g.metrics.evalFailures.Inc()
				return
			}
			// 进行类型强转，如果是告警规则，发送告警信息，
			if ar, ok := rule.(*AlertingRule); ok {
				// 发送告警规则给配置的alertmanager
				ar.sendAlerts(ctx, ts, g.opts.ResendDelay, g.interval, g.opts.NotifyFunc)
			}
			var (
				// 乱序数量
				numOutOfOrder = 0
				// 重复数量
				numDuplicates = 0
			)
			// 获得写入管理器/缓冲器 tsdb remoteStorage
			app, err := g.opts.Appendable.Appender()
			if err != nil {
				level.Warn(g.logger).Log("msg", "creating appender failed", "err", err)
				return
			}
			// 成功写入的数据集合
			seriesReturned := make(map[string]labels.Labels, len(g.seriesInPreviousEval[i]))
			// 处理查询结果集合
			for _, s := range vector {
				// 存储写入器中添加查询结果
				if _, err := app.Add(s.Metric, s.T, s.V); err != nil {
					// handle error
					switch err {
					case storage.ErrOutOfOrderSample:
						numOutOfOrder++
						level.Debug(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
					case storage.ErrDuplicateSampleForTimestamp:
						numDuplicates++
						level.Debug(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
					default:
						level.Warn(g.logger).Log("msg", "Rule evaluation result discarded", "err", err, "sample", s)
					}
				} else {
					// 记录成功写入的数据metric
					// 没报错就代表写入成功
					seriesReturned[s.Metric.String()] = s.Metric
				}
			}
			if numOutOfOrder > 0 {
				level.Warn(g.logger).Log("msg", "Error on ingesting out-of-order result from rule evaluation", "numDropped", numOutOfOrder)
			}
			if numDuplicates > 0 {
				level.Warn(g.logger).Log("msg", "Error on ingesting results from rule evaluation with different value but same timestamp", "numDropped", numDuplicates)
			}
			// 处理之前的series,
			for metric, lset := range g.seriesInPreviousEval[i] {
				if _, ok := seriesReturned[metric]; !ok {
					// Series no longer exposed, mark it stale.
					// 使不用的series过期
					_, err = app.Add(lset, timestamp.FromTime(ts), math.Float64frombits(value.StaleNaN))
					switch err {
					case nil:
					case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
						// Do not count these in logging, as this is expected if series
						// is exposed from a different rule.
					default:
						level.Warn(g.logger).Log("msg", "adding stale sample failed", "sample", metric, "err", err)
					}
				}
			}
			// 数据持久化
			if err := app.Commit(); err != nil {
				level.Warn(g.logger).Log("msg", "rule sample appending failed", "err", err)
			} else {
				g.seriesInPreviousEval[i] = seriesReturned
			}
		}(i, rule)
	}
	// 处理失效series
	if len(g.staleSeries) != 0 {
		app, err := g.opts.Appendable.Appender()
		if err != nil {
			level.Warn(g.logger).Log("msg", "creating appender failed", "err", err)
			return
		}
		// series失效
		for _, s := range g.staleSeries {
			// Rule that produced series no longer configured, mark it stale.
			// 若rule没有配置，使之失效
			_, err = app.Add(s, timestamp.FromTime(ts), math.Float64frombits(value.StaleNaN))
			switch err {
			case nil:
			case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
				// Do not count these in logging, as this is expected if series
				// is exposed from a different rule.
			default:
				level.Warn(g.logger).Log("msg", "adding stale sample for previous configuration failed", "sample", s, "err", err)
			}
		}
		if err := app.Commit(); err != nil {
			level.Warn(g.logger).Log("msg", "stale sample appending for previous configuration failed", "err", err)
		} else {
			g.staleSeries = nil
		}
	}

}

// RestoreForState restores the 'for' state of the alerts
// by looking up last ActiveAt from storage.
// TODO 恢复for状态？
func (g *Group) RestoreForState(ts time.Time) {
	maxtMS := int64(model.TimeFromUnixNano(ts.UnixNano()))
	// We allow restoration only if alerts were active before after certain time.
	mint := ts.Add(-g.opts.OutageTolerance)
	mintMS := int64(model.TimeFromUnixNano(mint.UnixNano()))
	q, err := g.opts.TSDB.Querier(g.opts.Context, mintMS, maxtMS)
	if err != nil {
		level.Error(g.logger).Log("msg", "Failed to get Querier", "err", err)
		return
	}
	defer func() {
		if err := q.Close(); err != nil {
			level.Error(g.logger).Log("msg", "Failed to close Querier", "err", err)
		}
	}()

	for _, rule := range g.Rules() {
		alertRule, ok := rule.(*AlertingRule)
		if !ok {
			continue
		}

		alertHoldDuration := alertRule.HoldDuration()
		if alertHoldDuration < g.opts.ForGracePeriod {
			// If alertHoldDuration is already less than grace period, we would not
			// like to make it wait for `g.opts.ForGracePeriod` time before firing.
			// Hence we skip restoration, which will make it wait for alertHoldDuration.
			alertRule.SetRestored(true)
			continue
		}

		alertRule.ForEachActiveAlert(func(a *Alert) {
			smpl := alertRule.forStateSample(a, time.Now(), 0)
			var matchers []*labels.Matcher
			for _, l := range smpl.Metric {
				mt, err := labels.NewMatcher(labels.MatchEqual, l.Name, l.Value)
				if err != nil {
					panic(err)
				}
				matchers = append(matchers, mt)
			}

			sset, err, _ := q.Select(nil, matchers...)
			if err != nil {
				level.Error(g.logger).Log("msg", "Failed to restore 'for' state",
					labels.AlertName, alertRule.Name(), "stage", "Select", "err", err)
				return
			}

			seriesFound := false
			var s storage.Series
			for sset.Next() {
				// Query assures that smpl.Metric is included in sset.At().Labels(),
				// hence just checking the length would act like equality.
				// (This is faster than calling labels.Compare again as we already have some info).
				if len(sset.At().Labels()) == len(smpl.Metric) {
					s = sset.At()
					seriesFound = true
					break
				}
			}

			if !seriesFound {
				return
			}

			// Series found for the 'for' state.
			var t int64
			var v float64
			it := s.Iterator()
			for it.Next() {
				t, v = it.At()
			}
			if it.Err() != nil {
				level.Error(g.logger).Log("msg", "Failed to restore 'for' state",
					labels.AlertName, alertRule.Name(), "stage", "Iterator", "err", it.Err())
				return
			}
			if value.IsStaleNaN(v) { // Alert was not active.
				return
			}

			downAt := time.Unix(t/1000, 0)
			restoredActiveAt := time.Unix(int64(v), 0)
			timeSpentPending := downAt.Sub(restoredActiveAt)
			timeRemainingPending := alertHoldDuration - timeSpentPending

			if timeRemainingPending <= 0 {
				// It means that alert was firing when prometheus went down.
				// In the next Eval, the state of this alert will be set back to
				// firing again if it's still firing in that Eval.
				// Nothing to be done in this case.
			} else if timeRemainingPending < g.opts.ForGracePeriod {
				// (new) restoredActiveAt = (ts + m.opts.ForGracePeriod) - alertHoldDuration
				//                            /* new firing time */      /* moving back by hold duration */
				//
				// Proof of correctness:
				// firingTime = restoredActiveAt.Add(alertHoldDuration)
				//            = ts + m.opts.ForGracePeriod - alertHoldDuration + alertHoldDuration
				//            = ts + m.opts.ForGracePeriod
				//
				// Time remaining to fire = firingTime.Sub(ts)
				//                        = (ts + m.opts.ForGracePeriod) - ts
				//                        = m.opts.ForGracePeriod
				restoredActiveAt = ts.Add(g.opts.ForGracePeriod).Add(-alertHoldDuration)
			} else {
				// By shifting ActiveAt to the future (ActiveAt + some_duration),
				// the total pending time from the original ActiveAt
				// would be `alertHoldDuration + some_duration`.
				// Here, some_duration = downDuration.
				downDuration := ts.Sub(downAt)
				restoredActiveAt = restoredActiveAt.Add(downDuration)
			}

			a.ActiveAt = restoredActiveAt
			level.Debug(g.logger).Log("msg", "'for' state restored",
				labels.AlertName, alertRule.Name(), "restored_time", a.ActiveAt.Format(time.RFC850),
				"labels", a.Labels.String())

		})

		alertRule.SetRestored(true)
	}

}

// The Manager manages recording and alerting rules.
// rule管理器
type Manager struct {
	opts     *ManagerOptions
	groups   map[string]*Group
	mtx      sync.RWMutex
	block    chan struct{}
	restored bool

	logger log.Logger
}

// Appendable returns an Appender.
// 对存储器appender的上层封装接口
type Appendable interface {
	Appender() (storage.Appender, error)
}

// NotifyFunc sends notifications about a set of alerts generated by the given expression.
// 发送告警通知
type NotifyFunc func(ctx context.Context, expr string, alerts ...*Alert)

// ManagerOptions bundles options for the Manager.
type ManagerOptions struct {
	ExternalURL     *url.URL
	QueryFunc       QueryFunc
	NotifyFunc      NotifyFunc
	Context         context.Context
	Appendable      Appendable
	TSDB            storage.Storage
	Logger          log.Logger
	Registerer      prometheus.Registerer
	OutageTolerance time.Duration
	ForGracePeriod  time.Duration
	ResendDelay     time.Duration

	Metrics *Metrics
}

// NewManager returns an implementation of Manager, ready to be started
// by calling the Run method.
func NewManager(o *ManagerOptions) *Manager {
	if o.Metrics == nil {
		o.Metrics = NewGroupMetrics(o.Registerer)
	}

	m := &Manager{
		groups: map[string]*Group{},
		opts:   o,
		block:  make(chan struct{}),
		logger: o.Logger,
	}

	if o.Registerer != nil {
		o.Registerer.MustRegister(m)
	}

	o.Metrics.iterationsMissed.Inc()
	return m
}

// Run starts processing of the rule manager.
//关闭阻塞channel 以启动
func (m *Manager) Run() {
	close(m.block)
}

// Stop the rule manager's rule evaluation cycles.
// 关闭manager
func (m *Manager) Stop() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	level.Info(m.logger).Log("msg", "Stopping rule manager...")

	for _, eg := range m.groups {
		eg.stop()
	}

	level.Info(m.logger).Log("msg", "Rule manager stopped")
}

// Update the rule manager's state as the config requires. If
// loading the new rules failed the old rule set is restored.
// 根据配置文件 更新rule manager，若更新失败，则回滚旧配置
func (m *Manager) Update(interval time.Duration, files []string, externalLabels labels.Labels) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	// 加载配置文件中的groups(group name 唯一)
	groups, errs := m.LoadGroups(interval, externalLabels, files...)
	if errs != nil {
		for _, e := range errs {
			level.Error(m.logger).Log("msg", "loading groups failed", "err", e)
		}
		return errors.New("error loading rules, previous rule set restored")
	}
	m.restored = true

	var wg sync.WaitGroup
	// 处理group
	for _, newg := range groups {
		wg.Add(1)

		// If there is an old group with the same identifier, stop it and wait for
		// it to finish the current iteration. Then copy it into the new group.
		// 若存在相同的group 等待其暂停 并且替换为新的group
		gn := groupKey(newg.name, newg.file)
		oldg, ok := m.groups[gn]
		// 删除old group
		delete(m.groups, gn)

		go func(newg *Group) {
			if ok {
				oldg.stop()
				newg.CopyState(oldg)
			}
			go func() {
				// Wait with starting evaluation until the rule manager
				// is told to run. This is necessary to avoid running
				// queries against a bootstrapping storage.
				<-m.block
				// 运行 group主函数 周期性执行rule运算
				newg.run(m.opts.Context)
			}()
			wg.Done()
		}(newg)
	}

	// Stop remaining old groups.
	for _, oldg := range m.groups {
		// 关闭过期的group run()函数
		oldg.stop()
	}

	wg.Wait()
	m.groups = groups

	return nil
}

// LoadGroups reads groups from a list of files.
// 从文件中加载规则组 rule group
func (m *Manager) LoadGroups(
	interval time.Duration, externalLabels labels.Labels, filenames ...string,
) (map[string]*Group, []error) {
	groups := make(map[string]*Group)

	shouldRestore := !m.restored

	for _, fn := range filenames {
		rgs, errs := rulefmt.ParseFile(fn)
		if errs != nil {
			return nil, errs
		}

		for _, rg := range rgs.Groups {
			itv := interval
			if rg.Interval != 0 {
				itv = time.Duration(rg.Interval)
			}

			rules := make([]Rule, 0, len(rg.Rules))
			for _, r := range rg.Rules {
				expr, err := promql.ParseExpr(r.Expr)
				if err != nil {
					return nil, []error{errors.Wrap(err, fn)}
				}

				if r.Alert != "" {
					rules = append(rules, NewAlertingRule(
						r.Alert,
						expr,
						time.Duration(r.For),
						labels.FromMap(r.Labels),
						labels.FromMap(r.Annotations),
						externalLabels,
						m.restored,
						log.With(m.logger, "alert", r.Alert),
					))
					continue
				}
				rules = append(rules, NewRecordingRule(
					r.Record,
					expr,
					labels.FromMap(r.Labels),
				))
			}

			groups[groupKey(rg.Name, fn)] = NewGroup(rg.Name, fn, itv, rules, shouldRestore, m.opts)
		}
	}

	return groups, nil
}


// Add rule by api
func (m *Manager) BuildGroup(
	interval time.Duration, externalLabels labels.Labels, rg *rulefmt.RuleGroup) (*Group, error) {
	shouldRestore := !m.restored
	fn := "extra_" + rg.Name

	itv := interval
	if rg.Interval != 0 {
		itv = time.Duration(rg.Interval)
	}

	rules := make([]Rule, 0, len(rg.Rules))
	for _, r := range rg.Rules {
		expr, err := promql.ParseExpr(r.Expr)
		if err != nil {
			return nil, errors.Wrap(err, fn)
		}

		if r.Alert != "" {
			rules = append(rules, NewAlertingRule(
				r.Alert,
				expr,
				time.Duration(r.For),
				labels.FromMap(r.Labels),
				labels.FromMap(r.Annotations),
				externalLabels,
				m.restored,
				log.With(m.logger, "alert", r.Alert),
			))
			continue
		}
		rules = append(rules, NewRecordingRule(
			r.Record,
			expr,
			labels.FromMap(r.Labels),
		))
	}
	nGroup := NewGroup(rg.Name, fn, itv, rules, shouldRestore, m.opts)
	// declare is added by api
	nGroup.extraAdditional = true
	return nGroup, nil
}

// Add rule by api
func (m *Manager) AddExtraRule(
	interval time.Duration, externalLabels labels.Labels, rg *rulefmt.RuleGroup) error {
	fn := "extra_" + rg.Name
	nGroup, err:= m.BuildGroup(interval, externalLabels, rg)
	if err != nil {
		return err
	}

	nGroup.RunNow(m.opts.Context)
	m.groups[groupKey(rg.Name, fn)] = nGroup
 	return nil
}

func (g *Group) RunNow(c context.Context) {
	go func(newg *Group, nc context.Context) {
		newg.run(nc)
	}(g, c)
}

// Group names need not be unique across filenames.
// 获取group的uid
func groupKey(name, file string) string {
	return name + ";" + file
}

func (m Manager) GroupKey(name, file string) string {
	return groupKey(name, file)
}

// RuleGroups returns the list of manager's rule groups.
// 获取manager的group list
func (m *Manager) RuleGroups() []*Group {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	rgs := make([]*Group, 0, len(m.groups))
	for _, g := range m.groups {
		rgs = append(rgs, g)
	}

	sort.Slice(rgs, func(i, j int) bool {
		if rgs[i].file != rgs[j].file {
			return rgs[i].file < rgs[j].file
		}
		return rgs[i].name < rgs[j].name
	})

	return rgs
}

func (m *Manager) GroupsList() map[string]*Group {
	return m.groups
}

// Rules returns the list of the manager's rules.
func (m *Manager) Rules() []Rule {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	var rules []Rule
	for _, g := range m.groups {
		rules = append(rules, g.rules...)
	}

	return rules
}

// AlertingRules returns the list of the manager's alerting rules.
// 获取告警记录
func (m *Manager) AlertingRules() []*AlertingRule {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	alerts := []*AlertingRule{}
	for _, rule := range m.Rules() {
		if alertingRule, ok := rule.(*AlertingRule); ok {
			alerts = append(alerts, alertingRule)
		}
	}

	return alerts
}

// Describe implements prometheus.Collector.
func (m *Manager) Describe(ch chan<- *prometheus.Desc) {
	ch <- groupInterval
}

// Collect implements prometheus.Collector.
func (m *Manager) Collect(ch chan<- prometheus.Metric) {
	for _, g := range m.RuleGroups() {
		ch <- prometheus.MustNewConstMetric(groupInterval,
			prometheus.GaugeValue,
			g.interval.Seconds(),
			groupKey(g.file, g.name))
	}
}
