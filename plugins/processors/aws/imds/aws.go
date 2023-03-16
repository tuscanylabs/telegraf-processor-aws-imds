package aws

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/parallel"
	"github.com/influxdata/telegraf/plugins/processors"
	"github.com/patrickmn/go-cache"
)

//go:embed sample.conf
var sampleConfig string

type AwsIMDSProcessor struct {
	ImdsTags         []string        `toml:"imds_tags"`
	Timeout          config.Duration `toml:"timeout"`
	Ordered          bool            `toml:"ordered"`
	MaxParallelCalls int             `toml:"max_parallel_calls"`
	CacheTTL         int             `toml:"cache_ttl"`
	Log              telegraf.Logger `toml:"-"`
	imdsClient       *imds.Client
	imdsTagsMap      map[string]struct{}
	parallel         parallel.Parallel
	instanceID       string
	cache            *cache.Cache
	rwLock           sync.RWMutex
}

const (
	DefaultMaxOrderedQueueSize = 10_000
	DefaultMaxParallelCalls    = 10
	DefaultTimeout             = 10 * time.Second
	DefaultCacheTTL            = 24
)

var allowedImdsTags = map[string]struct{}{
	"accountId":        {},
	"architecture":     {},
	"availabilityZone": {},
	"billingProducts":  {},
	"imageId":          {},
	"instanceId":       {},
	"instanceType":     {},
	"kernelId":         {},
	"pendingTime":      {},
	"privateIp":        {},
	"ramdiskId":        {},
	"region":           {},
	"version":          {},
}

func (*AwsIMDSProcessor) SampleConfig() string {
	return sampleConfig
}

func (r *AwsIMDSProcessor) Add(metric telegraf.Metric, _ telegraf.Accumulator) error {
	r.parallel.Enqueue(metric)
	return nil
}

func (r *AwsIMDSProcessor) Init() error {
	r.Log.Debug("Initializing AWS IMDS Processor")

	for _, tag := range r.ImdsTags {
		if len(tag) == 0 || !isIMDSTagAllowed(tag) {
			return fmt.Errorf("not allowed metadata tag specified in configuration: %s", tag)
		}
		r.imdsTagsMap[tag] = struct{}{}
	}
	if len(r.imdsTagsMap) == 0 {
		return errors.New("no allowed metadata tags specified in configuration")
	}

	// Cache will prevent hammering of the IMDS url which can result in throttling and unnecessary HTTP traffic which
	// may be detected by instrumentation tools such as Pixie
	r.cache = cache.New(
		time.Duration(r.CacheTTL)*time.Hour,
		time.Duration(r.CacheTTL)*time.Hour,
	)

	return nil
}

func (r *AwsIMDSProcessor) Start(acc telegraf.Accumulator) error {
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("failed loading default AWS config: %w", err)
	}
	r.imdsClient = imds.NewFromConfig(cfg)

	iido, err := r.imdsClient.GetInstanceIdentityDocument(
		ctx,
		&imds.GetInstanceIdentityDocumentInput{},
	)
	if err != nil {
		return fmt.Errorf("failed getting instance identity document: %w", err)
	}

	r.instanceID = iido.InstanceID

	if r.Ordered {
		r.parallel = parallel.NewOrdered(acc, r.asyncAdd, DefaultMaxOrderedQueueSize, r.MaxParallelCalls)
	} else {
		r.parallel = parallel.NewUnordered(acc, r.asyncAdd, r.MaxParallelCalls)
	}

	return nil
}

func (r *AwsIMDSProcessor) Stop() {
	if r.parallel != nil {
		r.parallel.Stop()
	}
}

func (r *AwsIMDSProcessor) Lookup(tag string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(r.Timeout))
	defer cancel()

	// check if the value is cached
	r.rwLock.RLock()
	result, found := r.cache.Get(tag)
	if found {
		defer r.rwLock.RUnlock()
		// cache is valid
		return result.(string), nil
	}
	r.rwLock.RUnlock()

	r.Log.Infof("Cache miss for tag: %s", tag)

	r.rwLock.Lock()
	defer r.rwLock.Unlock()
	iido, err := r.imdsClient.GetInstanceIdentityDocument(
		ctx,
		&imds.GetInstanceIdentityDocumentInput{},
	)
	if err != nil {
		return "", err
	}
	v := getTagFromInstanceIdentityDocument(iido, tag)
	if v != "" {
		r.cache.Set(tag, v, cache.DefaultExpiration)
	}
	return v, nil
}

func (r *AwsIMDSProcessor) asyncAdd(metric telegraf.Metric) []telegraf.Metric {
	if len(r.imdsTagsMap) > 0 {
		for tag := range r.imdsTagsMap {
			result, err := r.Lookup(tag)
			if err != nil {
				r.Log.Errorf("Error when looking up: %v", err)
				continue
			}
			if result == "" {

			}
			metric.AddTag(tag, result)
		}
	}

	return []telegraf.Metric{metric}
}

func init() {
	processors.AddStreaming("aws_imds", func() telegraf.StreamingProcessor {
		return newAwsIMDSProcessor()
	})
}

func newAwsIMDSProcessor() *AwsIMDSProcessor {
	return &AwsIMDSProcessor{
		MaxParallelCalls: DefaultMaxParallelCalls,
		Timeout:          config.Duration(DefaultTimeout),
		imdsTagsMap:      make(map[string]struct{}),
		CacheTTL:         DefaultCacheTTL,
	}
}

func getTagFromInstanceIdentityDocument(o *imds.GetInstanceIdentityDocumentOutput, tag string) string {
	switch tag {
	case "accountId":
		return o.AccountID
	case "architecture":
		return o.Architecture
	case "availabilityZone":
		return o.AvailabilityZone
	case "billingProducts":
		return strings.Join(o.BillingProducts, ",")
	case "imageId":
		return o.ImageID
	case "instanceId":
		return o.InstanceID
	case "instanceType":
		return o.InstanceType
	case "kernelId":
		return o.KernelID
	case "pendingTime":
		return o.PendingTime.String()
	case "privateIp":
		return o.PrivateIP
	case "ramdiskId":
		return o.RamdiskID
	case "region":
		return o.Region
	case "version":
		return o.Version
	default:
		return ""
	}
}

func isIMDSTagAllowed(tag string) bool {
	_, ok := allowedImdsTags[tag]
	return ok
}
