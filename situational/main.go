package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcorex"
	"github.com/couchbase/gocbcorex/contrib/dinoctl"
	"github.com/couchbaselabs/cbdinocluster/clusterdef"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

var dinoIdFlag = flag.String("dinoid", "", "an existing dino-cluster id to use")
var newClusterFlag = flag.Bool("new-dino", false, "whether to create a new dino-cluster to use")
var verboseFlag = flag.Bool("v", false, "whether to use verbose logging")

func main() {
	ctx := context.Background()

	flag.Parse()

	var logger *zap.Logger
	if !*verboseFlag {
		logger, _ = zap.NewDevelopment(
			zap.IncreaseLevel(zapcore.InfoLevel))
	} else {
		logger, _ = zap.NewDevelopment()
	}

	var dinoId string
	var createCluster bool
	if *dinoIdFlag != "" && *newClusterFlag {
		logger.Fatal("cannot specify both dinoid and new-dino flags")
		return
	} else if *dinoIdFlag != "" {
		dinoId = *dinoIdFlag
	} else if *newClusterFlag {
		dinoId = ""
		createCluster = true
	} else {
		logger.Fatal("must specify either dinoid or new-dino flag")
		return
	}

	logger.Debug("starting",
		zap.String("dinoId", dinoId))

	dino := dinoctl.DinoCtl{
		Logger:    logger.Named("dinoctl"),
		LogOutput: true,
	}

	marshalDef := func(def interface{}) string {
		bytes, _ := yaml.Marshal(def)
		return string(bytes)
	}

	// starting point
	startDef := marshalDef(clusterdef.Cluster{
		NodeGroups: []*clusterdef.NodeGroup{
			{
				Count:   4,
				Version: "7.6.0-2038",
				Services: []clusterdef.Service{
					clusterdef.KvService,
					clusterdef.IndexService,
					clusterdef.QueryService,
					clusterdef.SearchService,
				},
			},
		},
	})

	// swap-rebalance
	rebalDef := marshalDef(clusterdef.Cluster{
		NodeGroups: []*clusterdef.NodeGroup{
			{
				Count:   3,
				Version: "7.6.0-2038",
				Services: []clusterdef.Service{
					clusterdef.KvService,
					clusterdef.IndexService,
					clusterdef.QueryService,
					clusterdef.SearchService,
				},
			},
		},
	})

	if createCluster {
		logger.Info("creating new cluster")

		newDinoId, err := dino.Alloc(dinoctl.AllocDef{Def: startDef})
		if err != nil {
			logger.Fatal("failed to allocate cluster", zap.Error(err))
		}

		dinoId = newDinoId
	}

	if !createCluster {
		logger.Info("rebalancing to starting state")

		err := dino.Modify(dinoId, dinoctl.AllocDef{Def: startDef})
		if err != nil {
			logger.Fatal("failed to modify cluster to starting state", zap.Error(err))
		}
	}

	logger.Info("setting up agent")

	// get the connection string for the cluster
	ipAddr, err := dino.Ip(dinoId)
	if err != nil {
		logger.Fatal("failed to get the clusters connection string", zap.Error(err))
	}

	// create an agent to interact with the cluster
	agent, err := gocbcorex.CreateAgent(ctx, gocbcorex.AgentOptions{
		Logger:    logger.WithOptions(zap.IncreaseLevel(zap.InfoLevel)),
		TLSConfig: nil,
		Authenticator: &gocbcorex.PasswordAuthenticator{
			Username: "Administrator",
			Password: "password",
		},
		BucketName: "default",
		SeedConfig: gocbcorex.SeedConfig{
			HTTPAddrs: []string{ipAddr + ":8091"},
			MemdAddrs: nil,
		},
	})
	if err != nil {
		logger.Fatal("failed to create agent", zap.Error(err))
	}

	// load 100 documents
	logger.Info("loading documents")
	for i := 0; i < 100; i++ {
		agent.Upsert(ctx, &gocbcorex.UpsertOptions{
			ScopeName:      "_default",
			CollectionName: "_default",
			Key:            []byte(fmt.Sprintf("situational-%d", i)),
			Value:          []byte(`{"foo":"bar"}`),
		})
	}

	// start a thread doing operations constantly
	logger.Info("setting up check thread")
	checkCtx, checkCancel := context.WithCancel(ctx)
	var completedOps uint64
	go func() {
		ctx := checkCtx

		var lastOp time.Time
		for {
			if ctx.Err() != nil {
				break
			}

			nextOp := lastOp.Add(10 * time.Millisecond)
			if nextOp.After(time.Now()) {
				time.Sleep(time.Until(nextOp))
			}

			opCtx, cancel := context.WithTimeout(ctx, 2500*time.Millisecond)

			randDocIdx := rand.Intn(100)
			_, err := agent.Get(opCtx, &gocbcorex.GetOptions{
				ScopeName:      "_default",
				CollectionName: "_default",
				Key:            []byte(fmt.Sprintf("situational-%d", randDocIdx)),
			})
			cancel()
			if err != nil {
				logger.Fatal("operation failed", zap.Error(err))
			}

			atomic.AddUint64(&completedOps, 1)
		}
	}()

	// wait 10 seconds for things to stabilize
	logger.Info("waiting for stability...")
	time.Sleep(10 * time.Second)

	// start the modification
	logger.Info("starting rebalance...")
	err = dino.Modify(dinoId, dinoctl.AllocDef{Def: rebalDef})
	if err != nil {
		logger.Fatal("failed to modify cluster to rebalance state", zap.Error(err))
	}

	// wait for 10 seconds to stabilize after the modification
	logger.Info("waiting for stability...")
	time.Sleep(10 * time.Second)

	// shut down the check thread
	logger.Info("shutting down check thread")
	checkCancel()

	logger.Info("check thread results",
		zap.Uint64("completedOps", completedOps))

	if createCluster {
		err := dino.Remove(dinoId)
		if err != nil {
			logger.Fatal("failed to remove cluster", zap.Error(err))
		}
	}
}
