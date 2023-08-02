/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"golang.org/x/net/context"
	cri "k8s.io/cri-api/pkg/apis"
	pb "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type podMetricsOptions struct {
	// output format
	output string

	// live watch
	watch bool
}

var podMetricsCommand = &cli.Command{
	Name:                   "metricsp",
	Aliases:                []string{"metrics"},
	Usage:                  "List pod resource usage statistics",
	UseShortOptionHandling: true,
	ArgsUsage:              "[ID]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:    "output",
			Aliases: []string{"o"},
			Usage:   "Output format, One of: json|yaml",
		},
		&cli.BoolFlag{
			Name:    "watch",
			Aliases: []string{"w"},
			Usage:   "Watch pod metrics",
		},
	},
	Action: func(c *cli.Context) error {
		if c.NArg() > 0 {
			return cli.ShowSubcommandHelp(c)
		}

		client, err := getRuntimeService(c, 0)
		if err != nil {
			return fmt.Errorf("get runtime service: %w", err)
		}

		opts := podMetricsOptions{
			output: c.String("output"),
			watch:  c.Bool("watch"),
		}

		switch opts.output {
		case "json", "yaml", "":
		default:
			return cli.ShowSubcommandHelp(c)
		}

		if err := podMetrics(c.Context, client, opts); err != nil {
			return fmt.Errorf("get pod metrics: %w", err)
		}

		return nil
	},
}

func podMetrics(
	c context.Context,
	client cri.RuntimeService,
	opts podMetricsOptions,
) error {
	if !opts.watch {
		if err := displayPodMetrics(c, client, opts); err != nil {
			return fmt.Errorf("display pod metrics: %w", err)
		}

		return nil
	}

	displayErrCh := make(chan error, 1)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	watchCtx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Put the displayPodMetrics in another goroutine, because it might be
	// time consuming with lots of pods and we want to cancel it
	// ASAP when user hit CtrlC
	go func() {
		for range ticker.C {
			if err := displayPodMetrics(watchCtx, client, opts); err != nil {
				displayErrCh <- err
				break
			}
		}
	}()

	// listen for CtrlC or error
	select {
	case <-SetupInterruptSignalHandler():
		cancelFn()
		return nil
	case err := <-displayErrCh:
		return err
	}
}

func displayPodMetrics(
	c context.Context,
	client cri.RuntimeService,
	opts podMetricsOptions,
) error {
	metrics, err := podSandboxMetrics(client)
	if err != nil {
		return err
	}

	response := &pb.ListPodSandboxMetricsResponse{PodMetrics: metrics}
	switch opts.output {
	case "json", "":
		return outputProtobufObjAsJSON(response)
	case "yaml":
		return outputProtobufObjAsYAML(response)
	}
	return nil
}

func podSandboxMetrics(client cri.RuntimeService) ([]*pb.PodSandboxMetrics, error) {
	metrics, err := client.ListPodSandboxMetrics(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("list pod sandbox metrics: %w", err)
	}
	logrus.Debugf("PodMetrics: %v", metrics)

	return metrics, nil
}
