// Copyright 2016-2018, Pulumi Corporation.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deploy

import (
	"context"
	"fmt"

	"github.com/blang/semver"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/pulumi/pulumi/pkg/resource/deploy/providers"
	"github.com/pulumi/pulumi/pkg/resource/plugin"
	"github.com/pulumi/pulumi/pkg/tokens"
	"github.com/pulumi/pulumi/pkg/util/contract"
	"github.com/pulumi/pulumi/pkg/util/logging"
	"github.com/pulumi/pulumi/pkg/util/result"
	"github.com/pulumi/pulumi/pkg/util/rpcutil"
	pulumirpc "github.com/pulumi/pulumi/sdk/proto/go"
)

// NewQuerySource returns a planning source that fetches resources by evaluating
// a package with a set of args and a confgiuration map.  This evaluation is
// performed using the given plugin context and may optionally use the given
// plugin host (or the default, if this is nil).  Note that closing the eval
// source also closes the host.
func NewQuerySource(plugctx *plugin.Context, runinfo *EvalRunInfo,
	defaultProviderVersions map[tokens.Package]*semver.Version, dryRun bool) Source {

	return &querySource{
		plugctx:                 plugctx,
		runinfo:                 runinfo,
		defaultProviderVersions: defaultProviderVersions,
		dryRun:                  dryRun,
	}
}

type querySource struct {
	plugctx                 *plugin.Context                    // the plugin context.
	runinfo                 *EvalRunInfo                       // the directives to use when running the program.
	defaultProviderVersions map[tokens.Package]*semver.Version // the default provider versions for this source.
	dryRun                  bool                               // true if this is a dry-run operation only.
}

func (src *querySource) Close() error {
	return nil
}

// Project is the name of the project being run by this evaluation source.
func (src *querySource) Project() tokens.PackageName {
	return src.runinfo.Proj.Name
}

// Stack is the name of the stack being targeted by this evaluation source.
func (src *querySource) Stack() tokens.QName {
	return src.runinfo.Target.Name
}

func (src *querySource) Info() interface{} { return src.runinfo }

// Iterate will spawn an evaluator coroutine and prepare to interact with it on subsequent calls to Next.
func (src *querySource) Iterate(
	ctx context.Context, opts Options, providers ProviderSource) (SourceIterator, result.Result) {

	contract.Ignore(ctx) // TODO[pulumi/pulumi#1714]

	// First, fire up a resource monitor that will watch for and record resource creation.
	regChan := make(chan *registerResourceEvent)
	regOutChan := make(chan *registerResourceOutputsEvent)
	regReadChan := make(chan *readResourceEvent)
	mon, err := newQueryResourceMonitor(src, providers, regChan, regOutChan, regReadChan)
	if err != nil {
		return nil, result.FromError(errors.Wrap(err, "failed to start resource monitor"))
	}

	// Create a new iterator with appropriate channels, and gear up to go!
	iter := &querySourceIterator{
		mon:         mon,
		src:         src,
		regChan:     regChan,
		regOutChan:  regOutChan,
		regReadChan: regReadChan,
		finChan:     make(chan result.Result),
	}

	// Now invoke Run in a goroutine.  All subsequent resource creation events will come in over the gRPC channel,
	// and we will pump them through the channel.  If the Run call ultimately fails, we need to propagate the error.
	iter.forkRun(opts)

	// Finally, return the fresh iterator that the caller can use to take things from here.
	return iter, nil
}

type QuerySourceIterator interface {
	Wait() result.Result
}

type querySourceIterator struct {
	mon         *resmon                            // the resource monitor, per iterator.
	src         *querySource                       // the owning eval source object.
	regChan     chan *registerResourceEvent        // the channel that contains resource registrations.
	regOutChan  chan *registerResourceOutputsEvent // the channel that contains resource completions.
	regReadChan chan *readResourceEvent            // the channel that contains read resource requests.
	finChan     chan result.Result                 // the channel that communicates completion.
	done        bool                               // set to true when the evaluation is done.
}

func (iter *querySourceIterator) Close() error {
	// Cancel the monitor and reclaim any associated resources.
	return iter.mon.Cancel()
}

func (iter *querySourceIterator) Next() (SourceEvent, result.Result) {
	// If we are done, quit.
	if iter.done {
		return nil, nil
	}

	// Await the program to compute some more state and then inspect what it has to say.
	select {
	case reg := <-iter.regChan:
		contract.Assert(reg != nil)
		goal := reg.Goal()
		logging.V(5).Infof("EvalSourceIterator produced a registration: t=%v,name=%v,#props=%v",
			goal.Type, goal.Name, len(goal.Properties))
		return reg, nil
	case regOut := <-iter.regOutChan:
		contract.Assert(regOut != nil)
		logging.V(5).Infof("EvalSourceIterator produced a completion: urn=%v,#outs=%v",
			regOut.URN(), len(regOut.Outputs()))
		return regOut, nil
	case read := <-iter.regReadChan:
		contract.Assert(read != nil)
		logging.V(5).Infoln("EvalSourceIterator produced a read")
		return read, nil
	case res := <-iter.finChan:
		// If we are finished, we can safely exit.  The contract with the language provider is that this implies
		// that the language runtime has exited and so calling Close on the plugin is fine.
		iter.done = true
		if res != nil {
			if res.IsBail() {
				logging.V(5).Infof("EvalSourceIterator ended with bail.")
			} else {
				logging.V(5).Infof("EvalSourceIterator ended with an error: %v", res.Error())
			}
		}
		return nil, res
	}
}

func (iter *querySourceIterator) Wait() result.Result {
	select {
	case res := <-iter.finChan:
		iter.done = true
		return res
	}
}

// forkRun performs the evaluation from a distinct goroutine.  This function blocks until it's our turn to go.
func (iter *querySourceIterator) forkRun(opts Options) {
	// Fire up the goroutine to make the RPC invocation against the language runtime.  As this executes, calls
	// to queue things up in the resource channel will occur, and we will serve them concurrently.
	go func() {
		// Next, launch the language plugin.
		run := func() result.Result {
			rt := iter.src.runinfo.Proj.Runtime.Name()
			langhost, err := iter.src.plugctx.Host.LanguageRuntime(rt)
			if err != nil {
				return result.FromError(errors.Wrapf(err, "failed to launch language host %s", rt))
			}
			contract.Assertf(langhost != nil, "expected non-nil language host %s", rt)

			// Make sure to clean up before exiting.
			defer contract.IgnoreClose(langhost)

			// Decrypt the configuration.
			config, err := iter.src.runinfo.Target.Config.Decrypt(iter.src.runinfo.Target.Decrypter)
			if err != nil {
				return result.FromError(err)
			}

			// Now run the actual program.
			progerr, bail, err := langhost.Run(plugin.RunInfo{
				MonitorAddress: iter.mon.Address(),
				Stack:          string(iter.src.runinfo.Target.Name),
				Project:        string(iter.src.runinfo.Proj.Name),
				Pwd:            iter.src.runinfo.Pwd,
				Program:        iter.src.runinfo.Program,
				Args:           iter.src.runinfo.Args,
				Config:         config,
				DryRun:         iter.src.dryRun,
				Parallel:       opts.Parallel,
			})

			// Check if we were asked to Bail.  This a special random constant used for that
			// purpose.
			if err == nil && bail {
				return result.Bail()
			}

			if err == nil && progerr != "" {
				// If the program had an unhandled error; propagate it to the caller.
				err = errors.Errorf("an unhandled error occurred: %v", progerr)
			}
			return result.WrapIfNonNil(err)
		}

		// Communicate the error, if it exists, or nil if the program exited cleanly.
		iter.finChan <- run()
	}()
}

// newResourceMonitor creates a new resource monitor RPC server.
func newQueryResourceMonitor(src *querySource, provs ProviderSource, regChan chan *registerResourceEvent,
	regOutChan chan *registerResourceOutputsEvent, regReadChan chan *readResourceEvent) (*resmon, error) {

	// Create our cancellation channel.
	cancel := make(chan bool)

	// Create a new default provider manager.
	d := &defaultProviders{
		defaultVersions: src.defaultProviderVersions,
		providers:       make(map[string]providers.Reference),
		config:          src.runinfo.Target,
		requests:        make(chan defaultProviderRequest),
		regChan:         regChan,
		cancel:          cancel,
	}

	// New up an engine RPC server.
	resmon := &resmon{
		providers:        provs,
		defaultProviders: d,
		regChan:          regChan,
		regOutChan:       regOutChan,
		regReadChan:      regReadChan,
		cancel:           cancel,
	}

	// Fire up a gRPC server and start listening for incomings.
	port, done, err := rpcutil.Serve(0, resmon.cancel, []func(*grpc.Server) error{
		func(srv *grpc.Server) error {
			pulumirpc.RegisterResourceMonitorServer(srv, resmon)
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	resmon.addr = fmt.Sprintf("127.0.0.1:%d", port)
	resmon.done = done

	go d.serve()

	return resmon, nil
}
