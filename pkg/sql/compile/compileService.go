// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	txnClient "github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// todo: Move it to a CN level structure next day.
var compileService *ServiceOfCompile

func init() {
	compileService = InitCompileService()
	txnClient.SetRunningPipelineManagement(compileService)
}

func GetCompileService() *ServiceOfCompile {
	return compileService
}

// ServiceOfCompile is used to manage the lifecycle of Compile structures,
// including their creation and deletion.
//
// It also tracks the currently active complies within a single CN.
type ServiceOfCompile struct {
	sync.RWMutex

	// ongoing compiles with additional information.
	aliveCompiles map[*Compile]compileAdditionalInformation
}

// compileAdditionalInformation holds additional information for one compile.
// to help control one compile.
type compileAdditionalInformation struct {
	// mustReturnError holds an error that must be returned if set.
	mustReturnError error

	// queryCancel is a method to cancel an ongoing query.
	queryCancel context.CancelFunc
	// queryDone is a waiter that checks if this query has been completed or not.
	queryDone queryDoneWaiter
}

// kill one query and block until it was completed.
func (info *compileAdditionalInformation) kill(errResult error) {
	info.queryCancel()
	info.queryDone.checkCompleted()
	info.mustReturnError = errResult
}

type queryDoneWaiter chan bool

func newQueryDoneWaiter() queryDoneWaiter {
	return make(chan bool, 1)
}

func (waiter queryDoneWaiter) noticeQueryCompleted() {
	waiter <- true
}

func (waiter queryDoneWaiter) checkCompleted() {
	<-waiter
	waiter <- true
}

func (waiter queryDoneWaiter) clear() {
	for len(waiter) > 0 {
		<-waiter
	}
}

func InitCompileService() *ServiceOfCompile {
	srv := &ServiceOfCompile{
		aliveCompiles: make(map[*Compile]compileAdditionalInformation, 1024),
	}
	return srv
}

func (srv *ServiceOfCompile) getCompile(proc *process.Process) *Compile {
	// make sure the process has a cancel function.
	if proc.Cancel == nil {
		proc.Ctx, proc.Cancel = context.WithCancel(proc.Ctx)
	}

	runningCompile := reuse.Alloc[Compile](nil)
	// runningCompile := &Compile{
	// 	affectRows:   &atomic.Uint64{},
	// 	lock:         &sync.RWMutex{},
	// 	counterSet:   &perfcounter.CounterSet{},
	// 	nodeRegs:     make(map[[2]int32]*process.WaitRegister),
	// 	stepRegs:     make(map[int32][][2]int32),
	// 	metaTables:   make(map[string]struct{}),
	// 	lockTables:   make(map[uint64]*plan.LockTarget),
	// 	MessageBoard: process.NewMessageBoard(),
	// }
	if runningCompile.MessageBoard.OnUsed {
		fmt.Print("dddd")
	}
	runningCompile.AllocMsg = "alloc:" + time.Now().String() + " : " + string(debug.Stack())
	runningCompile.MessageBoard.OnUsed = true
	runningCompile.proc = proc
	compPtr := fmt.Sprintf("%p", runningCompile)
	runningCompile.MessageBoard.CompPtr = compPtr
	runningCompile.MessageBoard.AllocMsg = "alloc [" + compPtr + "]" + time.Now().String() + " : " + string(debug.Stack())
	return runningCompile
}

func (srv *ServiceOfCompile) startService(runningCompile *Compile) {
	if runningCompile.queryStatus == nil {
		runningCompile.queryStatus = newQueryDoneWaiter()
	} else {
		runningCompile.queryStatus.clear()
	}

	srv.Lock()
	srv.aliveCompiles[runningCompile] = compileAdditionalInformation{
		mustReturnError: nil,
		queryCancel:     runningCompile.proc.Cancel,
		queryDone:       runningCompile.queryStatus,
	}
	srv.Unlock()

}

func (srv *ServiceOfCompile) endService(c *Compile) (mustReturnError bool, err error) {
	c.queryStatus.noticeQueryCompleted()
	srv.Lock()
	if item, ok := srv.aliveCompiles[c]; ok {
		err = item.mustReturnError
	}
	delete(srv.aliveCompiles, c)
	c.queryStatus.clear()
	srv.Unlock()
	return err != nil, err
}

func (srv *ServiceOfCompile) putCompile(c *Compile) {
	if !c.MessageBoard.OnUsed {
		aa := "free" + time.Now().String() + " : " + string(debug.Stack())
		fmt.Print(aa)
	}
	str := fmt.Sprintf("%p", c)
	if process.GlobalMbCompileMap.Contains(str) {
		msg := "free" + time.Now().String() + " : " + string(debug.Stack())
		fmt.Print(msg)
	}
	if !c.isPrepare {
		c.FreeMsg = "free" + time.Now().String() + " : " + string(debug.Stack())
		c.OnUsed = false

		c.MessageBoard.FreeMsg = "free [" + str + "]" + time.Now().String() + " : " + string(debug.Stack())
		c.MessageBoard.OnUsed = false

		reuse.Free[Compile](c, nil)

	}

}

func (srv *ServiceOfCompile) aliveCompile() int {
	srv.Lock()
	defer srv.Unlock()

	return len(srv.aliveCompiles)
}

func (srv *ServiceOfCompile) PauseService() {
	srv.Lock()
}

func (srv *ServiceOfCompile) ResumeService() {
	srv.Unlock()
}

func (srv *ServiceOfCompile) KillAllQueriesWithError(err error) {
	for _, v := range srv.aliveCompiles {
		v.kill(err)
	}
}
