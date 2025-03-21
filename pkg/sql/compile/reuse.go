// Copyright 2022 Matrix Origin
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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/message"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/common/reuse"
)

func init() {
	reuse.CreatePool[Compile](
		func() *Compile {
			return &Compile{
				affectRows:   &atomic.Uint64{},
				counterSet:   &perfcounter.CounterSet{},
				nodeRegs:     make(map[[2]int32]*process.WaitRegister),
				stepRegs:     make(map[int32][][2]int32),
				lockTables:   make(map[uint64]*plan.LockTarget),
				MessageBoard: message.NewMessageBoard(),
			}
		},
		func(c *Compile) {
			c.clear()
		},
		reuse.DefaultOptions[Compile]().
			WithEnableChecker(),
	)

	reuse.CreatePool[Scope](
		func() *Scope {
			return &Scope{}
		},
		func(s *Scope) { *s = Scope{} },
		reuse.DefaultOptions[Scope]().
			WithEnableChecker(),
	)

	reuse.CreatePool[AnalyzeModule](
		func() *AnalyzeModule {
			return &AnalyzeModule{}
		},
		func(a *AnalyzeModule) { *a = AnalyzeModule{} },
		reuse.DefaultOptions[AnalyzeModule]().
			WithEnableChecker(),
	)

	reuse.CreatePool[fuzzyCheck](
		func() *fuzzyCheck {
			return &fuzzyCheck{}
		},
		func(f *fuzzyCheck) { f.clear() },
		reuse.DefaultOptions[fuzzyCheck]().
			WithEnableChecker(),
	)
}
