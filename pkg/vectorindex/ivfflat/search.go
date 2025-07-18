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

package ivfflat

import (
	"context"
	"fmt"
	"strconv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/cache"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var runSql = sqlexec.RunSql
var runSql_streaming = sqlexec.RunStreamingSql

type Centroid[T types.RealNumbers] struct {
	Id  int64
	Vec []T
}

// Ivf search index struct to hold the usearch index
type IvfflatSearchIndex[T types.RealNumbers] struct {
	Version   int64
	Centroids []Centroid[T]
}

// This is the Ivf search implementation that implement VectorIndexSearchIf interface
type IvfflatSearch[T types.RealNumbers] struct {
	Idxcfg        vectorindex.IndexConfig
	Tblcfg        vectorindex.IndexTableConfig
	Index         *IvfflatSearchIndex[T]
	ThreadsSearch int64
}

func (idx *IvfflatSearchIndex[T]) LoadIndex(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, nthread int64) error {

	idx.Version = idxcfg.Ivfflat.Version
	sql := fmt.Sprintf("SELECT `%s`, `%s` FROM `%s`.`%s` WHERE `%s` = %d",
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_id,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_centroid,
		tblcfg.DbName, tblcfg.IndexTable,
		catalog.SystemSI_IVFFLAT_TblCol_Centroids_version,
		idxcfg.Ivfflat.Version)

	//os.Stderr.WriteString(fmt.Sprintf("Load Index SQL = %s\n", sql))
	res, err := runSql(proc, sql)
	if err != nil {
		return err
	}
	defer res.Close()

	if len(res.Batches) == 0 {
		return nil
	}

	idx.Centroids = make([]Centroid[T], 0, idxcfg.Ivfflat.Lists)
	for _, bat := range res.Batches {
		idVec := bat.Vecs[0]
		faVec := bat.Vecs[1]
		for r := range bat.RowCount() {
			id := vector.GetFixedAtWithTypeCheck[int64](idVec, r)
			if faVec.IsNull(uint64(r)) {
				//os.Stderr.WriteString("Centroid is NULL\n")
				continue
			}
			vec := types.BytesToArray[T](faVec.GetBytesAt(r))
			idx.Centroids = append(idx.Centroids, Centroid[T]{Id: id, Vec: vec})
		}
	}

	//os.Stderr.WriteString(fmt.Sprintf("%d centroids loaded... lists = %d, centroid %v\n", len(idx.Centroids), idxcfg.Ivfflat.Lists, idx.Centroids))
	return nil
}

// load chunk from database
func (idx *IvfflatSearchIndex[T]) searchEntries(lctx context.Context, proc *process.Process,
	query []T, distfn metric.DistanceFunction[T], heap *vectorindex.SearchResultSafeHeap,
	stream_chan chan executor.Result, error_chan chan error) (stream_closed bool, err error) {

	var res executor.Result
	var ok bool

	select {
	case res, ok = <-stream_chan:
		if !ok {
			return true, nil
		}
	case err = <-error_chan:
		return false, err
	case <-proc.Ctx.Done():
		return false, proc.Ctx.Err()
	case <-lctx.Done():
		// local context cancelled. something went wrong with other threads
		return false, context.Cause(lctx)
	}

	bat := res.Batches[0]
	defer res.Close()

	for i := 0; i < bat.RowCount(); i++ {
		pk := vector.GetAny(bat.Vecs[0], i)
		if bat.Vecs[1].IsNull(uint64(i)) {
			continue
		}
		vec := types.BytesToArray[T](bat.Vecs[1].GetBytesAt(i))
		dist, err := distfn(query, vec)
		if err != nil {
			return false, err
		}

		heap.Push(&vectorindex.SearchResultAnyKey{Id: pk, Distance: float64(dist)})
	}
	return false, nil
}

func (idx *IvfflatSearchIndex[T]) findCentroids(proc *process.Process, query []T, distfn metric.DistanceFunction[T], idxcfg vectorindex.IndexConfig, probe uint, nthread int64) ([]int64, error) {

	if len(idx.Centroids) == 0 {
		// empty index has id = 1
		return []int64{1}, nil
	}

	nworker := int(nthread)
	ncentroids := int(len(idx.Centroids))
	if ncentroids < nworker {
		nworker = ncentroids
	}
	errs := make(chan error, nworker)

	heap := vectorindex.NewSearchResultSafeHeap(ncentroids)
	var wg sync.WaitGroup
	for n := 0; n < nworker; n++ {
		wg.Add(1)
		go func(tid int) {
			defer wg.Done()
			for i, c := range idx.Centroids {
				if i%nworker != tid {
					continue
				}
				dist, err := distfn(query, c.Vec)
				if err != nil {
					errs <- err
					return
				}
				heap.Push(&vectorindex.SearchResult{Id: c.Id, Distance: float64(dist)})
			}
		}(n)
	}

	wg.Wait()

	if len(errs) > 0 {
		return nil, <-errs
	}

	res := make([]int64, 0, probe)
	n := heap.Len()
	for i := 0; i < int(probe) && i < n; i++ {
		srif := heap.Pop()
		sr, ok := srif.(*vectorindex.SearchResult)
		if !ok {
			return nil, moerr.NewInternalError(proc.Ctx, "findCentroids: heap return key is not int64")
		}
		res = append(res, sr.Id)
	}

	//os.Stderr.WriteString(fmt.Sprintf("probe %d... return centroid ids %v\n", probe, res))
	return res, nil
}

// Call usearch.Search
func (idx *IvfflatSearchIndex[T]) Search(proc *process.Process, idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig, query []T, rt vectorindex.RuntimeConfig, nthread int64) (keys any, distances []float64, err error) {

	stream_chan := make(chan executor.Result, nthread)
	error_chan := make(chan error, nthread)
	lctx := context.Background()
	lctx, cancel := context.WithCancelCause(lctx)
	defer cancel(nil)

	distfn, err := metric.ResolveDistanceFn[T](metric.MetricType(idxcfg.Ivfflat.Metric))
	if err != nil {
		return nil, nil, err
	}

	centroids_ids, err := idx.findCentroids(proc, query, distfn, idxcfg, rt.Probe, nthread)
	if err != nil {
		return nil, nil, err
	}

	var instr string
	for i, c := range centroids_ids {
		if i > 0 {
			instr += ","
		}
		instr += strconv.FormatInt(c, 10)
	}

	sql := fmt.Sprintf("SELECT `%s`, `%s` FROM `%s`.`%s` WHERE `%s` = %d AND `%s` IN (%s)",
		catalog.SystemSI_IVFFLAT_TblCol_Entries_pk,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_entry,
		tblcfg.DbName, tblcfg.EntriesTable,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_version,
		idx.Version,
		catalog.SystemSI_IVFFLAT_TblCol_Entries_id,
		instr,
	)
	//os.Stderr.WriteString(sql)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := runSql_streaming(proc, sql, stream_chan, error_chan)
		if err != nil {
			// send multiple errors to stop the nworker threads
			cancel(err)
			return
		}
	}()

	heap := vectorindex.NewSearchResultSafeHeap(int(rt.Probe * 1000))

	for n := int64(0); n < nthread; n++ {

		wg.Add(1)

		go func() {
			defer wg.Done()

			// brute force search with selected centroids
			sql_closed := false
			var err2 error
			for !sql_closed {
				sql_closed, err2 = idx.searchEntries(lctx, proc, query, distfn, heap, stream_chan, error_chan)
				if err2 != nil {
					cancel(err2)
					return
				}
			}
		}()
	}

	wg.Wait()

	// check local context cancelled
	select {
	case <-proc.Ctx.Done():
		return nil, nil, proc.Ctx.Err()
	case <-lctx.Done():
		err := context.Cause(lctx)
		return nil, nil, err
	default:
	}

	resid := make([]any, 0, rt.Limit)
	resdist := make([]float64, 0, rt.Limit)
	n := heap.Len()
	for i := 0; i < int(rt.Limit) && i < n; i++ {
		srif := heap.Pop()
		sr, ok := srif.(*vectorindex.SearchResultAnyKey)
		if !ok {
			return nil, nil, moerr.NewInternalError(proc.Ctx, "ivf search: heap return key is not any")
		}
		resid = append(resid, sr.Id)
		resdist = append(resdist, sr.Distance)
	}

	return resid, resdist, nil
}

func (idx *IvfflatSearchIndex[T]) Destroy() {
	idx.Centroids = nil
}

func NewIvfflatSearch[T types.RealNumbers](idxcfg vectorindex.IndexConfig, tblcfg vectorindex.IndexTableConfig) *IvfflatSearch[T] {
	nthread := vectorindex.GetConcurrency(tblcfg.ThreadsSearch)
	s := &IvfflatSearch[T]{Idxcfg: idxcfg, Tblcfg: tblcfg, ThreadsSearch: nthread}
	return s
}

// Search the hnsw index (implement VectorIndexSearch.Search)
func (s *IvfflatSearch[T]) Search(proc *process.Process, anyquery any, rt vectorindex.RuntimeConfig) (keys any, distances []float64, err error) {
	query, ok := anyquery.([]T)
	if !ok {
		return nil, nil, moerr.NewInternalErrorNoCtx("IvfSearch: query not match with index type")
	}

	return s.Index.Search(proc, s.Idxcfg, s.Tblcfg, query, rt, s.ThreadsSearch)
}

func (s *IvfflatSearch[T]) Contains(key int64) (bool, error) {
	return true, nil
}

// Destroy IvfflatSearch (implement VectorIndexSearch.Destroy)
func (s *IvfflatSearch[T]) Destroy() {
	// destroy index
	if s.Index != nil {
		s.Index.Destroy()
	}
	s.Index = nil
}

// load index from database (implement VectorIndexSearch.LoadFromDatabase)
func (s *IvfflatSearch[T]) Load(proc *process.Process) error {

	idx := &IvfflatSearchIndex[T]{}
	// load index model
	err := idx.LoadIndex(proc, s.Idxcfg, s.Tblcfg, s.ThreadsSearch)
	if err != nil {
		return err
	}
	s.Index = idx
	return nil
}

// check config and update some parameters such as ef_search
func (s *IvfflatSearch[T]) UpdateConfig(newalgo cache.VectorIndexSearchIf) error {

	return nil
}
