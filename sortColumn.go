package main

import (
  "sort"
)

// multiSorter example from https://golang.org/pkg/sort/  SortMultiKeys
// sort columns based on defined less function

type lessFunc func(p1, p2 *[]string) bool

type multiSorter struct {
  rows  [][]string
  less  []lessFunc
}

func (ms *multiSorter) Sort(rws [][]string) {
  ms.rows = rws
  sort.Sort(ms)
}

func OrderedBy(l ...lessFunc) *multiSorter {
  return &multiSorter {
           less: l,
  }
}

func (ms *multiSorter) Len() int { return len(ms.rows) }
func (ms *multiSorter) Swap(i, j int) { ms.rows[i], ms.rows[j] = ms.rows[j], ms.rows[i] }
func (ms *multiSorter) Less(i, j int) bool {
  p, q := &ms.rows[i], &ms.rows[j]
  // Try all but the last comparison.
  var k int
  for k = 0; k < len(ms.less)-1; k++ {
    less := ms.less[k]
    switch {
    case less(p, q):
      // p < q, so we have a decision.
      return true
    case less(q, p):
      // p > q, so we have a decision.
      return false
    }
    // p == q; try the next comparison.
  }
  // All comparisons to here said "equal", so just return whatever
  // the final comparison reports.
  return ms.less[k](p, q)
}

func incColumn(col int) lessFunc {
  return func(rowi, rowj *[]string) bool {
           return (*rowi)[col] < (*rowj)[col]
         }
}

func decColumn(col int) lessFunc {
  return func(rowi, rowj *[]string) bool {
           return (*rowi)[col] > (*rowj)[col]
         }
}
