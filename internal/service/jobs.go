package service

import (
	"atlas-render/internal/model"
	"atlas-render/internal/raster"
	"atlas-render/internal/scene"
	"fmt"
	"sort"
	"time"
)

// pending counts unfinished work that occupies queue capacity: jobs still
// waiting on predecessors plus jobs already executable.
func pending(st model.State) int {
	n := 0
	for _, j := range st.Jobs {
		if j.State == "waiting" || j.State == "queued" || j.State == "running" {
			n++
		}
	}
	return n
}

// normalizeDeps canonicalizes the declared predecessors: order is not
// significant, duplicates are rejected, and the bound keeps persisted
// promises small. The sorted list is what gets stored and fingerprinted.
func normalizeDeps(dependsOn []string) ([]string, error) {
	if len(dependsOn) > model.MaxDependencies {
		return nil, model.Invalid("too many dependencies (max %d)", model.MaxDependencies)
	}
	deps := append([]string(nil), dependsOn...)
	sort.Strings(deps)
	for i, d := range deps {
		if !scene.ValidID(d) {
			return nil, model.Invalid("invalid dependency job id: %s", d)
		}
		if i > 0 && deps[i-1] == d {
			return nil, model.Invalid("duplicate dependency %s", d)
		}
	}
	return deps, nil
}

func (s *Service) Submit(snapshotID string, scale int, key string, dependsOn []string) (model.Job, error) {
	var out model.Job
	if !scene.ValidID(key) {
		return out, model.Invalid("idempotency key required (1..80 identifier characters)")
	}
	deps, e := normalizeDeps(dependsOn)
	if e != nil {
		return out, e
	}
	// The receipt fingerprint covers the request semantics: snapshot, scale,
	// and the dependency set. Dependency-free requests keep the historical
	// fingerprint shape so receipts recorded before dependencies existed
	// still replay the same job.
	parts := []any{snapshotID, scale}
	if len(deps) > 0 {
		parts = append(parts, deps)
	}
	fingerprint := model.CanonicalDigest(parts)
	e = s.Store.Update(func(st *model.State) error {
		if r, ok := st.Receipts[key]; ok {
			if r.Fingerprint != fingerprint {
				return model.Conflict("idempotency key used for different request")
			}
			out = st.Jobs[r.JobID]
			return nil
		}
		snap, ok := st.Snapshots[snapshotID]
		if !ok {
			return model.Missing("snapshot not found")
		}
		if snap.State != "active" {
			return model.Conflict("snapshot is revoked")
		}
		if _, e := raster.Compile(snap, scale); e != nil {
			return e
		}
		for _, d := range deps {
			if _, ok := st.Jobs[d]; !ok {
				return model.Missing(fmt.Sprintf("dependency job not found: %s", d))
			}
		}
		if pending(*st) >= 64 {
			return model.Busy("render queue is full")
		}
		now := time.Now().UTC()
		// A job whose predecessors have all already succeeded is executable
		// immediately; otherwise it waits until the success commit of its
		// last unfinished predecessor promotes it in the same transaction.
		state, kind := "queued", "job.queued"
		if !model.DepsSatisfied(*st, model.Job{DependsOn: deps}) {
			state, kind = "waiting", "job.waiting"
		}
		out = model.Job{ID: model.ID("job", st.Sequence+1), SnapshotID: snapshotID, State: state, Scale: scale, Attempt: 1, DependsOn: deps, CreatedAt: now, UpdatedAt: now, Payload: snap}
		st.Jobs[out.ID] = out
		st.Receipts[key] = model.Receipt{Key: key, Fingerprint: fingerprint, JobID: out.ID}
		st.Emit(kind, out.ID)
		return nil
	})
	return out, e
}
func (s *Service) Cancel(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State == "cancelled" {
			out = j
			return nil
		}
		if j.State != "waiting" && j.State != "queued" && j.State != "running" {
			return model.Conflict("job has already finished")
		}
		j.State = "cancelled"
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.cancelled", id)
		out = j
		return nil
	})
	return out, e
}
func (s *Service) Retry(id string) (model.Job, error) {
	var out model.Job
	e := s.Store.Update(func(st *model.State) error {
		j, ok := st.Jobs[id]
		if !ok {
			return model.Missing("job not found")
		}
		if j.State != "failed" && j.State != "cancelled" {
			return model.Conflict("only failed or cancelled jobs can retry")
		}
		if pending(*st) >= 64 {
			return model.Busy("render queue is full")
		}
		// Re-enter the pipeline respecting declared predecessors: a retried
		// job whose dependencies are not all satisfied goes back to waiting
		// instead of jumping the queue.
		j.State = "queued"
		if !model.DepsSatisfied(*st, j) {
			j.State = "waiting"
		}
		j.Error = ""
		j.Attempt++
		j.UpdatedAt = time.Now().UTC()
		st.Jobs[id] = j
		st.Emit("job.retried", id)
		out = j
		return nil
	})
	return out, e
}
func (s *Service) Purge(id string) error {
	return s.Store.Update(func(st *model.State) error {
		a, ok := st.Artifacts[id]
		if !ok {
			return model.Missing("artifact not found")
		}
		if a.State == "purged" {
			return nil
		}
		a.State = "purged"
		a.PNG = nil
		st.Artifacts[id] = a
		st.Emit("artifact.purged", id)
		return nil
	})
}
func (s *Service) Artifact(id string) (model.Artifact, error) {
	a, ok := s.Store.Read().Artifacts[id]
	if !ok {
		return a, model.Missing("artifact not found")
	}
	if a.State == "purged" {
		return a, model.Gone(fmt.Sprintf("artifact %s was purged", id))
	}
	return a, nil
}
