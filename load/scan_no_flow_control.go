package load

import (
		"hash/fnv"

		"github.com/timescale/tsbs/pkg/targets"
		tsbsMongo "github.com/timescale/tsbs/pkg/targets/mongo"
)

func hashMeow(s string) uint {
	h := fnv.New32a()
	h.Write([]byte(s))
	return uint(h.Sum32())
}

// scanWithoutFlowControl reads data from the DataSource ds until a limit is reached (if -1, all items are read).
// Data is then placed into appropriate batches, using the supplied PointIndexer,
// which are then dispatched to workers (channel idx chosen by PointIndexer).
// readDs does no flow control, if the capacity of a channel is reached, scanning stops for all
// workers. (should only happen if channel-capacity is low and one worker is unreasonable slower than the rest)
// in that case just set hash-workers to false and use 1 channel for all workers.
func scanWithoutFlowControl(
	ds targets.DataSource, indexer targets.PointIndexer, factory targets.BatchFactory, channels []chan targets.Batch,
	batchSize uint, limit uint64, batchedInserts bool, metaFieldIndex string) uint64 {
	if batchSize == 0 {
		panic("batch size can't be 0")
	}
	numChannels := len(channels)
	batches := make([]targets.Batch, numChannels)
	for i := 0; i < numChannels; i++ {
		batches[i] = factory.New()
	}
	var itemsRead uint64
	for {
		if limit > 0 && itemsRead >= limit {
			break
		}
		item := ds.NextItem()
		if item.Data == nil {
			// Nothing to scan any more - input is empty or failed
			// Time to exit
			break
		}
		itemsRead++

		idx := indexer.GetIndex(item)
		metaIndexVal := ""
		metaIndexValExists := false
		if batchedInserts { 
			t := &tsbsMongo.MongoTag{}
			p := item.Data.(*tsbsMongo.MongoPoint)
			for j := 0; j < p.TagsLength(); j++ {  
				p.Tags(t, j)  
				if string(t.Key()) == metaFieldIndex {
					metaIndexVal = string(t.Value())
					metaIndexValExists = true
				}
			}
			// Only assign the channel based on the metaIndexVal if we find a metaIndexVal
			if metaIndexValExists {
				idx = hashMeow(metaIndexVal) % uint(numChannels) 
			} 
		}

		batches[idx].Append(item)

		if batches[idx].Len() >= batchSize {
			channels[idx] <- batches[idx]
			batches[idx] = factory.New()
		}
	}

	for idx, unfilledBatch := range batches {
		if unfilledBatch.Len() > 0 {
			channels[idx] <- unfilledBatch
		}
	}
	return itemsRead
}
