package load

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
	tsbsMongo "github.com/timescale/tsbs/pkg/targets/mongo"
)

// sendOrQueueBatch attempts to send a Batch of data on a duplexChannel.
// If it would block or there is other work to be sent first, the Batch is stored on a queue.
// The count of outstanding work is adjusted upwards
func sendOrQueueBatchMetaFields(ch *duplexChannel, count *int, batch targets.Batch, unsent []targets.Batch) []targets.Batch {
	// In case there are no outstanding batches yet and there are empty positions in toWorker queue
	// we can send/put batch into toWorker queue
	*count++
	if len(unsent) == 0 && len(ch.toWorker) < cap(ch.toWorker) {
		ch.sendToWorker(batch)
	} else {
		return append(unsent, batch)
	}
	return unsent
}

// scanWithBatchingMetaFields reads data from the DataSource ds until a limit is reached
// (if -1, all items are read).
// Data is then placed into a map with the key being the meta field's value and the value
// being the Data with the corresponding meta field. 
// Data with the same meta field values are then dispatched to workers (duplexChannel
// chosen by PointIndexer). 
func scanWithBatchingMetaFields(  
	channels []*duplexChannel, batchSize uint, limit uint64,  
	ds targets.DataSource, factory targets.BatchFactory, indexer targets.PointIndexer,  
	metaFieldIndex string) uint64 {  
  
	var itemsRead uint64  
	numChannels := len(channels)  
  
	if batchSize < 1 {  
		panic("--batch-size cannot be less than 1")  
	}  
  
	// TODO: Figure out if you need to still have unsentBatches because we aren't
	// implementing with flow control anymore.
	// Current batches (per channel) that are being filled with items from scanner
	fillingBatches := make([]targets.Batch, numChannels)
	for i := range fillingBatches {
		fillingBatches[i] = factory.New()
	}

	// Batches that are ready to be set when space on a channel opens
	unsentBatches := make([][]targets.Batch, numChannels)
	for i := range unsentBatches {
		unsentBatches[i] = []targets.Batch{}
	}
  
	// Map for storing points based on meta field value  
	pointsBatchedOnMetaField := make(map[string][]data.LoadedPoint)  
  
	// Process input data and populate pointsBatchedOnMetaField  
	for {  
		// Stop if we've reached the limit.  
		if limit > 0 && itemsRead == limit {  
			break  
		}  
  
		item := ds.NextItem()  
		if item.Data == nil {  
			break // No more items to process  
		}  
		itemsRead++  
  
		// Group data points based on the meta field value  
		// TODO: double-check that this is just a point and not a group of points...
		tagsMap := map[string]string{}  
		t := &tsbsMongo.MongoTag{}
		p := item.Data.(*tsbsMongo.MongoPoint)
		for j := 0; j < p.TagsLength(); j++ {  
			p.Tags(t, j)  
			tagsMap[string(t.Key())] = string(t.Value())  
		}  

		metaIndexValue := tagsMap[metaFieldIndex]  
		pointsBatchedOnMetaField[metaIndexValue] = append(pointsBatchedOnMetaField[metaIndexValue], item)  
	}  

  	// We aren't implementing flow control here so ocnt gets largely ignored.
	ocnt := 0
	idx := 0
	// Range through all the points for each associated meta field.
	for _, points := range pointsBatchedOnMetaField {
		pointIdx := 0
		pointsLen := len(points)
		
		for pointIdx < pointsLen {
			// Determine how many points we can take to fill the current batch.
			// TODO: Is this conversion problematic
			remainingSpace := int(batchSize - fillingBatches[idx].Len())
			endIdx := pointIdx + remainingSpace
			if endIdx > pointsLen {
				endIdx = pointsLen
			}

			// Append points to the current batch we are filling
			for _, point := range points[pointIdx:endIdx] {
				fillingBatches[idx].Append(point)
			}

			// Update the starting index for the next slice
			pointIdx = endIdx

			// If the batch is filled, send it and switch to the next channel
			if fillingBatches[idx].Len() >= batchSize {
				unsentBatches[idx] = sendOrQueueBatchMetaFields(channels[idx], &ocnt, fillingBatches[idx], unsentBatches[idx])
				fillingBatches[idx] = factory.New()
				idx += 1
				if idx == numChannels {
					idx = 0
				}
			}
		}
	}

	// Send remaining batches that are not full but contain items  
	for idx, b := range fillingBatches {  
		if b.Len() > 0 {  
			unsentBatches[idx] = sendOrQueueBatchMetaFields(channels[idx], &ocnt, fillingBatches[idx], unsentBatches[idx])
		}  
	}  
  
	return itemsRead  
}  