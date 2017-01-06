package mock

import (
	sstream "github.com/fuserobotics/statestream"
	"time"
)

func MockDataset() []*sstream.StreamEntry {
	now := RootMockTime
	res := []*sstream.StreamEntry{}
	for i := 0; i < 10; i++ {
		typ := sstream.StreamEntryMutation
		if i == 0 || i == 5 {
			typ = sstream.StreamEntrySnapshot
		}
		res = append(res, &sstream.StreamEntry{
			Data: sstream.StateData{
				"test": i + 1,
			},
			Timestamp: now.Add(time.Duration(-10+i) * time.Second),
			Type:      typ,
		})
	}
	return res
}
