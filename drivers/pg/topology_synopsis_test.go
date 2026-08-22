package pg

import "testing"

func TestTraversalTopologySynopsisAvailable(t *testing.T) {
	testCases := []struct {
		name     string
		synopsis TraversalTopologySynopsis
		want     bool
	}{
		{
			name: "current ready generation",
			synopsis: TraversalTopologySynopsis{
				Epoch:                1,
				SourceMutationEpoch:  4,
				CurrentMutationEpoch: 4,
				Status:               "ready",
			},
			want: true,
		},
		{
			name: "missing generation",
			synopsis: TraversalTopologySynopsis{
				SourceMutationEpoch:  4,
				CurrentMutationEpoch: 4,
				Status:               "ready",
			},
		},
		{
			name: "stale generation",
			synopsis: TraversalTopologySynopsis{
				Epoch:                1,
				SourceMutationEpoch:  3,
				CurrentMutationEpoch: 4,
				Status:               "ready",
			},
		},
		{
			name: "failed generation",
			synopsis: TraversalTopologySynopsis{
				Epoch:                1,
				SourceMutationEpoch:  4,
				CurrentMutationEpoch: 4,
				Status:               "failed",
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.synopsis.Available(); got != testCase.want {
				t.Fatalf("Available() = %t, want %t", got, testCase.want)
			}
		})
	}
}
