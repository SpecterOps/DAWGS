package retriever

import "testing"

func TestRetrieverNextProgressAtInterval(t *testing.T) {
	cases := []struct {
		name           string
		processed      int64
		planned        int64
		nextProgressAt int64
		interval       int64
		want           int64
	}{
		{
			name:           "before threshold",
			processed:      9,
			planned:        100,
			nextProgressAt: 10,
			interval:       10,
			want:           10,
		},
		{
			name:           "at threshold",
			processed:      10,
			planned:        100,
			nextProgressAt: 10,
			interval:       10,
			want:           20,
		},
		{
			name:           "large jump",
			processed:      95,
			planned:        200,
			nextProgressAt: 10,
			interval:       10,
			want:           100,
		},
		{
			name:           "planned boundary",
			processed:      95,
			planned:        100,
			nextProgressAt: 10,
			interval:       10,
			want:           0,
		},
		{
			name:           "default interval",
			processed:      retrieverProgressEntityInterval * 2,
			planned:        retrieverProgressEntityInterval * 4,
			nextProgressAt: retrieverProgressEntityInterval,
			interval:       0,
			want:           retrieverProgressEntityInterval * 3,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := retrieverNextProgressAtInterval(testCase.processed, testCase.planned, testCase.nextProgressAt, testCase.interval)
			if got != testCase.want {
				t.Fatalf("next progress = %d, want %d", got, testCase.want)
			}
		})
	}
}
