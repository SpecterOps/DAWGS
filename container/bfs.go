package container

import (
	"bufio"
	"compress/gzip"
	"context"
	"os"

	"github.com/specterops/dawgs/graph"
	"github.com/specterops/dawgs/util"
)

type BFSTreeFile struct {
	Path     string
	NumPaths uint64
}

func (s BFSTreeFile) Remove() error {
	return os.Remove(s.Path)
}

func (s BFSTreeFile) ReadEach(ctx context.Context, delegate func(next *Segment) (bool, error)) error {
	if fin, err := os.Open(s.Path); err != nil {
		return err
	} else {
		defer fin.Close()

		if gzipReader, err := gzip.NewReader(fin); err != nil {
			return err
		} else {
			defer gzipReader.Close()

			scanner := bufio.NewScanner(fin)
			scanner.Split(bufio.ScanLines)

			for scanner.Scan() {
				if err := scanner.Err(); err != nil {
					return err
				}

				if shouldContinue, err := delegate(UnmarshalSegment(scanner.Bytes())); err != nil {
					return err
				} else if !shouldContinue {
					break
				}
			}
		}
	}

	return nil
}

func WriteZoneBFSTree(zoneNodes graph.NodeSet, ts Triplestore, scratchPath string, maxDepth int) (BFSTreeFile, error) {
	var (
		numPaths           = uint64(0)
		zoneNodeMembership = zoneNodes.IDBitmap()
	)

	defer util.SLogMeasureFunction("WriteZoneBFSTree")()

	if scratchFile, err := os.CreateTemp(scratchPath, "zcp"); err != nil {
		return BFSTreeFile{}, err
	} else {
		scratchFileWriter := gzip.NewWriter(scratchFile)

		defer scratchFile.Close()
		defer scratchFileWriter.Close()

		for zoneNodeID, _ := range zoneNodes {
			TSBFS(
				ts,
				zoneNodeID.Uint64(),
				graph.DirectionInbound,
				maxDepth,
				func(edge Edge) bool {
					return !zoneNodeMembership.Contains(edge.Start)
				},
				func(segment *Segment) bool {
					numPaths += 1

					if err := MarshalSegment(segment, scratchFileWriter); err != nil {
						panic(err)
					}

					if _, err := scratchFileWriter.Write([]byte("\n")); err != nil {
						panic(err)
					}

					return true
				},
			)
		}

		return BFSTreeFile{
			Path:     scratchFile.Name(),
			NumPaths: numPaths,
		}, nil
	}
}
