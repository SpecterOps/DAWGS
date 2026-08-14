package ret

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/specterops/dawgs/ret/archive"
	"github.com/specterops/dawgs/ret/observe"
)

const (
	packOperationName   = "pack"
	unpackOperationName = "unpack"
)

type PackConfig struct {
	CollectionDirectory string
	ArchivePath         string
	Recipient           archive.PublicKey
	Observer            observe.Observer
}

type UnpackConfig struct {
	ArchivePath     string
	OutputDirectory string
	Identity        archive.PrivateKey
	Observer        observe.Observer
}

type KeygenConfig struct {
	PrivateKeyPath string
	PublicKeyPath  string
}

func (s PackConfig) Validate() error {
	if strings.TrimSpace(s.CollectionDirectory) == "" ||
		strings.TrimSpace(s.ArchivePath) == "" {
		return fmt.Errorf("%w: collection directory and archive path are required", ErrInvalidConfig)
	}
	if s.Recipient == (archive.PublicKey{}) {
		return fmt.Errorf("%w: archive recipient is required", ErrInvalidConfig)
	}
	return nil
}

func (s UnpackConfig) Validate() error {
	if strings.TrimSpace(s.ArchivePath) == "" ||
		strings.TrimSpace(s.OutputDirectory) == "" {
		return fmt.Errorf("%w: archive path and output directory are required", ErrInvalidConfig)
	}
	if s.Identity == (archive.PrivateKey{}) {
		return fmt.Errorf("%w: archive identity is required", ErrInvalidConfig)
	}
	return nil
}

func (s KeygenConfig) Validate() error {
	if strings.TrimSpace(s.PrivateKeyPath) == "" ||
		strings.TrimSpace(s.PublicKeyPath) == "" {
		return fmt.Errorf("%w: private and public key paths are required", ErrInvalidConfig)
	}
	privatePath, err := filepath.Abs(s.PrivateKeyPath)
	if err != nil {
		return fmt.Errorf("%w: resolve private key path: %w", ErrInvalidConfig, err)
	}
	publicPath, err := filepath.Abs(s.PublicKeyPath)
	if err != nil {
		return fmt.Errorf("%w: resolve public key path: %w", ErrInvalidConfig, err)
	}
	if filepath.Clean(privatePath) == filepath.Clean(publicPath) {
		return fmt.Errorf("%w: private and public key paths must differ", ErrInvalidConfig)
	}
	return nil
}

func Pack(ctx context.Context, config PackConfig) (resultErr error) {
	started := time.Now()
	observe.Emit(ctx, config.Observer, observe.OperationStarted{Operation: packOperationName})
	defer func() {
		observe.Emit(ctx, config.Observer, observe.OperationCompleted{
			Operation: packOperationName,
			Duration:  time.Since(started),
			Err:       resultErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("pack: %w", err)
	}
	if err := config.Validate(); err != nil {
		return err
	}
	return archive.Create(ctx, archive.CreateConfig{
		CollectionDirectory: config.CollectionDirectory,
		ArchivePath:         config.ArchivePath,
		Recipient:           config.Recipient,
		Observer:            config.Observer,
	})
}

func Unpack(ctx context.Context, config UnpackConfig) (resultErr error) {
	started := time.Now()
	observe.Emit(ctx, config.Observer, observe.OperationStarted{Operation: unpackOperationName})
	defer func() {
		observe.Emit(ctx, config.Observer, observe.OperationCompleted{
			Operation: unpackOperationName,
			Duration:  time.Since(started),
			Err:       resultErr,
		})
	}()

	if err := ctx.Err(); err != nil {
		return fmt.Errorf("unpack: %w", err)
	}
	if err := config.Validate(); err != nil {
		return err
	}
	return archive.Extract(ctx, archive.ExtractConfig{
		ArchivePath:     config.ArchivePath,
		OutputDirectory: config.OutputDirectory,
		Identity:        config.Identity,
		Observer:        config.Observer,
	})
}

func Keygen(config KeygenConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	public, private, err := archive.GenerateKeyPair()
	if err != nil {
		return err
	}
	return archive.WriteKeyPair(
		config.PrivateKeyPath,
		private,
		config.PublicKeyPath,
		public,
	)
}
