package job

import (
	"github.com/coraxster/DataDumper/config"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

type Job struct {
	Path   string
	T      *config.Task
	S      Status
	f      *os.File
	errors []error
}

type Status int

const (
	StatusUnlocked = iota // default state
	StatusLocked
	StatusSuccess
	StatusFailed
	StatusError
)

func (j *Job) Prepare() error {
	var err error
	j.f, err = os.OpenFile(j.Path, os.O_RDWR, os.ModeExclusive)
	j.S = StatusLocked
	if err != nil {
		j.S = StatusError
		j.errors = append(j.errors, err)
	}
	return err
}

func (j *Job) Bytes() (b []byte, err error) {
	stat, err := j.f.Stat()
	if err != nil {
		j.S = StatusError
		err = errors.Wrap(err, "file get info failed")
		j.errors = append(j.errors, err)
		return
	}

	b = make([]byte, stat.Size())

	if stat.Size() == 0 {
		return b, nil
	}

	_, err = j.f.Read(b)
	if err != nil {
		j.S = StatusError
		err = errors.Wrap(err, "file read failed")
		j.errors = append(j.errors, err)
		b = nil
	}
	return
}

func (j *Job) Finish() error {
	if j.S != StatusSuccess && j.S != StatusFailed && j.S != StatusLocked {
		return nil
	}
	err := j.f.Close()
	if err != nil {
		j.S = StatusError
		j.errors = append(j.errors, err)
		return err
	}
	var newPath string
	if j.S == StatusSuccess {
		newPath = j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
	}
	if j.S == StatusFailed {
		newPath = j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
	}
	if newPath != "" {
		err = os.Rename(j.Path, newPath)
	}
	if err != nil {
		j.S = StatusError
		j.errors = append(j.errors, err)
	}
	return err
}

func (j *Job) Error() string {
	var s string
	for _, e := range j.errors {
		s = s + ", " + e.Error()
	}
	return s
}

func Split(jobs []*Job, lim int) [][]*Job {
	var chunk []*Job
	chunks := make([][]*Job, 0, len(jobs)/lim+1)
	for len(jobs) >= lim {
		chunk, jobs = jobs[:lim], jobs[lim:]
		chunks = append(chunks, chunk)
	}
	if len(jobs) > 0 {
		chunks = append(chunks, jobs[:])
	}
	return chunks
}
