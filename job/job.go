package job

import (
	"github.com/coraxster/DataDumper/config"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
)

type Job interface {
	Prepare() error
	Bytes() ([]byte, error)
	Finish() error
	Error() string
	SetStatus(Status)
	GetStatus() Status
	GetQueue() string
	GetPath() string
}

type Status int

const (
	StatusUnlocked = iota // default state
	StatusLocked
	StatusSuccess
	StatusFailed
	StatusError
)

type job struct {
	path   string
	t      *config.Task
	s      Status
	f      *os.File
	errors []error
}

func MakeJob(path string, task *config.Task) Job {
	return &job{
		path: path,
		t:    task,
	}
}

func (j *job) SetStatus(s Status) {
	j.s = s
}

func (j *job) GetStatus() Status {
	return j.s
}

func (j *job) GetPath() string {
	return j.path
}

func (j *job) GetQueue() string {
	return j.t.Queue
}

func (j *job) Prepare() error {
	var err error
	j.f, err = os.OpenFile(j.path, os.O_RDWR, os.ModeExclusive)
	j.s = StatusLocked
	if err != nil {
		j.s = StatusError
		j.errors = append(j.errors, err)
	}
	return err
}

func (j *job) Bytes() (b []byte, err error) {
	stat, err := j.f.Stat()
	if err != nil {
		j.s = StatusError
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
		j.s = StatusError
		err = errors.Wrap(err, "file read failed")
		j.errors = append(j.errors, err)
		b = nil
	}
	return
}

func (j *job) Finish() error {
	if j.s != StatusSuccess && j.s != StatusFailed && j.s != StatusLocked {
		return nil
	}
	err := j.f.Close()
	if err != nil {
		j.s = StatusError
		j.errors = append(j.errors, err)
		return err
	}
	var newPath string
	if j.s == StatusSuccess {
		newPath = j.t.OutDir + string(os.PathSeparator) + filepath.Base(j.path)
	}
	if j.s == StatusFailed {
		newPath = j.t.ErrDir + string(os.PathSeparator) + filepath.Base(j.path)
	}
	if newPath != "" {
		err = os.Rename(j.path, newPath)
	}
	if err != nil {
		j.s = StatusError
		j.errors = append(j.errors, err)
	}
	return err
}

func (j *job) Error() string {
	var s string
	for _, e := range j.errors {
		s = s + ", " + e.Error()
	}
	return s
}

func Split(jobs []Job, lim int) [][]Job {
	var chunk []Job
	chunks := make([][]Job, 0, len(jobs)/lim+1)
	for len(jobs) >= lim {
		chunk, jobs = jobs[:lim], jobs[lim:]
		chunks = append(chunks, chunk)
	}
	if len(jobs) > 0 {
		chunks = append(chunks, jobs[:])
	}
	return chunks
}
