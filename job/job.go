package job

import (
	"github.com/coraxster/DataDumper/config"
	"github.com/pkg/errors"
	"io"
	"os"
	"path/filepath"
)

type Job interface {
	Bytes() ([]byte, error)
	Finish(bool) error
	GetQueue() string
	GetPath() string
}

type job struct {
	path string
	t    *config.Task
	f    *os.File
}

func MakeJob(path string, task *config.Task) *job {
	return &job{
		path: path,
		t:    task,
	}
}

func (j *job) GetPath() string {
	return j.path
}

func (j *job) GetQueue() string {
	return j.t.Queue
}

func (j *job) Bytes() (b []byte, err error) {
	j.f, err = os.OpenFile(j.path, os.O_RDWR, os.ModeExclusive)
	if err != nil {
		err = errors.Wrap(err, "file open failed")
		return
	}
	stat, err := j.f.Stat()
	if err != nil {
		err = errors.Wrap(err, "file get info failed")
		_ = j.f.Close()
		return
	}
	b = make([]byte, stat.Size())
	if stat.Size() == 0 {
		return
	}
	_, err = j.f.Read(b)
	if err == io.EOF {
		return b, nil
	}
	if err != nil {
		err = errors.Wrap(err, "file read failed")
		b = nil
		_ = j.f.Close()
	}
	return
}

func (j *job) Finish(result bool) error {
	err := j.f.Close()
	if err != nil {
		return err
	}
	var newPath string
	if result {
		newPath = j.t.OutDir + string(os.PathSeparator) + filepath.Base(j.path)
	} else {
		newPath = j.t.ErrDir + string(os.PathSeparator) + filepath.Base(j.path)
	}
	return os.Rename(j.path, newPath)
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
