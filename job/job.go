package job

import (
	"errors"
	"github.com/coraxster/DataDumper/config"
	"log"
	"os"
	"path/filepath"
)

type Job struct {
	Path string
	T    *config.Task
	S    Status
	f    *os.File
}

type Status int

const (
	StatusNew = iota // default state
	StatusPrepared
	StatusSuccess
	StatusFailed
	StatusError
)

func (j *Job) Prepare() (b []byte, err error) {
	j.f, err = os.OpenFile(j.Path, os.O_RDWR, os.ModeExclusive)
	defer func() {
		j.S = StatusPrepared
		if err != nil {
			j.S = StatusError
			fErr := j.f.Close()
			err = errors.New(err.Error() + fErr.Error())
		}
	}()
	if err != nil {
		return
	}

	stat, err := j.f.Stat()
	if err != nil {
		err = errors.New("File getting info failed. " + err.Error())
		return
	}

	b = make([]byte, stat.Size())
	_, err = j.f.Read(b)
	if err != nil {
		err = errors.New("File read failed. " + err.Error())
		b = nil
	}
	return
}

func (j *Job) Success() {
	j.S = StatusSuccess
	if err := j.f.Close(); err != nil {
		j.S = StatusError
		log.Println("[WARNING] File unlock failed. ", err.Error())
		return
	}
	newPath := j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		j.S = StatusError
		log.Println("[WARNING] File move failed. ", err.Error())
	}
}

func (j *Job) Failed() {
	j.S = StatusFailed
	if err := j.f.Close(); err != nil {
		j.S = StatusError
		log.Println("[WARNING] File unlock failed. ", err.Error())
		return
	}
	newPath := j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		j.S = StatusError
		log.Println("[WARNING] File move failed. ", err.Error())
	}
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
