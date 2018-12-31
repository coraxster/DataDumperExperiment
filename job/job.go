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
	if err != nil {
		j.S = StatusError
		err = errors.New("file lock/open failed. " + err.Error())
		return
	}

	defer func() {
		j.S = StatusPrepared
		if err != nil {
			j.Failed()
		}
	}()

	stat, err := j.f.Stat()
	if err != nil {
		err = errors.New("file get info failed. " + err.Error())
		return
	}

	if stat.Size() == 0 {
		err = errors.New("got empty file: " + j.Path)
		return
	}

	b = make([]byte, stat.Size())
	_, err = j.f.Read(b)
	if err != nil {
		err = errors.New("file read failed. " + err.Error())
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
