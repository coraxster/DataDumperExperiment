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
}

func (j *Job) Bytes() (b []byte, err error) {
	f, err := os.OpenFile(j.Path, os.O_RDWR, os.ModeExclusive)
	if err != nil {
		return nil, err
	}
	defer func() {
		if fErr := f.Close(); fErr != nil {
			if err != nil {
				err = errors.New(err.Error() + fErr.Error())
			} else {
				err = fErr
			}
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		err = errors.New("File getting info failed. " + err.Error())
		return
	}

	if stat.Size() == 0 {
		return
	}

	b = make([]byte, stat.Size())
	_, err = f.Read(b)
	if err != nil {
		err = errors.New("File read failed. " + err.Error())
		b = nil
	}
	return
}

func (j *Job) Success() {
	newPath := j.T.OutDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
	}
}

func (j *Job) Failed() {
	newPath := j.T.ErrDir + string(os.PathSeparator) + filepath.Base(j.Path)
	err := os.Rename(j.Path, newPath)
	if err != nil {
		log.Println("File move failed. ", err.Error())
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
