package config

import (
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
)

type Config struct {
	Rabbit RabbitConfig `json:"rabbit"`
	Tasks  []Task       `json:"tasks"`
}

type Task struct {
	InDir  string `json:"in-dir"`
	OutDir string `json:"out-dir"`
	ErrDir string `json:"err-dir"`
	Queue  string `json:"queue"`
}

type RabbitConfig struct {
	Host string `json:"host"`
	Port string `json:"port"`
	User string `json:"user"`
	Pass string `json:"pass"`
}

func Load(file string) (*Config, error) {
	var config Config
	configFile, err := os.Open(file)
	if err != nil {
		return nil, err
	}

	jsonParser := json.NewDecoder(configFile)
	err = jsonParser.Decode(&config)
	if err != nil {
		return nil, err
	}

	err = configFile.Close()
	if err != nil {
		return nil, err
	}

	return &config, config.checkDirs()
}

func (conf *Config) checkDirs() error {
	if len(conf.Tasks) == 0 {
		return errors.New("no tasks in config")
	}

	for _, t := range conf.Tasks {
		err := checkDir(t.InDir)
		if err != nil {
			return err
		}
		err = checkDir(t.OutDir)
		if err != nil {
			return err
		}
		err = checkDir(t.ErrDir)
		if err != nil {
			return err
		}
	}

	return nil
}

func checkDir(s string) error {
	info, err := os.Stat(s)
	if os.IsNotExist(err) {
		err = os.MkdirAll(s, 0755)
		if err != nil {
			return errors.Wrap(err, "create dir failed")
		}
		info, err = os.Stat(s)
	}

	if err != nil {
		return err
	}

	if !info.IsDir() {
		return errors.New("path isn't a directory: " + s)
	}

	err = ioutil.WriteFile(s+"/test", []byte("Hi\n"), 0666)
	if err != nil {
		return errors.Wrap(err, "write temp file failed")
	}

	err = os.Remove(s + "/test")
	if err != nil {
		return errors.Wrap(err, "delete temp file failed")
	}

	return nil
}

func (conf *Config) GetQueues() []string {
	queues := make([]string, 0, len(conf.Tasks))
	for _, t := range conf.Tasks {
		queues = append(queues, t.Queue)
	}
	return queues
}
