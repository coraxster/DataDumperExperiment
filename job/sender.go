package job

type Sender interface {
	Process([]Job)
}
