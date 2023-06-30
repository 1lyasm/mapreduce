package common

const (
	IpAddr string = "192.168.1.7"
	Port   string = "1234"
)

func MakeFailMsg(funcName string, e error) string {
	return funcName + " failed: " + e.Error()
}

type ArgType struct {
	Arg int
}
type ReplyType struct {
	Reply int
}
