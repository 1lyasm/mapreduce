package common

const (
	IpAddr string = "127.0.0.1"
	Port   string = "1234"
)

func MakeFailMsg(funcName string) string {
	return funcName + " failed"
}
