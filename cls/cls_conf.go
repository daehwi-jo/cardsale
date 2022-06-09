package cls

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var CfgServers []CfgServer
var ConfDir string
var cls_Version string = "v----------^^^^^^^^^^----------v"
var Eth_card string
var SvrIdx int

const (
	SIGUSR1 = syscall.Signal(0xa)
	SIGUSR2 = syscall.Signal(0xc)
)

func SignalLog(binName string) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, SIGUSR1, SIGUSR2)

	go func() {
		for {
			sig := <-sigs
			switch sig {
			case SIGUSR1:
				setLoglevel(0)
				break
			case SIGUSR2:
				fileName := fmt.Sprintf("%s/log/%sLEVEL", ConfDir, binName)
				value, err := ioutil.ReadFile(fileName)
				if err != nil {
					fmt.Printf("[FAIL] [%s] is not existed\n", fileName)
					break
				}

				tmpValue := strings.TrimSuffix(string(value), "\n") // 끝에 LF 문자 있는 경우 제거

				//fmt.Printf("[INFO] logLevel : [%s]\n", string(tmpValue))

				logLevel, _ := strconv.Atoi(string(tmpValue))

				fmt.Printf("[INFO] logLevel is changed : [%d]\n", logLevel)
				setLoglevel(logLevel)
				break
			}
		}
	}()
}

func runname(s string) string {
	i := strings.LastIndex(s, "/")
	if i > 0 {
		s = s[i+1:]
	} else {
		i = strings.LastIndex(s, "\\")
		if i > 0 {
			s = s[i+1:]
		}
	}

	s = strings.Replace(s, ".", "", -1)
	return s
}

// Windows 인 경우 Program Files가 띄어쓰기가 들어가므로 conf 경로가 짤림.
func confDirString(args []string) string {
	var length int = len(args)
	var dir string

	for i := 2; i < length; i++ {
		// log 옵션이나 background mark 이면 추가 하지 않음
		if args[i] == "-L" || args[i] == "-l" || args[i] == "&" {
			break
		}

		if i == 2 {
			dir = fmt.Sprintf("%s", args[i])
		} else if i > 2 && args[i] != "-e" {
			dir += fmt.Sprintf(" %s", args[i])
		} else {
			break
		}
	}

	return dir
}

func Cls_conf(args []string) string {
	var fname, eFname string
	//var fname string
	var lname string

	//args := os.Args
	binName := runname(args[0])

	if len(args) > 1 && args[1] == "-d" {
		ConfDir = confDirString(args)

		fmt.Printf("[INFO] ConfDir (%s)\n", ConfDir)

		//ConfDir = args[2]
		//fname = fmt.Sprintf("%s/%s.ini", args[2], binName)
		//fname = fmt.Sprintf("%s/conf/%s.ini", args[2], binName)
		//lname = fmt.Sprintf("%s/log/%sLOG", args[2], binName)
		// 운영 환경
		if len(os.Getenv("SERVER_TYPE")) > 0{
			fname = fmt.Sprintf("%s/conf/%s.ini.prod", ConfDir, binName)
		}else{
			fname = fmt.Sprintf("%s/conf/%s.ini", ConfDir, binName)
		}
		lname = fmt.Sprintf("%s/log/%sLOG", ConfDir, binName)

		eFname = fmt.Sprintf("%s/conf/%s.inc", ConfDir, binName)

		if _, err := os.Stat(fname); !os.IsNotExist(err) { // file exists
			fmt.Printf("[INFO] CREATE ENC CONF (%s)\n", eFname)
			src, _ := ioutil.ReadFile(fname)
			ioutil.WriteFile(eFname, []byte(EEncode(src)), 0644)

			v, r := GetTokenValue("CONF_DEL", eFname)
			if r != CONF_ERR && v == "1" {
				fmt.Printf("[INFO] DEL CONF (%s)\n", v)
				os.Remove(fmt.Sprintf("%s/conf/%s.ini.prod", ConfDir, binName))
				os.Remove(fmt.Sprintf("%s/conf/%s.ini", ConfDir, binName))
			}
		}
		fname = eFname

	} else if len(args) > 1 && args[1] == "-v" {
		fmt.Printf("[INFO] %s verison is %s\n", binName, cls_Version)
		os.Exit(0)

	} else {
		fmt.Println("[FAIL] -d [path] : 데몬 기동 path - 데몬 경로 입력")
		os.Exit(0)
		//fname = fmt.Sprintf("%s.ini", binName)
		fname = fmt.Sprintf("conf/%s.ini", binName)
		lname = fmt.Sprintf("log/%sLOG", binName)
	}

	if len(args) > 3 && (args[3] == "-L" || args[3] == "-l") {
		level, err := strconv.Atoi(strings.TrimSpace(args[4]))
		if err != nil {
			fmt.Println("[ERROR] level setting error : ", err)
			return fname
		}
		setLoglevel(level)
	} else {
		logLevel := os.Getenv("LOG_ON")
		if len(logLevel) > 0{
			level, _ := strconv.Atoi(strings.TrimSpace(logLevel))
			setLoglevel(level)
			fmt.Printf("[INFO] LOG LEVEL (%s)\n", logLevel)
		}else{
			// 주석처리 되어 있는 sys_conf에서 설정했던 로그레벨 설정
			v, r := GetTokenValue("LOG_ON", fname)
			if r != CONF_ERR {
				level, _ := strconv.Atoi(strings.TrimSpace(v))
				setLoglevel(level)
				fmt.Printf("[INFO] LOG LEVEL (%s)\n", v)
			} else {
				setLoglevel(4)
				fmt.Printf("[INFO] LOG LEVEL (4)\n")
			}
		}
	}

	if len(args) > 5 && args[5] == "-e" {
		if args[4] == "&" {
			fmt.Print("[ERROR] input ethernet card name\n")
			os.Exit(1)
		}

		if len(args) > 6 && strings.TrimSpace(args[5]) != "&" { //한국말일 경우 띄어쓰기가 있음 (ex. 이더넷 2)
			Eth_card = strings.TrimSpace(args[4]) + " " + strings.TrimSpace(args[5])
		} else { //ex. eth0
			Eth_card = strings.TrimSpace(args[4])
		}
	}

	NewLog(lname)

	//NewStat(binName)

	NewSentry(binName)

	//sys_conf(fname)

	SignalLog(binName)

	return fname
}
