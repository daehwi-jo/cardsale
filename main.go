package main

import (
	"cardsales/cls"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/jasonlvhit/gocron"
)

var fname string
var serID, serTy string
var lprintf func(int, string, ...interface{}) = cls.Lprintf
var searchTypeStr = map[int]string{
	ONE: "ONE",
	MON: "MON",
	WEK: "WEK",
	RTY: "RTY",
	POD: "POD",
	NEW: "NEW",
}

func main() {
	fname = cls.Cls_conf(os.Args)
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	lprintf(3, "** start cardsales scrapping : fname(%s)\n", fname)
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	// DB connect
	ret := cls.Db_conf(fname)
	if ret < 0 {
		lprintf(1, "[ERROR] DB connection error\n")
		return
	}
	defer cls.DBc.Close()

	//callDaySum()
	//callDaySumPlus()
	//return

	// config value - container 환경변수로 처리
	serID = os.Getenv("SERVER_ID")
	if len(serID) == 0 {
		// SERVER ID setting
		id, r := cls.GetTokenValue("SERVER_ID", fname)
		if r == cls.CONF_ERR {
			lprintf(1, "[ERROR] SERVER_ID not exist value\n")
			return
		}
		serID = id
	}
	lprintf(4, "[INFO] SERVER_ID : %s", serID)

	// config value - container 환경변수로 처리
	serPort := os.Getenv("SERVER_PORT")
	if len(serPort) == 0 {
		sp, r := cls.GetTokenValue("SERVER_PORT", fname)
		if r == cls.CONF_ERR {
			lprintf(1, "[ERROR] SERVER_PORT not exist value\n")
			return
		}

		serPort = sp
	}
	lprintf(4, "[INFO] SERVER_PORT : %s", serPort)

	// config value - container 환경변수로 처리
	serTy = os.Getenv("SERVER_TY")
	if len(serTy) == 0 {
		// SERVER TYPE ID setting
		ty, r := cls.GetTokenValue("SERVER_TY", fname)
		if r == cls.CONF_ERR {
			lprintf(1, "[ERROR] SERVER_TY not exist value\n")
			return
		}
		serTy = ty
	}
	lprintf(4, "[INFO] SERVER_TY : %s", serTy)

	// SCHEDULER setting
	sch, r := cls.GetTokenValue("SCHEDULER", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] SCHEDULER not exist value\n")
		return
	}
	wSchd, r := cls.GetTokenValue("WEEKSCHED", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] WEEKSCHED not exist value\n")
		return
	}
	mSchd, r := cls.GetTokenValue("MONSCHED", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] MONSCHED not exist value\n")
		return
	}
	ySchd, r := cls.GetTokenValue("YEARSCHED", fname)
	if r == cls.CONF_ERR {
		lprintf(4, "[ERROR] YEARSCHED not exist value\n")
	}
	cSchd, r := cls.GetTokenValue("CHANNEL", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] CHANNEL not exist value\n")
		return
	}
	dSchd, r := cls.GetTokenValue("DAYSUMSCHED", fname)
	if r == cls.CONF_ERR {
		lprintf(1, "[ERROR] DAYSUMSCHED not exist value\n")
		return
	}

	// 10~11시에 2번, 20~21시에 2번 (하루 수집)
	schedules := strings.Split(sch, ",")
	g := gocron.NewScheduler()
	for _, schedule := range schedules {
		g.Every(1).Day().At(schedule).Do(collect, ONE, "", "", POD)
	}

	// 10~20시 매 시간 한번씩 (일주일 수집)
	weekSchd := strings.Split(wSchd, ",")
	for _, schedule := range weekSchd {
		g.Every(1).Day().At(schedule).Do(collect, WEK, "", "", POD)
	}

	// 신규 가입자
	// 9~23시 매 시간 정각 혹은 30분에 한번씩 (지난달 1일부터 수집)
	monSchd := strings.Split(mSchd, ",")
	for _, schedule := range monSchd {
		g.Every(1).Day().At(schedule).Do(collect, MON, "", "", NEW)
	}

	// 신규 가입자
	// 새벽 1:30, 3:30, 5:30에 작년 1월부터 지지난달 말일까지 수집
	if len(ySchd) > 0 {
		yaerSchd := strings.Split(ySchd, ",")
		for _, schedule := range yaerSchd {
			g.Every(1).Day().At(schedule).Do(collect, YEAR, "", "", NEW_TEST)
		}
	}

	channelSchd := strings.Split(cSchd, ",")
	for _, schedule := range channelSchd {
		g.Every(1).Day().At(schedule).Do(callChannel)
	}

	// 0:40분, 10:40, 15:40, 20:40분에 정기적으로 sum
	daySumSchd := strings.Split(dSchd, ",")
	for _, schedule := range daySumSchd {
		g.Every(1).Day().At(schedule).Do(callDaySum)
	}

	g.Start()
	defer g.Clear()

	http.HandleFunc("/reCollects", reCollects)
	http.HandleFunc("/reCollect", reCollect)
	http.HandleFunc("/collect", callCollect)
	http.HandleFunc("/yearCollect", yaerCollect)
	http.HandleFunc("/collectweek", weekCollects)
	http.HandleFunc("/reNewMember", reNewMember)

	// SERVER setting
	// serIP, r := cls.GetTokenValue("SERVER_IP", fname)
	// if r == cls.CONF_ERR {
	// 	lprintf(1, "[ERROR] SERVER_IP not exist value\n")
	// 	return
	// }

	// err := http.ListenAndServe(fmt.Sprintf("localhost:%s", serPort), nil)
	// err := http.ListenAndServe(fmt.Sprintf("%s:%s", serIP, serPort), nil)
	err := http.ListenAndServe(fmt.Sprintf(":%s", serPort), nil)
	if err != nil {
		lprintf(1, "[ERROR] ListenAndServe error(%s) \n", err.Error())
		return
	}
}

func reNewMember(w http.ResponseWriter, r *http.Request) {
	// restID := r.URL.Query().Get("restId")
	restID := r.FormValue("restId")
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> newMember START .... [%s] << \n", restID)
	if len(restID) == 0 || len(bsDt) == 0 {
		lprintf(1, "[ERROR]Required parameter missing\n")
		http.Error(w, "Required parameter missing", http.StatusBadRequest)
	} else {
		ret := collect(MON, restID, bsDt, NEW)
		if ret <= 0 {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
		} else {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
		}
	}

	lprintf(3, ">> newMember END .... [%s] << \n", restID)
}

// 지정가맹점의 요청일자 기준 7일 데이터 수집
// param(필수): restId, bsDt
func reCollects(w http.ResponseWriter, r *http.Request) {
	// restID := r.URL.Query().Get("restId")
	// bsDt := r.URL.Query().Get("bsDt")
	restID := r.FormValue("restId")
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> reCollects START .... [%s:%s] << \n", restID, bsDt)
	if len(restID) == 0 || len(bsDt) == 0 {
		lprintf(1, "[ERROR]Required parameter missing\n")
		http.Error(w, "Required parameter missing", http.StatusBadRequest)
	} else {
		ret := collect(WEK, restID, bsDt, RTY)
		if ret <= 0 {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
		} else {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
		}
	}
	lprintf(3, ">> reCollects END .... [%s:%s] << \n", restID, bsDt)
}

// 지정가맹점의 요청일자 1일 데이터 수집
// param(필수): restId, bsDt
func reCollect(w http.ResponseWriter, r *http.Request) {
	// restID := r.URL.Query().Get("restId")
	// bsDt := r.URL.Query().Get("bsDt")
	restID := r.FormValue("restId")
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> reCollect START .... [%s:%s] << \n", restID, bsDt)
	if len(restID) == 0 || len(bsDt) == 0 {
		lprintf(1, "[ERROR]Required parameter missing\n")
		http.Error(w, "Required parameter missing", http.StatusBadRequest)
	} else {
		ret := collect(ONE, restID, bsDt, RTY)
		if ret <= 0 {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
		} else {
			fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
		}
	}
	lprintf(3, ">> reCollect END .... [%s:%s] << \n", restID, bsDt)
}

// 기본 7일 데이터 수집
// param(필수): restId, bsDt
func weekCollects(w http.ResponseWriter, r *http.Request) {
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> weekCollects START .... [%s] << \n", bsDt)

	ret := collect(WEK, "", bsDt, POD)
	if ret == 0 {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
	} else {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
	}
	lprintf(3, ">> weekCollects END ....  << \n")
}

// 기본데이터 수집
// param(선택): bsDt(없는 경우 전날자 수집)
func callCollect(w http.ResponseWriter, r *http.Request) {
	bsDt := r.FormValue("bsDt")
	lprintf(3, ">> callCollect START .... [%s] << \n", bsDt)

	ret := collect(ONE, "", bsDt, POD)
	if ret == 0 {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
	} else {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
	}
	lprintf(3, ">> callCollect END ....  << \n")
}

// param(선택): bsDt(없는 경우 전날자 수집)
func yaerCollect(w http.ResponseWriter, r *http.Request) {
	restId := r.FormValue("restId")

	lprintf(3, ">> yaerCollect START .... << \n")

	ret := collect(YEAR, restId, "", YEAR)
	if ret == 0 {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusBadRequest, ret)
	} else {
		fmt.Fprintf(w, "{\"code\":\"%d\",\"cnt\":\"%d\"}\n", http.StatusOK, ret)
	}
	lprintf(3, ">> yaerCollect END ....  << \n")
}

func collect(searchTy int, restID, reqDt string, retryType int) int {
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
	lprintf(4, ">> collect START .... [%s:%s:%s:%s] << \n", searchTypeStr[searchTy], searchTypeStr[retryType], restID, reqDt)

	// 수집일자
	today := time.Now().Format("20060102")
	var bsDt string
	if len(reqDt) > 0 {
		bsDt = reqDt
	} else {
		bsDt = time.Now().AddDate(0, 0, -1).Format("20060102")
	}

	// 21시가 지났거나 신규 가맹점은 수집 후 +1일에 전송
	sendDt := today
	nowTime := time.Now().Format("150405")
	if retryType == NEW || nowTime > "21" {
		sendDt = time.Now().AddDate(0, 0, 1).Format("20060102")
	}

	lprintf(3, "[INFO] 오늘=%s, 조회일=%s, 전송일=%s\n", today, bsDt, sendDt)

	// 날짜변경을 위해 조회기준일을 Time 값으로 변경
	timeBsDt, err := time.Parse("20060102", bsDt)
	if err != nil {
		lprintf(1, "[ERROR] time.Parse (%s) \n", err.Error())
		return -1
	}

	// 데이터를 수집할 가맹점정보 가져오기
	var compInfors []CompInfoType
	if len(restID) == 0 {
		if retryType == NEW {
			// 신규 가맹점 수집인 경우는 오늘 가입한 가맹점만 돈다. -> 수집 날짜가 28일 미만인 가맹점 색출
			compInfors = getCompInfosNew(serID, bsDt, today)
		} else if retryType == NEW_TEST {
			// 신규 가맹점 작년 1월부터 지지난달 데이터 수집 테스트
			compInfors = getCompInfosNew2(serID, bsDt)
		} else {
			// 기존 가맹점 수집
			compInfors = getCompInfos(serID, bsDt)
		}

		if len(compInfors) == 0 {
			lprintf(4, "[INFO] getCompInfo: not found company info \n")
			return -2
		}
	} else {
		if retryType == NEW {
			// 특정 신규 가맹점 재 수집인 경우는 오늘 가입한 가맹점만 돈다.
			sendDt = "------" // 전송일을 특정 할 수 있게 해준다. (나중에 DB에서 변경)
			today = bsDt      // 인자로 받은 날자를 조회일로 한다.
			compInfors = getCompInfosByRestIDNew(restID, bsDt, today)
		} else if retryType == YEAR {
			// 올해 1월부터 수집
			compInfors = getCompInfosYear(serID, restID)
		} else {
			compInfors = getCompInfosByRestID(restID, bsDt)
		}
		if len(compInfors) == 0 {
			lprintf(4, "[INFO] getCompInfosByRestID: not found company info (rest=%s) \n", restID)
			return -3
		}
	}
	lprintf(4, "[INFO] 가맹점정보 (%d건)(%v) \n", len(compInfors), compInfors)

	// 수집일수 체크
	var searchDay int
	if searchTy == MON { //	월간 조회 전달 1일 부터 수집
		currentYear, currentMonth, _ := timeBsDt.Date()
		firstOfMonth := time.Date(currentYear, currentMonth, 1, 0, 0, 0, 0, timeBsDt.Location())
		firstOfMonth = firstOfMonth.AddDate(0, -1, 0)
		diff := timeBsDt.Sub(firstOfMonth)
		searchDay = int(diff.Hours()/24) + 1
		lprintf(4, "[INFO] firstOfMonth(%s)~timeBsDt(%s)=search(%d일) \n", firstOfMonth.Format("2006-01-02 15:04:05"), timeBsDt.Format("2006-01-02 15:04:05"), searchDay)
	} else if searchTy == YEAR {

		// 작년도 1월부터 지지난달 말일 까지 수집
		/*
			currentYear, currentMonth, currentDay := timeBsDt.Date()
			firstOfMonth := time.Date(currentYear-1, 1, 1, 0, 0, 0, 0, timeBsDt.Location())
			timeBsDt = time.Date(currentYear, currentMonth-1, currentDay, 0, 0, 0, 0, timeBsDt.Location())
			timeBsDt = timeBsDt.AddDate(0,0,-currentDay)
		*/

		currentYear, currentMonth, currentDay := timeBsDt.Date()

		// 올해 1월 1일 부터 수집
		firstOfMonth := time.Date(currentYear, 1, 1, 0, 0, 0, 0, timeBsDt.Location())
		// 작년 7월 1일 부터 수집
		//firstOfMonth := time.Date(currentYear-1, 7, 1, 0, 0, 0, 0, timeBsDt.Location())

		//timeBsDt = time.Date(currentYear, 3, 31, 0, 0, 0, 0, timeBsDt.Location())

		// 지지난달 말일 까지
		timeBsDt = time.Date(currentYear, currentMonth-1, currentDay, 0, 0, 0, 0, timeBsDt.Location())
		timeBsDt = timeBsDt.AddDate(0, 0, -currentDay)

		diff := timeBsDt.Sub(firstOfMonth)
		searchDay = int(diff.Hours()/24) + 1
		lprintf(4, "[INFO] firstOfMonth(%s)~endOfMonth(%s)=search(%d일) \n", firstOfMonth.Format("2006-01-02 15:04:05"), timeBsDt.Format("2006-01-02 15:04:05"), searchDay)
	} else if searchTy == WEK {
		searchDay = 7
	} else {
		searchDay = 1
	}

	var serverStatus bool
	//	wg := sync.WaitGroup{}
	for idx, compInfo := range compInfors {
		goID := idx
		comp := compInfo
		//	wg.Add(1) // WaitGroup의 고루틴 개수 1 증가
		//	go func() {
		//		defer wg.Done()

		// 수집할 날자 리스트 만듬
		var dateList []string
		startDt := timeBsDt.AddDate(0, 0, -(searchDay - 1)).Format("20060102")
		endDt := bsDt

		// 과거 일부터 수집 시작
		for i := searchDay - 1; i >= 0; i-- {
			tmpsDt := timeBsDt.AddDate(0, 0, -(i)).Format("20060102")
			dateList = append(dateList, tmpsDt)
		}

		// 매일 당일 조회는 이전 데이터수집 결과 조회 (오늘 돌았던 적이 있고, 정상이면 수집 안함).
		if retryType == POD {
			syncInfos := selectSync(goID, comp.BizNum, startDt, endDt)
			lprintf(4, "[INFO][go-%d] syncInfos=%v \n", goID, syncInfos)
			if searchTy == ONE {
				// 이전 결과 상태 체크
				if syncInfos[bsDt].StsCd != "2" && len(syncInfos[bsDt].StsCd) != 0 {
					// 오늘 수집 정상 SKIP
					lprintf(4, "[INFO][go-%d] today collect success already (%s)\n", goID, comp.BizNum)
					continue // return
				}
			} else if searchTy == WEK || searchTy == MON {
				// 주간/월간 조회시 오늘 업데이트 또는 인서트 되어서 정상 결과를 가진 것은 제외
				var newDateList []string
				for _, eachDay := range dateList {
					if syncInfos[eachDay].ErrCd != "0000" ||
						(!strings.HasPrefix(syncInfos[eachDay].ModDt, today) &&
							!strings.HasPrefix(syncInfos[eachDay].RegDt, today)) {
						newDateList = append(newDateList, eachDay)
					}
				}
				dateList = newDateList
			}
		}
		lprintf(4, "[INFO][go-%d] search dateList (%v)\n", goID, dateList)
		if len(dateList) == 0 {
			lprintf(4, "[INFO][go-%d] today collect success already (%s)\n", goID, comp.BizNum)
			continue
		}

		// cardsales server status check
		if !serverStatus {
			if serverHealth() < 0 {
				lprintf(1, "[ERROR][go-%d] server connect fail\n", goID)
				sync := SyncInfoType{comp.BizNum, strings.ReplaceAll(bsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", CcErrHttp, ""}
				insertSync(goID, sync)
				serverStatus = false
				continue // return
			} else {
				serverStatus = true
			}
		}

		// login
		resp, err := login(goID, comp.LnID, comp.LnPsw)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] login fail (%s)\n", goID, err.Error())
			// Sync 결과 저장 (login 오류, 조회 시작 기준 일로)
			sync := SyncInfoType{comp.BizNum, strings.ReplaceAll(bsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", CcErrLogin, ""}
			insertSync(goID, sync)
			serverStatus = false
			continue // return
		}
		cookie := resp.Cookie
		lprintf(4, "[INFO][go-%d] login succes wait 100 ms(%v) \n", goID, comp.LnID)

		// grpId가 여러개일 경우(한명이 여러 사업자를 가진 경우) 처리 추가 필요함
		grpIds, err, newCookie := getGrpId(goID, resp.Cookie, comp)
		cookie = newCookie
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getGrpId (%s) \n", goID, err.Error())
			sync := SyncInfoType{comp.BizNum, strings.ReplaceAll(bsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", CcErrGrpId, ""}
			insertSync(goID, sync)
			continue // return
		}
		lprintf(4, "[INFO][go-%d] getGrpId (%v) \n", goID, grpIds)

		var result int
		if result = getSalesData(&dateList, goID, comp, grpIds[0].Code, cookie, sendDt); result == ERROR {
			// 2초만 휴식 빠르게 재시도 후 다음 기회를 노리자
			time.Sleep(2 * time.Second)
			lprintf(4, "[INFO][go-%d] retry search dateList (%v)\n", goID, dateList)
			result = getSalesData(&dateList, goID, comp, grpIds[0].Code, cookie, sendDt)
		}

		// 최종 경과가 실패이면 다음 주기를 기다림
		if result == ERROR {
			continue // return
		}

		// 주기 호출인 경우, 오늘자 조회 성공이 면 가맹점 Push
		if retryType == POD && searchTy == ONE {
			// 가맹점 push
			ok := checkPushState(goID, comp.BizNum, bsDt)
			if ok && len(os.Getenv("SERVER_TYPE")) > 0 {
				lprintf(4, "[INFO][go-%d] send Push:(%s) \n", goID, comp.BizNum)
				updatePushState(goID, comp.BizNum, bsDt)
				//pushURI := "DaRaYo/api/common/commonPush.json?userId=" + comp.BizNum + "&userTy=5&msgCode=5002"
				//cls.HttpRequest("HTTP", "GET", "api.darayo.com", "80", pushURI, true)
				pushURI := "api/cash/commons/pushMsg?pushType=M&menu=home&userType=2&bizNum=" + comp.BizNum + "&msgCode=5002"
				cls.HttpRequest("HTTPS", "GET", "cashapi.darayo.com", "7788", pushURI, true)
			}
		}

		if comp.LnFirstYn == "N" {
			updateCompInfo(goID, comp.BizNum)
			comp.LnFirstYn = "Y"
		}
		//		}()
	}
	//	wg.Wait()

	sumCnt, retCnt := getResultCnt(bsDt, restID, serID)
	lprintf(4, ">> collect END.... [%s:%s:%s:%s][%d/%d] << \n", searchTypeStr[searchTy], searchTypeStr[retryType], restID, reqDt, sumCnt, len(compInfors))
	lprintf(3, "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")

	if retryType == RTY {
		return sumCnt
	}
	return retCnt

}

// channel push (결과)
func callChannel() {

	if len(os.Getenv("SERVER_TYPE")) == 0 {
		return
	}

	if strings.Compare(serTy, "TST") == 0 {
		lprintf(4, "[INFO] Do not send push to channel in TEST ")
		return
	}

	bsDt := time.Now().AddDate(0, 0, -1).Format("20060102")

	// 수집 대상 회사
	compInfors := getCompInfos(serID, bsDt)

	// 수집결과 체크
	sumCnt, _ := getResultCnt(bsDt, "", serID)

	if sumCnt == len(compInfors) {
		errMsg := fmt.Sprintf("[%s]매출데이터 수집 전체 성공 (%d/%d store)", serID, sumCnt, len(compInfors))
		sendChannel("전체 가맹점 수집 성공", errMsg, "655403")
	} else {
		// 비밀번호 실패 체크
		missPwd, _ := getResultCnt(bsDt, "miss", serID)

		errMsg := fmt.Sprintf("[%s]매출데이터 수집 상태 : 실패 (%d store) 중 비밀번호 오류 (%d store), 성공 (%d store)", serID, len(compInfors)-sumCnt, missPwd, sumCnt)
		sendChannel("수집 실패 가맹점 발생", errMsg, "655403")
	}
}

func getSalesData(dateList *[]string, goID int, comp CompInfoType, code string, cookies []*http.Cookie, sendDt string) int {
	bizNum := comp.BizNum
	var failDate []string
	for _, selBsDt := range *dateList {
		time.Sleep(1 * time.Second)
		lprintf(3, "[INFO][go-%d] 수집일=%s\n", goID, selBsDt)
		//////////////////////////////////////
		// 1.승인 내역 처리
		apprCnt, apprAmt, apprErrCd, newCookie := getApproval(goID, cookies, selBsDt, code, comp)
		if apprErrCd != CcErrNo && apprErrCd != CcErrNoData && apprErrCd != CcErrSameData {
			// Sync 결과 저장(오류)
			sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, "0", "0", "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", apprErrCd, ""}
			insertSync(goID, sync)
			failDate = append(failDate, selBsDt)
			continue
		}
		cookies = newCookie
		//////////////////////////////////////
		// 2.매입내역 처리
		pcaCnt, pcaAmt, pcaErrCd, newCookie := getPurchase(goID, cookies, selBsDt, code, comp)
		if pcaErrCd != CcErrNo && pcaErrCd != CcErrNoData && pcaErrCd != CcErrSameData {
			// Sync 결과 저장(오류)
			sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, apprCnt, apprAmt, "0", "0", "0", "0", time.Now().Format("20060102150405"), "", "2", pcaErrCd, ""}
			insertSync(goID, sync)
			failDate = append(failDate, selBsDt)
			continue
		}
		cookies = newCookie
		//////////////////////////////////////
		// 3.입금내역 처리
		payCnt, payAmt, payErrCd, newCookie := getPayment(goID, cookies, selBsDt, selBsDt, code, comp)
		if payErrCd != CcErrNo && payErrCd != CcErrNoData && payErrCd != CcErrSameData {
			// Sync 결과 저장(오류)
			sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, apprCnt, apprAmt, pcaCnt, pcaAmt, "0", "0", time.Now().Format("20060102150405"), "", "2", payErrCd, ""}
			insertSync(goID, sync)
			failDate = append(failDate, selBsDt)
			continue
		}

		//////////////////////////////////////
		// Sync 결과 저장(정상)
		lprintf(4, "[INFO][go-%d] success => %v \n", goID, selBsDt)
		sync := SyncInfoType{bizNum, strings.ReplaceAll(selBsDt, "-", ""), siteCd, apprCnt, apprAmt, pcaCnt, pcaAmt, payCnt, payAmt, time.Now().Format("20060102150405"), "", "1", CcErrNo, sendDt}
		// 과거와 변경이 없는 경우 업데이트를 하지 않아서, 금결원 파일 생성을 피함
		insertSync(goID, sync)

		lprintf(4, "[INFO][go-%d] failDate => %v \n", goID, failDate)
	}

	if len(failDate) != 0 {
		*dateList = failDate[:]
		return ERROR
	}

	return NOERR
}

func serverHealth() int {
	_, err := http.Get("https://www.cardsales.or.kr/")
	if err != nil {
		return -1
	}

	return 1
}

func login(goID int, loginId, password string) (*LoginResponse, error) {
	// lprintf(4, "[INFO][go-%d] loginId/password=[%s/%s]\n", goID, loginId, password)
	apiUrl := "https://www.cardsales.or.kr"
	resource := "/authentication"
	data := url.Values{}
	data.Set("j_username", loginId)
	data.Set("j_password", password)
	u, _ := url.ParseRequestURI(apiUrl)
	u.Path = resource
	urlStr := u.String()

	req, err := http.NewRequest("POST", urlStr, strings.NewReader(data.Encode()))
	if err != nil {
		lprintf(1, "[ERROR][go-%d] login: http NewRequest (%s) \n", goID, err.Error())
		return nil, err
	}

	req.Header.Set("Connection", "keep-alive")
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Add("Origin", "https://www.cardsales.or.kr")
	req.Header.Add("Referer", "https://www.cardsales.or.kr/signin")

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] login: http (%s) \n", goID, err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 302 {
		lprintf(4, "[INFO][go-%d] resp=(%s) \n", goID, resp)
		lprintf(1, "[ERROR][go-%d] login: http resp (%d) \n", goID, resp.StatusCode)
		err = errors.New("login http resp error")
		return nil, err
	}

	cookie := resp.Cookies()
	return &LoginResponse{Cookie: cookie}, nil
}

func getGrpId(goID int, cookie []*http.Cookie, comp CompInfoType) ([]GrpIdType, error, []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/api/commonCode/merGrpCode"
	referer := "https://www.cardsales.or.kr/signin"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return nil, err, cookie
	}
	cookie = newCookie
	defer respData.Body.Close()

	var grpIds []GrpIdType
	if respData.StatusCode == http.StatusOK {
		bodyBytes, err := ioutil.ReadAll(respData.Body)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getGrpId: ioutil.ReadAll (%s) \n", goID, err.Error())
			return nil, err, cookie
		}
		// lprintf(4, "[INFO][go-%d] bodyBytes (%s) \n", goID, bodyBytes)

		if err := json.Unmarshal(bodyBytes, &grpIds); err != nil { //json byte array를 , 다른객체에 넣어줌
			lprintf(1, "[ERROR][go-%d] getGrpId: req body unmarshal (%s) \n", goID, err.Error())
			lprintf(1, "[ERROR][go-%d] getGrpId: req body=(%s) \n", goID, bodyBytes)
			return nil, err, cookie
		}

		// lprintf(4, "[INFO][go-%d] grpIds (%v) \n", goID, grpIds)
	} else {
		err = fmt.Errorf("Http resp StatusCode(%d)", respData.StatusCode)
		return nil, err, cookie
	}

	return grpIds, nil, cookie
}

// 승인내역 합계 & 리스트
func getApproval(goID int, cookie []*http.Cookie, bsDt, grpId string, comp CompInfoType) (appCnt, appAmt, errCd string, ncookie []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/api/approval/dayListAjax?stdDate=" + bsDt + "&merGrpId=" + grpId + "&cardCo=&merNo="
	referer := "https://www.cardsales.or.kr/page/approval/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return "", "", CcErrHttp, cookie
	}
	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return "", "", CcErrHttp, cookie
	}

	cookie = newCookie
	bizNum := comp.BizNum
	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getApproval: response (%s)", goID, err)
		return "", "", CcErrHttp, cookie
	}

	var approvalSum ApprovalSumType
	if err := json.Unmarshal(bodyBytes, &approvalSum); err != nil { //json byte array를 , 다른객체에 넣어줌
		lprintf(1, "[ERROR][go-%d] getApproval: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getApproval: req body=(%s) \n", goID, bodyBytes)
		return "", "", CcErrParsing, cookie
	}
	lprintf(3, "[INFO][go-%d] getApproval: resp approval sum (%s:%d건)(%v) \n", goID, bizNum, len(approvalSum.ResultList), approvalSum)

	apprSum := selectApprSum(goID, bizNum, bsDt)
	if apprSum == nil {
		return "", "", CcErrDb, cookie
	}
	lprintf(4, "[INFO][go-%d] getApproval: db approval sum (%v) \n", goID, apprSum)
	if len(apprSum.TotTrnsCnt) > 0 && apprSum.compare(approvalSum.ResultSum) != 0 {
		// DB의 데이터와 수집데이터가 같은 경우, 정상 응답
		lprintf(4, "[INFO][go-%d] getApproval: db approval sum amt (%v) = (%v) \n", goID, apprSum.TotTrnsAmt, approvalSum.ResultSum.TotTrnsAmt)
		return approvalSum.ResultSum.TotTrnsCnt, approvalSum.ResultSum.TotTrnsAmt, CcErrSameData, cookie
	}
	// DB의 데이터와 새로 수집한 데이터가 다른 경우, 재수집 후 삭제 -> insert
	lprintf(3, "[INFO][go-%d] DB=%v\n", goID, *apprSum)
	lprintf(3, "[INFO][go-%d] WEB=%v\n", goID, approvalSum.ResultSum)

	tranCnt, err := strconv.Atoi(approvalSum.ResultSum.TotTrnsCnt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalSum.ResultSum.TotTrnsCnt:%s)", goID, approvalSum.ResultSum.TotTrnsCnt)
		return "", "", CcErrDataFormat, cookie
	}
	if tranCnt > 0 {
		// 승인내역 합계 DB저장
		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))

		var cardCoUri, amtUri, tcntUri string
		// 승인내역 합계 리스트
		for _, approvalList := range approvalSum.ResultList {
			cardCoUri = cardCoUri + "&q.cardCoArray=" + approvalList.CardCo
			amtUri = amtUri + "&amt=" + approvalList.TrnsAmt
			tcntUri = tcntUri + "&tcnt=" + approvalList.TrnsCnt
		}
		// 승인내역 전체 상세조회
		address = "https://www.cardsales.or.kr/page/api/approval/detailDayListAjax?q.mode=&q.flag=&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=&q.stdDate=" + bsDt

		// detail
		loopCnt := tranCnt / datePerPage
		if len(approvalSum.ResultList)%datePerPage != 0 {
			loopCnt = loopCnt + 1
		}
		var updateOrgData []ApprovalDetailType
		var detailCnt, detailAmt int
		address = address + cardCoUri + amtUri + tcntUri
		deleteDataTemp(goID, ApprovalTy, bizNum, bsDt)

		// 상세조회 페이징 처리
		for i := 1; i <= loopCnt; i++ {
			tmpAddr := address + fmt.Sprintf("&q.dataPerPage=%d&currentPage=%d", datePerPage, i)
			cnt, amt, errCd, newCookie := getApprovalDetail(goID, cookie, bsDt, tmpAddr, comp, &updateOrgData)
			if errCd != CcErrNo {
				// lprintf(1, "[ERROR][go-%d] getApproval: failed to get detail list \n", goID)
				// 오류 시 새로 작성한 데이터 삭제 후 리턴
				deleteDataTemp(goID, ApprovalTy, bizNum, bsDt)
				return "", "", errCd, cookie
			}
			cookie = newCookie

			detailCnt += cnt
			detailAmt += amt
		}

		// 승인내역 상세 리스트 기존것 삭제 후 DB저장
		deleteData(goID, ApprovalTy, bizNum, bsDt)
		moveData(goID, ApprovalTy, bizNum, bsDt)

		// detail 저장 후 승인합계 DB 저장 (detail 새로 저장시 기존거 지움)
		row := insertData(goID, ApprovalSum, paramStr, &approvalSum.ResultSum)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getApproval: sum failed to store DB \n", goID)
			return "", "", CcErrDb, cookie
		}

		// 통계용 월간 승인 내역 저장
		row = insertMonthData(goID, bizNum, bsDt)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getApproval: month sum failed to store DB \n", goID)
			return "", "", CcErrDb, cookie
		}

		for _, approvalList := range approvalSum.ResultList {
			// 승인내역 합계 리스트 DB저장
			paramStr := make([]string, 0, 5)
			paramStr = append(paramStr, bizNum)
			paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
			row := insertData(goID, ApprovalList, paramStr, &approvalList)
			if row < 0 {
				lprintf(1, "[ERROR][go-%d] getApproval: sum list failed to store DB \n", goID)
				return "", "", CcErrDb, cookie
			}
		}

		// 취소건일 경우 승인거래의 상태를 취소대응으로 변경 및 취소거래 원거래일자 업데이트
		if len(updateOrgData) > 0 {
			lprintf(3, "[INFO][go-%d] updateOrgData(%v) \n", goID, updateOrgData)
			for _, upData := range updateOrgData {
				lprintf(4, "[INFO][go-%d] upData(%v) \n", goID, upData)

				// 승인거래 상태변경
				fields := []string{"STS_CD"}
				wheres := []string{"BIZ_NUM", "APRV_NO", "CARD_NO", "APRV_CLSS"}
				values := []string{"2", bizNum, upData.AuthNo, upData.CardNo, "0"}
				// "update cc_aprv_dtl set STS_CD=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=0;"
				ret := updateDetail(goID, ApprovalDetail, fields, wheres, values)
				lprintf(3, "[INFO][go-%d] getApprovalDetail: sts_cd update (%d건)(%s,%s,%s) \n", goID, ret, bizNum, upData.AuthNo, upData.CardNo)

				// 취소거래 원거래일자 변경
				upData.OrgTrDt = getOrgTrDt(goID, bizNum, upData)
				fields = []string{"ORG_TR_DT"}
				wheres = []string{"BIZ_NUM", "APRV_NO", "CARD_NO", "APRV_CLSS"}
				values = []string{upData.OrgTrDt, bizNum, upData.AuthNo, upData.CardNo, "1"}
				// "update cc_aprv_dtl set ORG_TR_DT=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=1;"
				ret = updateDetail(goID, ApprovalDetail, fields, wheres, values)
				lprintf(3, "[INFO][go-%d] getApprovalDetail: org_tr_dt(%s) update (%d건)(%s,%s,%s) \n", goID, upData.OrgTrDt, ret, bizNum, upData.AuthNo, upData.CardNo)
			}
		}

		// 합계, 상세내역 비교
		sumCnt, err := strconv.Atoi(approvalSum.ResultSum.TotTrnsCnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalSum.ResultSum.TotTrnsCnt:%s)", goID, approvalSum.ResultSum.TotTrnsCnt)
			return "", "", CcErrDataFormat, cookie
		}
		sumAmt, err := strconv.Atoi(approvalSum.ResultSum.TotTrnsAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalSum.ResultSum.TotTrnsAmt:%s)", goID, approvalSum.ResultSum.TotTrnsAmt)
			return "", "", CcErrDataFormat, cookie
		}

		if sumCnt != detailCnt {
			lprintf(1, "[ERROR][go-%d] getApproval: Differ to Approval Count sum(%d):detail(%d) \n", goID, sumCnt, detailCnt)

			// 합계건수 수정
			fields := []string{"TOT_CNT"}
			wheres := []string{"BIZ_NUM", "BS_DT"}
			values := []string{strconv.Itoa(detailCnt), bizNum, bsDt}
			// "update cc_aprv_sum set TOT_CNT=? where BIZ_NUM=? and BS_DT=?;"
			ret := updateSum(goID, ApprovalSum, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getApproval: tot_cnt update (%d건)(%s,%s) \n", goID, ret, bizNum, bsDt)

			return "", "", CcErrApprCnt, cookie
		}

		if sumAmt != detailAmt {
			lprintf(1, "[ERROR][go-%d] getApproval: Differ to Approval Amount sum(%d):detail(%d) \n", goID, sumAmt, detailAmt)

			// 합계금액 수정
			fields := []string{"TOT_AMT"}
			wheres := []string{"BIZ_NUM", "BS_DT"}
			values := []string{strconv.Itoa(detailAmt), bizNum, bsDt}
			// "update cc_aprv_sum set TOT_AMT=? where BIZ_NUM=? and BS_DT=?;"
			ret := updateSum(goID, ApprovalSum, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getApproval: tot_amt update (%d건)(%s,%s) \n", goID, ret, bizNum, bsDt)

			return "", "", CcErrApprAmt, cookie
		}

		return approvalSum.ResultSum.TotTrnsCnt, approvalSum.ResultSum.TotTrnsAmt, CcErrNo, cookie
	}

	return "0", "0", CcErrNoData, cookie
}

// 승인내역 상세 리스트
func getApprovalDetail(goID int, cookie []*http.Cookie, bsDt, address string, comp CompInfoType, canList *[]ApprovalDetailType) (int, int, string, []*http.Cookie) {
	referer := "https://www.cardsales.or.kr/page/approval/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return -1, -1, CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return -1, -1, CcErrHttp, cookie
	}

	cookie = newCookie
	bizNum := comp.BizNum

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getApprovalDetail: response (%s)", goID, err)
		return -1, -1, CcErrHttp, cookie
	}

	var approvalDetailList []ApprovalDetailType
	if err := json.Unmarshal(bodyBytes, &approvalDetailList); err != nil { //json byte array를 , 다른객체에 넣어줌
		lprintf(1, "[ERROR][go-%d] getApprovalDetail: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getApprovalDetail: req body=(%s) \n", goID, bodyBytes)
		return -1, -1, CcErrParsing, cookie
	}
	lprintf(4, "[INFO][go-%d] getApprovalDetail: resp approval detail (%s:%d건)(%v) \n", goID, bizNum, len(approvalDetailList), approvalDetailList)

	yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")
	var sumAmt int
	for _, approvalDetail := range approvalDetailList {
		if strings.TrimSpace(approvalDetail.AuthClss) == "1" {
			approvalDetail.StsCd = "3"

			// 취소 후처리 해야하는 데이터
			*canList = append(*canList, approvalDetail)
		} else {
			approvalDetail.StsCd = "1"
		}

		// 과거데이터 오류로 재수집할 경우 매입결과를 확인한 후 pca_yn 값을 처리
		if yesterday != approvalDetail.TrnsDate {
			pcaRet := getPcaResult(goID, bizNum, approvalDetail)
			if pcaRet > 0 {
				approvalDetail.PcaYn = "Y"
			} else {
				approvalDetail.PcaYn = "N"
			}
		} else {
			approvalDetail.PcaYn = "N"
		}

		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
		row := insertData(goID, ApprovalDetail, paramStr, &approvalDetail)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getApprovalDetail: detail list failed to store DB \n", goID)
			return -1, -1, CcErrDb, cookie
		}

		tmpAmt, err := strconv.Atoi(approvalDetail.AuthAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getApproval: data format (approvalDetail.AuthAmt:%s) \n", goID, approvalDetail.AuthAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		sumAmt += tmpAmt
	}

	sumCnt := len(approvalDetailList)

	return sumCnt, sumAmt, CcErrNo, cookie

}

// 매입내역 합계 & 리스트 (html 통신)
func getPurchase(goID int, cookie []*http.Cookie, bsDt, grpId string, comp CompInfoType) (purCnt, purAmt, errCd string, ncookie []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/purchase/day?q.flag=&q.stdYear=&q.stdMonth=&q.pageType=&q.searchDateCode=PCA_DATE&selAmt=0&selCnt=0&oldAmt=&q.oldMerNo=&q.oldMerGrpId=&q.oldSearchDate=&q.oldCardCo=&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=&q.searchDate=" + bsDt + "&q.dataPerPage=20"
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return "", "", CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return "", "", CcErrHttpResp, cookie
	}

	bizNum := comp.BizNum
	cookie = newCookie

	doc, err := goquery.NewDocumentFromReader(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPurchase: goquery NewDocumentFromReader (%s) \n", goID, err.Error())
		return "", "", CcErrHttp, cookie
	}

	// parsing html
	// 합계
	var purchaseSum PurchaseSumType
	doc.Find("div.table_cell_footer").Find("tr.toptal td").Each(func(i int, s *goquery.Selection) {
		val := s.Text()
		switch i {
		case 0:
			break
		case 1: // 매입건수
			purchaseSum.ResultSum.PcaCnt = val
		case 2: // 매입합계
			purchaseSum.ResultSum.PcaScdAmt = strings.ReplaceAll(val, ",", "")
		case 3: // 가맹점 수수료
			purchaseSum.ResultSum.MerFee = strings.ReplaceAll(val, ",", "")
		case 4: // 포인트 수수료
			purchaseSum.ResultSum.PntFee = strings.ReplaceAll(val, ",", "")
		case 5: // 기타 수수료
			purchaseSum.ResultSum.EtcFee = strings.ReplaceAll(val, ",", "")
		case 6: // 수수료계
			purchaseSum.ResultSum.TotFee = strings.ReplaceAll(val, ",", "")
		case 7: // 부가세
			purchaseSum.ResultSum.VatAmt = strings.ReplaceAll(val, ",", "")
		case 8: // 지급예정합계
			purchaseSum.ResultSum.OuptExptAmt = strings.ReplaceAll(val, ",", "")
		default:
			lprintf(1, "[ERROR][go-%d] getPurchase:unknown field (%s) \n", goID, val)
			break
		}
	})
	// lprintf(3, "[INFO][go-%d] getPurchase: resp purchase sum (%s)(%v) \n", goID, bizNum, purchaseSum.ResultSum)

	pcaSum := selectPcaSum(goID, bizNum, bsDt)
	if pcaSum == nil {
		return "", "", CcErrDb, cookie
	}
	lprintf(4, "[INFO][go-%d] getPurchase: db purchase sum (%v) \n", goID, pcaSum)
	if len(pcaSum.PcaCnt) > 0 && pcaSum.compare(purchaseSum.ResultSum) != 0 {
		// DB의 데이터와 수집데이터가 같은 경우, 정상 응답
		lprintf(4, "[INFO][go-%d] getPurchase: db purchase sum same (%v) \n", goID, pcaSum)
		return purchaseSum.ResultSum.PcaCnt, purchaseSum.ResultSum.PcaScdAmt, CcErrSameData, cookie
	}
	// DB의 데이터와 수집데이터가 다른 경우, 삭제 후 재수집
	lprintf(4, "[INFO][go-%d] DB=%v\n", goID, *pcaSum)
	lprintf(4, "[INFO][go-%d] WEB=%v\n", goID, purchaseSum.ResultSum)

	cnt, err := strconv.Atoi(purchaseSum.ResultSum.PcaCnt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPurchase: data format (purchaseSum.ResultSum.PcaCnt:%s) \n", goID, purchaseSum.ResultSum.PcaCnt)
		lprintf(4, "[INFO] read data (%v)", doc)
		return "", "", CcErrDataFormat, cookie
	}
	if cnt > 0 {
		// 매입내역 합계 DB저장
		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))

		// 합계 상세조회
		tagBody := doc.Find("div.table_cell_body").Find("#tbodyMain")
		purchaseSum.ResultList = make([]ResultPurListType, tagBody.Find("tr#mainHistoryTr").Length())
		tagBody.Find("tr#mainHistoryTr td").Each(func(i int, s *goquery.Selection) {
			row := i / purItemCnt
			field := i % purItemCnt
			val := s.Text()
			switch field {
			case 0: // 카드사코드
				val, _ := s.Find("input").Attr("value")
				purchaseSum.ResultList[row].CardCo = val
			case 1: // 카드사명
				purchaseSum.ResultList[row].CardNm = val
			case 2: // 매입건수
				purchaseSum.ResultList[row].PcaCnt = val
			case 3: // 매입합계
				purchaseSum.ResultList[row].PcaScdAmt = strings.ReplaceAll(val, ",", "")
			case 4: // 가맹점 수수료
				purchaseSum.ResultList[row].MerFee = strings.ReplaceAll(val, ",", "")
			case 5: // 포인트 수수료
				purchaseSum.ResultList[row].PntFee = strings.ReplaceAll(val, ",", "")
			case 6: // 기타 수수료
				purchaseSum.ResultList[row].EtcFee = strings.ReplaceAll(val, ",", "")
			case 7: // 수수료계
				purchaseSum.ResultList[row].TotFee = strings.ReplaceAll(val, ",", "")
			case 8: // 부가세
				purchaseSum.ResultList[row].VatAmt = strings.ReplaceAll(val, ",", "")
			case 9: // 지급예정합계
				purchaseSum.ResultList[row].OuptExptAmt = strings.ReplaceAll(val, ",", "")
			default:
				lprintf(1, "[ERROR][go-%d] getPurchase: unknown field (%s) \n", goID, val)
				break
			}
		})
		lprintf(3, "[INFO][go-%d] getPurchase: resp purchase sum list (%s:%d건)(%v) \n", goID, bizNum, len(purchaseSum.ResultList), purchaseSum)

		var chkArrBaseUri, amtUri, tcntUri, chkArrUri string
		address = "https://www.cardsales.or.kr/page/api/purchase/dayDetail?q.flag=&q.stdYear=&q.stdMonth=&q.pageType=&q.searchDateCode=PCA_DATE&selAmt=" + purchaseSum.ResultSum.PcaScdAmt + "&selCnt=" + purchaseSum.ResultSum.PcaCnt + "&oldAmt=&q.oldMerNo=&q.oldMerGrpId=" + grpId + "&q.oldSearchDate=" + bsDt + "&q.oldCardCo=&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=&q.searchDate=" + bsDt + "&checkbox-inline=on"
		for _, purchaseList := range purchaseSum.ResultList {
			chkArrBaseUri = chkArrBaseUri + "&chkArrBase=" + purchaseList.CardCo
			amtUri = amtUri + "&amt=" + purchaseList.PcaScdAmt
			tcntUri = tcntUri + "&tcnt=" + purchaseList.PcaCnt
			chkArrUri = chkArrUri + "&q.chkArr=" + purchaseList.CardCo
			//&q.dataPerPage=20&q.chkArr=04&q.chkArr=13&q.chkArr=03&q.chkArr=21&q.chkArr=12&currentPage=1
		}

		// detail
		sumCnt, err := strconv.Atoi(purchaseSum.ResultSum.PcaCnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPurchase: data format (purchaseSum.ResultSum.PcaCnt:%s) \n", goID, purchaseSum.ResultSum.PcaCnt)
			return "", "", CcErrDataFormat, cookie
		}

		loopCnt := sumCnt / datePerPage
		if sumCnt%datePerPage != 0 {
			loopCnt = loopCnt + 1
		}

		var updateOrgData []PurchaseDetailType
		var detailCnt, detailAmt int
		address = address + chkArrBaseUri + amtUri + tcntUri + chkArrUri
		deleteDataTemp(goID, PurchaseTy, bizNum, bsDt)
		for i := 1; i <= loopCnt; i++ {
			tmpAddr := address + fmt.Sprintf("&q.dataPerPage=%d&currentPage=%d", datePerPage, i)
			cnt, amt, errCd, newCookie := getPurchaseDetail(goID, cookie, bsDt, tmpAddr, comp, &updateOrgData)
			cookie = newCookie
			if errCd != CcErrNo {
				// lprintf(1, "[ERROR][go-%d] getPurchase: failed to get detail list from DB \n", goID)
				deleteDataTemp(goID, PurchaseTy, bizNum, bsDt)
				return "", "", errCd, cookie
			}

			detailCnt += cnt
			detailAmt += amt
		}

		// 매입내역 상세 리스트 기존 내용 삭제 후 DB저장
		deleteData(goID, PurchaseTy, bizNum, bsDt)
		moveData(goID, PurchaseTy, bizNum, bsDt)

		// detail 저장 후 매입 합계 DB 저장
		row := insertData(goID, PurchaseSum, paramStr, &purchaseSum.ResultSum)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getPurchase: Sum failed to store DB \n", goID)
			return "", "", CcErrDb, cookie
		}

		for _, purchaseList := range purchaseSum.ResultList {
			// 매입내역 합계 리스트 DB저장
			paramStr := make([]string, 0, 5)
			paramStr = append(paramStr, bizNum)
			paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
			row := insertData(goID, PurchaseList, paramStr, &purchaseList)
			if row < 0 {
				lprintf(1, "[ERROR][go-%d] getPurchase: sum list failed to store DB \n", goID)
				return "", "", CcErrDb, cookie
			}
		}

		// 매입취소건일 경우 매입승인거래의 상태를 취소대응으로 변경
		if len(updateOrgData) > 0 {
			lprintf(3, "[INFO][go-%d] updateOrgData(%v) \n", goID, updateOrgData)
			for _, upData := range updateOrgData {
				lprintf(4, "[INFO][go-%d] upData(%v) \n", goID, upData)

				fields := []string{"STS_CD"}
				wheres := []string{"BIZ_NUM", "APRV_NO", "CARD_NO", "APRV_CLSS"}
				values := []string{"2", bizNum, upData.AuthNo, upData.CardNo, "0"}
				// "update cc_pca_dtl set STS_CD=? where BIZ_NUM=? and APRV_NO=? and CARD_NO=? and APRV_CLSS=0;"
				ret := updateDetail(goID, PurchaseDetail, fields, wheres, values)
				lprintf(3, "[INFO][go-%d] getPurchase: sts_cd update (%d건)(%s,%s,%s) \n", goID, ret, bizNum, upData.AuthNo, upData.CardNo)
			}
		}

		// 합계, 상세내역 비교
		sumAmt, err := strconv.Atoi(purchaseSum.ResultSum.PcaScdAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPurchase: data format (purchaseSum.ResultSum.PcaCnt:%s)", goID, purchaseSum.ResultSum.PcaCnt)
			return "", "", CcErrDataFormat, cookie
		}

		if sumCnt != detailCnt {
			lprintf(1, "[ERROR][go-%d] getPurchase: differ to Purchase count sum(%d):detail(%d) \n", goID, sumCnt, detailCnt)

			// 합계건수 수정
			fields := []string{"PCA_CNT"}
			wheres := []string{"BIZ_NUM", "BS_DT"}
			values := []string{strconv.Itoa(detailCnt), bizNum, bsDt}
			// "update cc_pca_sum set PCA_CNT=? where BIZ_NUM=? and BS_DT=?;"
			ret := updateSum(goID, PurchaseSum, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getPurchase: pca_cnt update (%d건)(%s,%s) \n", goID, ret, bizNum, bsDt)

			return "", "", CcErrPcaCnt, cookie
		}

		if sumAmt != detailAmt {
			lprintf(1, "[ERROR][go-%d] getPurchase: differ to Purchase amount sum(%d):detail(%d) \n", goID, sumAmt, detailAmt)

			// 합계금액 수정
			fields := []string{"PCA_AMT"}
			wheres := []string{"BIZ_NUM", "BS_DT"}
			values := []string{strconv.Itoa(detailAmt), bizNum, bsDt}
			// "update cc_pca_sum set PCA_AMT=? where BIZ_NUM=? and BS_DT=?;"
			ret := updateSum(goID, PurchaseSum, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getPurchase: pca_amt update (%d건)(%s,%s) \n", goID, ret, bizNum, bsDt)

			return "", "", CcErrPcaAmt, cookie
		}

		return purchaseSum.ResultSum.PcaCnt, purchaseSum.ResultSum.PcaScdAmt, CcErrNo, cookie
	}

	lprintf(3, "[INFO][go-%d] getPurchase: resp purchase sum (%s:%d건)(%v) \n", goID, bizNum, len(purchaseSum.ResultList), purchaseSum)
	return "0", "0", CcErrNoData, cookie
}

// 매입내역 상세 리스트
func getPurchaseDetail(goID int, cookie []*http.Cookie, bsDt, address string, comp CompInfoType, canList *[]PurchaseDetailType) (purCnt, putAmt int, errCd string, ncookie []*http.Cookie) {
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return -1, -1, CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return -1, -1, CcErrHttpResp, cookie
	}
	bizNum := comp.BizNum
	cookie = newCookie

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPurchaseDetail: response {%s}", goID, err)
		return -1, -1, CcErrHttp, cookie
	}

	var purchaseDetailList []PurchaseDetailType
	if err := json.Unmarshal(bodyBytes, &purchaseDetailList); err != nil { //json byte array를 , 다른객체에 넣어줌
		lprintf(1, "[ERROR][go-%d] getPurchaseDetail: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getPurchaseDetail: req body=(%v) \n", bodyBytes)
		return -1, -1, CcErrParsing, cookie
	}
	lprintf(4, "[INFO][go-%d] getPurchaseDetail: resp purchase detail (%s:%d건)(%v) \n", goID, bizNum, len(purchaseDetailList), purchaseDetailList)

	var pcaSum int
	for _, purchaseDetail := range purchaseDetailList {
		tmpAmt, err := strconv.Atoi(purchaseDetail.PcaAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPurchaseDetail: data format (purchaseDetail.PcaAmt:%s) \n", goID, purchaseDetail.PcaAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		pcaSum = pcaSum + tmpAmt

		if strings.TrimSpace(purchaseDetail.AuthClss) == "1" {
			purchaseDetail.OrgTrDt = getRealTrDt(goID, bizNum, purchaseDetail)
			if len(purchaseDetail.OrgTrDt) == 0 {
				// 승인테이블에 취소거래가 없는 경우(부정거래 의심건)
				purchaseDetail.FraudYn = "Y"
				purchaseDetail.OrgTrDt = purchaseDetail.PcaDate
			}
			purchaseDetail.StsCd = "3"

			// 취소 후처리 해야하는 데이터
			*canList = append(*canList, purchaseDetail)
		} else {
			purchaseDetail.OrgTrDt = purchaseDetail.TrnsDate
			purchaseDetail.StsCd = "1"
		}

		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(bsDt, "-", ""))
		row := insertData(goID, PurchaseDetail, paramStr, &purchaseDetail)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getPurchaseDetail: detail list failed to store DB \n", goID)
			return -1, -1, CcErrDb, cookie
		}

		// 승인상세 테이블에 매입유무 업데이트
		fields := []string{"PCA_YN"}
		wheres := []string{"BIZ_NUM", "TR_DT", "APRV_NO", "CARD_NO", "APRV_AMT"}
		values := []string{"Y", bizNum, purchaseDetail.TrnsDate, purchaseDetail.AuthNo, purchaseDetail.CardNo, purchaseDetail.PcaAmt}
		ret := updateDetail(goID, ApprovalDetail, fields, wheres, values)
		lprintf(3, "[INFO][go-%d] getPurchaseDetail: pca_yn update (%d건)(%s,%s,%s,%s,%s) \n", goID, ret, bizNum, purchaseDetail.TrnsDate, purchaseDetail.AuthNo, purchaseDetail.CardNo, purchaseDetail.PcaAmt)
	}

	return len(purchaseDetailList), pcaSum, CcErrNo, cookie

}

// 입금내역 합계 리스트
func getPayment(goID int, cookie []*http.Cookie, startDate, endDate, grpId string, comp CompInfoType) (payCnt, payAmt, errCd string, ncookie []*http.Cookie) {
	address := "https://www.cardsales.or.kr/page/api/payment/termListAjax?" + "q.startDate=" + startDate + "&q.endDate=" + endDate + "&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo="
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, address, referer, comp)
	if err != nil {
		return "", "", CcErrHttp, cookie
	}
	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return "", "", CcErrHttpResp, cookie
	}

	bizNum := comp.BizNum
	cookie = newCookie

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPayment: response (%s)", goID, err)
		return "", "", CcErrHttp, cookie
	}

	var paymentSum PaymentSumType
	if err := json.Unmarshal(bodyBytes, &paymentSum); err != nil {
		lprintf(1, "[ERROR][go-%d] getPayment: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getPayment: req body=(%s) \n", goID, bodyBytes)
		return "", "", CcErrParsing, cookie
	}
	lprintf(3, "[INFO][go-%d] getPayment: resp payment sum (%s:%d건)(%v) \n", goID, bizNum, len(paymentSum.ResultList), paymentSum)

	// 입금내역 합계는 기간별 조회여서 결과값이 리스트로 전송됨
	// 그러나 실제 조회는 일별로 하기때문에 리스트에 실제 데이터는 1건만 조회됨
	if len(paymentSum.ResultList) > 0 {
		// 입금내역 상세 조회
		var sumCnt, sumAmt int
		var stdDateArray, amt, tcnt string
		deleteDataTemp(goID, PaymentTy, bizNum, startDate)
		address := "https://www.cardsales.or.kr/page/api/payment/detailTermListAjax?" + "q.mode=&q.flag=&q.stdYear=&q.stdMonth=&q.pageType=" + "&q.merGrpId=" + grpId + "&q.cardCo=&q.merNo=" + "&q.startDate=" + startDate + "&q.endDate=" + endDate
		for _, paymentList := range paymentSum.ResultList {
			stdDateArray = stdDateArray + "&q.stdDateArray=" + paymentList.PayDt // 입금 일자
			amt = amt + "&amt=" + paymentList.PayAmt                             // 입금 합계
			tcnt = tcnt + "&tcnt=" + paymentList.PcaCnt                          // 매출 건수

			sumCnt, err = strconv.Atoi(paymentList.PcaCnt)
			if err != nil {
				lprintf(1, "[ERROR][go-%d] getPayment: data format (paymentList.PcaCnt:%s)", goID, paymentList.PcaCnt)
				return "", "", CcErrDataFormat, cookie
			}
			sumAmt, err = strconv.Atoi(paymentList.PayAmt)
			if err != nil {
				lprintf(1, "[ERROR][go-%d] getPayment: data format (paymentList.PayAmt:%s)", goID, paymentList.PayAmt)
				return "", "", CcErrDataFormat, cookie
			}

			var pay PaymentResultListType
			pay.PayDt = startDate
			pay.PcaCnt = paymentList.PcaCnt
			pay.PcaAmt = paymentList.PcaAmt
			pay.PayAmt = paymentList.PayAmt
			paySum := selectPaySum(goID, bizNum, startDate)
			if paySum == nil {
				return "", "", CcErrDb, cookie
			}

			if len(paySum.PcaCnt) > 0 && paySum.compare(pay) != 0 {
				// DB의 데이터와 수집데이터가 같은 경우, 정상 응답
				lprintf(4, "[INFO][go-%d] getPayment: db Payment sum same (%v) \n", goID, paymentList.PayAmt)
				return paymentList.PcaCnt, paymentList.PayAmt, CcErrSameData, cookie
			}
			// DB의 데이터와 수집데이터가 다른 경우, 삭제 후 재수집
			lprintf(4, "[INFO][go-%d] DB=%v\n", goID, *paySum)
			lprintf(4, "[INFO][go-%d] WEB=%v\n", goID, pay)
		}

		// detail
		pageNo := 1
		address = address + stdDateArray + amt + tcnt + "&q.dataPerPage=" + strconv.Itoa(datePerPage)
		detailCnt, detailAmt, errCd, newCookie := getPaymentDetail(goID, cookie, address, comp, pageNo, startDate)
		cookie = newCookie
		if errCd != CcErrNo {
			// lprintf(1, "[ERROR][go-%d] getPayment: failed to get detail list \n", goID)
			deleteDataTemp(goID, ApprovalTy, bizNum, startDate)
			return "", "", errCd, cookie
		}

		// 입금내역 상세 리스트 삭제 후 DB저장
		deleteData(goID, PaymentTy, bizNum, startDate)
		moveData(goID, PaymentTy, bizNum, startDate)

		// detail 저장 후 입금내역 합계 리스트 DB저장
		for _, paymentList := range paymentSum.ResultList {
			paramStr := make([]string, 0, 5)
			paramStr = append(paramStr, bizNum)
			paramStr = append(paramStr, strings.ReplaceAll(paymentList.PayDt, "-", ""))
			row := insertData(goID, PaymentList, paramStr, &paymentList)
			if row < 0 {
				lprintf(1, "[ERROR][go-%d] getPayment: sum list failed to store DB \n", goID)
				return "", "", CcErrDb, cookie
			}
		}

		// 합계, 상세내역 비교
		if sumCnt != detailCnt {
			lprintf(1, "[ERROR][go-%d] getPayment: Differ to Payment count sum(%d):detail(%d) \n", goID, sumCnt, detailCnt)

			// 합계건수 수정
			fields := []string{"PCA_CNT"}
			wheres := []string{"BIZ_NUM", "BS_DT"}
			values := []string{strconv.Itoa(detailCnt), bizNum, startDate}
			// "update cc_pay_lst set PCA_CNT=? where BIZ_NUM=? and BS_DT=?;"
			ret := updateSum(goID, PaymentList, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getPayment: pca_cnt update (%d건)(%s,%s) \n", goID, ret, bizNum, startDate)

			return "", "", CcErrPayCnt, cookie
		}

		if sumAmt != detailAmt {
			lprintf(1, "[ERROR][go-%d] getPayment: Differ to Payment amount sum(%d):detail(%d) \n", goID, sumAmt, detailAmt)

			// 합계금액 수정
			fields := []string{"PCA_AMT"}
			wheres := []string{"BIZ_NUM", "BS_DT"}
			values := []string{strconv.Itoa(detailAmt), bizNum, startDate}
			// "update cc_pay_lst set PCA_AMT=? where BIZ_NUM=? and BS_DT=?;"
			ret := updateSum(goID, PaymentList, fields, wheres, values)
			lprintf(3, "[INFO][go-%d] getPayment: pca_amt update (%d건)(%s,%s) \n", goID, ret, bizNum, startDate)

			return "", "", CcErrPayAmt, cookie
		}

		return strconv.Itoa(sumCnt), strconv.Itoa(sumAmt), CcErrNo, cookie
	}
	return "0", "0", CcErrNoData, cookie

}

// 입금내역 상세 리스트
func getPaymentDetail(goID int, cookie []*http.Cookie, address string, comp CompInfoType, pageNo int, startDate string) (payCnt, payAmt int, errCd string, ncookie []*http.Cookie) {
	addressAndPage := address + "&currentPage=" + strconv.Itoa(pageNo)
	referer := "https://www.cardsales.or.kr/page/purchase/day"
	respData, err, newCookie := reqHttpLoginAgain(goID, cookie, addressAndPage, referer, comp)
	if err != nil {
		return -1, -1, CcErrHttp, cookie
	}

	if respData.StatusCode != http.StatusOK {
		respData.Body.Close()
		return -1, -1, CcErrHttpResp, cookie
	}

	bizNum := comp.BizNum
	cookie = newCookie

	bodyBytes, err := ioutil.ReadAll(respData.Body)
	respData.Body.Close()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] getPaymentDetail:response (%s)", goID, err)
		return -1, -1, CcErrHttp, cookie
	}

	var paymentDetail []PaymentDetailType
	if err := json.Unmarshal(bodyBytes, &paymentDetail); err != nil {
		lprintf(1, "[ERROR][go-%d] getPaymentDetail: req body unmarshal (%s) \n", goID, err.Error())
		lprintf(1, "[ERROR][go-%d] getPaymentDetail: req body=(%s) \n", goID, bodyBytes)
		return -1, -1, CcErrParsing, cookie
	}
	lprintf(4, "[INFO][go-%d] getPaymentDetail: resp payment details (%s:%d건)(%v) \n", goID, bizNum, len(paymentDetail), paymentDetail)

	var detailCnt, detailAmt int
	for _, detailList := range paymentDetail {
		tmpCnt, err := strconv.Atoi(detailList.PcaCnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPaymentDetail: data format (detailList.PayAmt:%s) \n", goID, detailList.PayAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		tmpAmt, err := strconv.Atoi(detailList.PayAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPaymentDetail: data format (detailList.PayAmt:%s) \n", goID, detailList.PayAmt)
			return -1, -1, CcErrDataFormat, cookie
		}
		detailCnt = detailCnt + tmpCnt
		detailAmt = detailAmt + tmpAmt

		paramStr := make([]string, 0, 5)
		paramStr = append(paramStr, bizNum)
		paramStr = append(paramStr, strings.ReplaceAll(detailList.PayDt, "-", ""))
		row := insertData(goID, PaymentDetail, paramStr, &detailList)
		if row < 0 {
			lprintf(1, "[ERROR][go-%d] getPaymentDetail: detail list failed to store DB \n", goID)
			return -1, -1, CcErrDb, cookie
		}
	}

	// totalCount 가 총합 건수
	// pageNo 단위 보다 totalCount 가 높을 때만 재호출
	if totalCnt, err := strconv.Atoi(paymentDetail[0].TotalCnt); err == nil {
		if (pageNo * datePerPage) < totalCnt {
			pageNo++
			cnt, amt, errCd, newCookie := getPaymentDetail(goID, cookie, address, comp, pageNo, startDate)
			cookie = newCookie
			if errCd != CcErrNo {
				// lprintf(1, "[ERROR][go-%d] getPaymentDetail: failed to get detail list \n", goID)
				return -1, -1, errCd, cookie
			}

			detailCnt += cnt
			detailAmt += amt
		}
	}
	return detailCnt, detailAmt, CcErrNo, cookie
}

func reqHttpLoginAgain(goID int, cookie []*http.Cookie, address, referer string, comp CompInfoType) (*http.Response, error, []*http.Cookie) {
	lprintf(4, "[INFO][go-%d] http NewRequest (%s) \n", goID, address)
	time.Sleep(500 * time.Millisecond)
	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] http NewRequest (%s) \n", goID, err.Error())
		return nil, err, cookie
	}
	for i := range cookie {
		req.AddCookie(cookie[i])
	}
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Host", "www.cardsales.or.kr")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
	req.Header.Set("Referer", referer)

	// send request
	client := &http.Client{
		Timeout: time.Second * 10,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		//	Transport: &http.Transport{
		//		TLSClientConfig: &tls.Config{
		//			InsecureSkipVerify: true,
		//		},
		//		},
	}
	respData, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] request (%s) \n", goID, err)
		return nil, err, cookie
	}

	if respData.StatusCode == 302 {
		lprintf(4, "[INFO][go-%d] login out=(%s) \n", goID, respData)
		respData.Body.Close()
		time.Sleep(1000 * time.Millisecond)
		resp, err := login(goID, comp.LnID, comp.LnPsw)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] login again error=(%s) \n", goID, err)
			return nil, err, cookie
		}
		cookie = resp.Cookie
		respData, err = reqHttp(goID, cookie, address, referer)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] login again and request error =(%s) \n", goID, err)
			return nil, err, cookie
		}
	}

	return respData, nil, cookie
}

func reqHttp(goID int, cookie []*http.Cookie, address, referer string) (*http.Response, error) {
	lprintf(4, "[INFO][go-%d] http NewRequest (%s) \n", goID, address)
	time.Sleep(1000 * time.Millisecond)
	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] http NewRequest (%s) \n", goID, err.Error())
		return nil, err
	}
	for i := range cookie {
		req.AddCookie(cookie[i])
	}
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Host", "www.cardsales.or.kr")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7")
	req.Header.Set("Cache-Control", "max-age=0")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36")
	req.Header.Set("Referer", referer)

	// send request
	client := &http.Client{
		Timeout: time.Second * 10,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	respData, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] request (%s) \n", goID, err)
		return nil, err
	}
	return respData, nil
}

// 어제, 오늘 매출합계 및 오늘 매출예측 수집
func callDaySum() {
	lprintf(3, ">> callDaySum START .... << \n")

	bizNumList := getDaySumStoreList(99, serID)
	if bizNumList == nil {
		lprintf(1, "[FAIL]getDaySumStoreList is nil(%s) \n", serID)
		return
	}

	for _, bizNum := range bizNumList {
		// 실제 매출합계와 예측값 구하기
		lprintf(3, "[INFO] biz_num = [%s]\n", bizNum)

		// 당일 이전 4주 합계
		startDt := time.Now().AddDate(0, 0, -29).Format("20060102")
		endDt := time.Now().AddDate(0, 0, -1).Format("20060102")
		lastAmt, expectAmt := getDaySumData(99, bizNum, startDt, endDt)
		lprintf(3, "[INFO][go-99]lastAmt=%s, expectAmt=%s \n", lastAmt, expectAmt)

		// 어제 매출합계 업데이트
		yesterday := time.Now().AddDate(0, 0, -1).Format("20060102")
		updateDaySum(99, bizNum, yesterday, lastAmt, "")

		// 당일 데이터 저장 유무 확인 오늘 예측
		today := time.Now().Format("20060102")
		checkBizNum := getDaySumCheck(99, bizNum, today)
		if checkBizNum == "" {
			insertDaySum(99, bizNum, today, "0", expectAmt)
		} else {
			updateDaySum(99, bizNum, today, "", expectAmt)
		}

		// 새로운 예측 추가 과거 7일 1
		expectAmtLastWeek := getExpectAmtLastWeek(99, bizNum, today)
		if len(expectAmtLastWeek) > 0 {
			updateExpectAmt(99, bizNum, today, expectAmtLastWeek, ExpectAmtLastWeek)
		}

		// 새로운 예측 추가 과거 14일
		expectAmtLastTwoWeek := getExpectAmtLastTwoWeek(99, bizNum, today)
		if len(expectAmtLastTwoWeek) > 0 {
			updateExpectAmt(99, bizNum, today, expectAmtLastTwoWeek, ExpectAmtLastTwoWeek)
		}

		// 새로운 예측 추가 과거 1달
		expectAmtLastMonth := getExpectAmtLastMonth(99, bizNum, today)
		if len(expectAmtLastMonth) > 0 {
			updateExpectAmt(99, bizNum, today, expectAmtLastMonth, ExpectAmtLastMonth)
		}

		// 내일 예측
		startDt = time.Now().AddDate(0, 0, -28).Format("20060102")
		endDt = time.Now().Format("20060102")
		tomorrow := time.Now().AddDate(0, 0, 1).Format("20060102")

		_, expectAmt = getDaySumDataTomorrow(99, bizNum, startDt, endDt)
		lprintf(3, "[INFO][go-99]expectAmt=%s \n", expectAmt)

		// 당일 데이터 저장 유무 확인
		checkBizNum = getDaySumCheck(99, bizNum, tomorrow)
		if len(checkBizNum) == 0 {
			insertDaySum(99, bizNum, tomorrow, "0", expectAmt)
		} else {
			updateDaySum(99, bizNum, tomorrow, "", expectAmt)
		}
	}

	lprintf(3, ">> callDaySum END ....  << \n")
}

// 기존 cc_day_sale_sum 테이블에 가맹점별로 컬럼 30개 만들기
func callDaySumPlus() {
	lprintf(3, ">> callDaySumPlus START .... << \n")

	bizNumList := getDaySumPlusList(99)
	if bizNumList == nil {
		lprintf(4, "[INFO]getDaySumPlusList is nil\n")
		return
	}

	for _, bizNum := range bizNumList {
		// 실제 매출합계와 예측값 구하기
		lprintf(3, "[INFO] biz_num = [%s]\n", bizNum)

		for i := 0; i < 5; i++ {
			// 당일 이전 4주 합계
			startDt := time.Now().AddDate(0, 0, -(29 + i)).Format("20060102")
			endDt := time.Now().AddDate(0, 0, -(1 + i)).Format("20060102")

			var bsDt string

			bsDt2 := time.Now()
			if i > 0 {
				bsDt = bsDt2.AddDate(0, 0, -(i)).Format("20060102")
			} else {
				bsDt = bsDt2.Format("20060102")
			}

			lastAmt, expectAmt := getDaySumData2(99, bizNum, startDt, endDt, bsDt)
			lprintf(3, "[INFO][go-99]lastAmt=%s, expectAmt=%s \n", lastAmt, expectAmt)

			checkBizNum := getDaySumCheck(99, bizNum, bsDt)
			if checkBizNum == "" {
				insertDaySum(99, bizNum, bsDt, lastAmt, expectAmt)
			} else {
				updateDaySum(99, bizNum, bsDt, lastAmt, expectAmt)
			}
		}
	}

	lprintf(3, ">> callDaySumPlus END....  << \n")
}
