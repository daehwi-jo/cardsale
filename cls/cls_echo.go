package cls

import (
	"encoding/json"
	"fmt"
	"github.com/labstack/echo/v4/middleware"
	"golang.org/x/net/http2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dgrijalva/jwt-go"
	"github.com/gorilla/sessions"
	"github.com/labstack/echo-contrib/session"
	"github.com/labstack/echo/v4"
)

type DomainInfo struct {
	Domain   string
	Protocol string
	Port     string
	Cert1    string
	Cert2    string

	EchoData *echo.Echo
}

type (
	Host struct {
		Echo *echo.Echo
	}
)

type PortList struct {
	port     string
	protocol string
	Path1    string
	Path2    string
	loggerYN string
}

type SubInfo struct {
	Domain   string
	EchoData *echo.Echo
}

type PortInfo struct {
	protocol string
	Cert1    string
	Cert2    string

	dList []SubInfo
}

type User struct {
	Uid     string    `json:"uid"`
	Alterid string    `json:"avatar"`
	Expired time.Time `json:"expired"`
}

var WebPortMap map[string]PortInfo
var portCnt int
var LoggerYN string
var sessDuration time.Duration
var idMgmt map[string]User

const (
	currentUserKey = "plus_auth_user"
	maxConnect = 200
	connectTimeout = 10
)

// 로그인이 성공하면 세션에 로그인 정보를 자장한다.
func SetLoginSession(c echo.Context, id string) echo.Context {
	Dprintf(4, c, "set login session (%s)", id)

	// delete
	c = ClearLoginSession(c, id)

	tm := time.Now().String()
	skey := ShaEncode([]byte(tm + id))

	u := &User{
		Uid:     id,
		Alterid: skey,
	}
	u.Expired = time.Now().Add(sessDuration)
	val, _ := json.Marshal(u)

	idMgmt[id] = *u
	sess, _ := session.Get("hydraplus", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   3600, // 1 hour
		HttpOnly: true,
	}
	sess.Values[currentUserKey] = val
	sess.Save(c.Request(), c.Response())

	// JWT에 세션 정보 저장
	SetLoginJWTSession(c, id, skey)
	return c
}

// 로그인이 성공하면 세션에 로그인 정보를 자장한다.
func SetLoginJWT(c echo.Context, id string) echo.Context {
	Dprintf(4, c, "set login session (%s)", id)

	// delete
	c = ClearLoginSession(c, id)

	tm := time.Now().String()
	skey := ShaEncode([]byte(tm + id))

	// JWT에 세션 정보 저장
	SetLoginJWTSession(c, id, skey)
	return c
}

// 로그인이 성공 시 세션 생성 후 jwt 토큰을 발행하여 저장한다.
func SetLoginJWTSession(c echo.Context, id, auth string) error {
	Dprintf(4, c, "set login session (%s)", id)

	// Create token
	token := jwt.New(jwt.SigningMethodHS256)

	// Set claims
	claims := token.Claims.(jwt.MapClaims)
	claims["ident"] = id
	claims["auth"] = auth
	claims["exp"] = time.Now().Add(time.Hour * 6).Unix() // 1 hour session

	// Generate encoded token and send it as response.
	t, err := token.SignedString([]byte("darago"))
	if err != nil {
		return err
	}
	c.Response().Header().Set("token", t)

	/*
		refreshToken := jwt.New(jwt.SigningMethodHS256)
		rtClaims := refreshToken.Claims.(jwt.MapClaims)
		rtClaims["sub"] = 1
		rtClaims["exp"] = time.Now().Add(time.Hour * 24).Unix()
		rt, err := refreshToken.SignedString([]byte("secret"))
		if err != nil {
			return err
		}
		return c.JSON(http.StatusOK, map[string]string{
			"access_token":  t,
			"refresh_token": rt,
		})
		c.Response().Header().Set("refresh_token", rt)
	*/

	return nil
	// c.JSON(http.StatusOK, map[string]string{ "token": t, })

}

func SetLoginOutJWT(c echo.Context, id, auth string) error {
	Dprintf(4, c, "set login session (%s)", id)

	// Create token
	token := jwt.New(jwt.SigningMethodHS256)

	// Set claims
	claims := token.Claims.(jwt.MapClaims)
	claims["ident"] = id
	claims["auth"] = auth
	claims["exp"] = time.Now().Add(time.Hour * 1).Unix() // 1 hour session

	// Generate encoded token and send it as response.
	t, err := token.SignedString([]byte("LogOutDarago"))
	if err != nil {
		return err
	}
	c.Response().Header().Set("token", t)


	return nil


}


// JWT 토큰에서 아이디를 추출한다.
func GetJwtId(c echo.Context) string {
	token := c.Get("user")
	if token == nil {
		return "notLogin"
	}

	user := token.(*jwt.Token)
	claims := user.Claims.(jwt.MapClaims)
	if claims == nil {
		return "notLogin"
	}
	return claims["ident"].(string)
}

// JWT 토큰에서 id auth 둘다 추출한다.
func GetJwtIdAuth(c echo.Context) (string, string) {
	token := c.Get("user")
	if token == nil {
		return "notLogin", "none"
	}

	user := token.(*jwt.Token)
	claims := user.Claims.(jwt.MapClaims)
	if claims == nil {
		return "notLogin", "none"
	}
	return claims["ident"].(string), claims["auth"].(string)
}

// JWT 토큰을 가져온다.
func GetJwtMap(c echo.Context) jwt.MapClaims {
	user := c.Get("user").(*jwt.Token)
	if user == nil {
		return nil
	}
	claims := user.Claims.(jwt.MapClaims)
	if claims == nil {
		return nil
	}
	return claims
}

// 세션에 데이터를 저장한다.
func SetTempSession(c echo.Context, id, value string) echo.Context {
	Dprintf(4, c, "set temp session (%s)", id)
	sess, _ := session.Get("hydratemp", c)
	sess.Options = &sessions.Options{
		Path:     "/",
		MaxAge:   60, // 1 min
		HttpOnly: true,
	}
	sess.Values[id] = value
	sess.Save(c.Request(), c.Response())
	return c
}

// 세션에서 데이터를 가져온다.
func GetTempSession(c echo.Context, id string) (string, bool) {
	Dprintf(4, c, "get data session ")

	sess, err := session.Get("hydratemp", c)
	if err != nil {
		Dprintf(4, c, "sesstion get error (%s) ", err)
		return "", false
	}

	val, exist := sess.Values[id]
	if exist {
		return val.(string), exist
	}
	return "", exist
}

// 클라이언트의 세션을 정리해준다. (로그아웃)
func ClearLoginSession(c echo.Context, id string) echo.Context {
	Dprintf(4, c, "delete login session (%s)", id)
	sess, err := session.Get("hydraplus", c)
	if err == nil {
		sess.Options = &sessions.Options{
			Path:     "/",
			MaxAge:   -1, // delete
			HttpOnly: true,
		}
		sess.Values[currentUserKey] = ""
		sess.Save(c.Request(), c.Response())
	}
	//SetLoginOutJWT(c, "notlogin", "")
	delete(idMgmt, id)
	return c
}

// 로그인 세션 점검 함수 .
func CheckLoginValid(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		url := c.Request().URL.Path
		if strings.HasPrefix(url, "/public") || strings.HasPrefix(url, "/login") {
			return next(c)
		}

		Dprintf(4, c, "get url (%s)", url)
		if GetNotLogin(url) {
			return next(c)
		}

		cc, ok := CheckLoginSession(c)
		if ok {
			return next(cc)
		}
		return cc.Redirect(http.StatusTemporaryRedirect, "/login")
	}
}

// 세션을 분해하여 유효한지 확인한다.
func CheckLoginSession(c echo.Context) (echo.Context, bool) {
	Dprintf(4, c, "check login session ")

	sess, err := session.Get("hydraplus", c)
	if err != nil {
		Dprintf(4, c, "sesstion get error (%s) ", err)
		return c, false
	}

	val, exist := sess.Values[currentUserKey]
	if !exist {
		Dprintf(4, c, "there is not login session ")
		return c, false
	}

	data, ok := val.([]byte)
	if !ok {
		Dprintf(4, c, "there is trouble in session ")
		return c, false
	}

	var u User
	json.Unmarshal(data, &u)

	skey, exist := idMgmt[u.Uid]
	// 세션이 없는 경우 JWT 세션을 넣어줌
	if !exist {
		jwtId, jwtKey := GetJwtIdAuth(c)
		if jwtId == "notlogin" || u.Alterid != jwtKey || u.Uid != jwtId {
			Dprintf(4, c, "there is not login skey ")
			return c, false
		}
		u := &User{
			Uid:     jwtId,
			Alterid: jwtKey,
		}
		idMgmt[u.Uid] = *u
	}

	Dprintf(4, c, "id(%s) key(%s) <==> (%s)", u.Uid, u.Alterid, skey.Alterid)
	//if skey.Alterid != u.Alterid {
	//		Dprintf(4, c, "there is different login skey ")
	//		return c, false
	//	}

	// time
	if u.Expired.Sub(time.Now()) < 0 {
		Dprintf(4, c, "session time out")
		return c, false
	}

	u.Expired = time.Now().Add(1 * time.Hour)
	newval, _ := json.Marshal(u)
	sess.Values[currentUserKey] = newval
	sess.Save(c.Request(), c.Response())

	return c, true
}

// 세션을 정리해 준다.
func idMgmtClear() {
	for _, idm := range idMgmt {
		// login 한지 24시간이 지난 애는 강제 out
		if time.Now().Sub(idm.Expired) >= (time.Hour * 24) {
			delete(idMgmt, idm.Uid)
		}
	}
}

// port로만 띄울때 - domain을 사용하지 않는다.
// port 별로 1개 씩 web server가 매핑된다.
func StartLocalPort(domainList []DomainInfo) {

	if !mappingPortProtocol(domainList) {
		Lprintf(1, "[FAIL] port, protocol  duplicate \n")
		return
	}
	portCnt = len(WebPortMap)
	Lprintf(4, "[INFO] port list cnt (%d)\n", portCnt)

	if len(domainList) != portCnt {
		Lprintf(1, "[FAIL] only port vs domain 1:1 -> (%d:%d)", len(domainList), portCnt)
		return
	}

	sChk := make(chan string)
	for port, pInfo := range WebPortMap {
		go ServeWebPort(port, pInfo, sChk, false, false)
	}

	for {
		select {
		case svrPort := <-sChk:
			Lprintf(1, "[FAIL] WebServer Down port (%s)", svrPort)
			return
		}
		time.Sleep(time.Minute * 1)
		idMgmtClear()
	}
	return
}

func ServeWebPort(port string, pInfo PortInfo, state chan string, login, httpsmode bool) {

	Lprintf(4, "[INFO] ServeWebPort start")

	serverConf := &http2.Server{
		MaxConcurrentStreams: maxConnect,
		IdleTimeout:          connectTimeout * time.Second,
	}

	e := pInfo.dList[0].EchoData
	if LoggerYN == "Y" {
		e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
			Format: "[System] ${time_rfc3339} | ${status} | ${latency_human} | ${method} ${uri} \n",
		}))
	}
	e.Use(middleware.Recover())
	e.Use(session.Middleware(sessions.NewCookieStore([]byte("innogshydra"))))
	e.Use(middleware.SecureWithConfig(middleware.SecureConfig{
		XSSProtection: "1; mode=block",
		XFrameOptions: "SAMEORIGIN",
		HSTSMaxAge:    3600,
		//	ContentSecurityPolicy: "default-src 'self'",
		//	ContentTypeNosniff: "nosniff",
	}))
	if login {
		e.Use(CheckLoginValid)
	}

	if pInfo.protocol == "HTTPS" {
		Lprintf(4, "[INFO] RUN HTTPS WebServer port(%s) cert(%s), key(%s)start", port, pInfo.Cert1, pInfo.Cert2)
		if httpsmode {
			e.Pre(middleware.HTTPSRedirect())
			go func(c *echo.Echo) {
				//e.Start(":80")
				e.StartH2CServer(":80", serverConf)
			}(e)
		}
		e.StartTLS(":"+port, pInfo.Cert1, pInfo.Cert2)
	} else {
		Lprintf(4, "[INFO] RUN HTTP WebServer port(%s) start", port)
		//e.Start(":" + port)
		e.StartH2CServer(":"+port, serverConf)
	}
	state <- port
}

// only true 이면 - domain check 를 한다.
func StartDomainCheck(domainList []DomainInfo, only, login, httpsmode bool) {
	if !mappingPortProtocol(domainList) {
		Lprintf(1, "[FAIL] port, protocol  duplicate \n")
		return
	}
	portCnt = len(WebPortMap)
	Lprintf(4, "[INFO] port list cnt (%d)\n", portCnt)

	sChk := make(chan string)
	for port, pInfo := range WebPortMap {
		if len(pInfo.dList) == 1 && !only {
			go ServeWebPort(port, pInfo, sChk, login, httpsmode)
		} else {
			go ServeWeb(port, pInfo, sChk, login, httpsmode)
		}
	}

	for {
		select {
		case svrPort := <-sChk:
			Lprintf(1, "[FAIL] WebServer Down port (%s)", svrPort)
			return
		}
		time.Sleep(time.Minute * 1)
		idMgmtClear()
	}

	Lprintf(1, "[FAIL] WebServer Down")
}

// 여러 Domain으로 띄울때 - 해당 domain 만 accept 된다
func StartDomainHttps(domainList []DomainInfo) {
	StartDomainCheck(domainList, false, false, true)
}

// 여러 Domain으로 띄울때 - 해당 domain 만 accept 된다
func StartDomain(domainList []DomainInfo) {
	StartDomainCheck(domainList, false, false, false)
}

// 여러 Domain으로 띄울때 - 해당 domain 만 accept 된다 - login session check
func StartDomainLogin(domainList []DomainInfo) {
	StartDomainCheck(domainList, false, true, false)
}

// 여러 Domain으로 띄울때 - 해당 domain 만 accept 된다 - login session check - http -> https
func StartDomainLoginHttps(domainList []DomainInfo) {
	StartDomainCheck(domainList, false, true, true)
}

func ServeWeb(port string, pInfo PortInfo, state chan string, login, httpsmode bool) {

	serverConf := &http2.Server{
		MaxConcurrentStreams: maxConnect,
		IdleTimeout:          connectTimeout * time.Second,
	}

	// port echo
	e := echo.New()
	if LoggerYN == "Y" {
		e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
			Format: "[System] ${time_rfc3339} | ${status} | ${latency_human} | ${method} ${uri} \n",
		}))
	}
	e.Use(middleware.SecureWithConfig(middleware.SecureConfig{
		XSSProtection: "1; mode=block",
		XFrameOptions: "SAMEORIGIN",
		HSTSMaxAge:    3600,
		//	ContentSecurityPolicy: "default-src 'self'",
		//	ContentTypeNosniff: "nosniff",
	}))

	// same port
	hostList := make(map[string]*Host)
	for i := 0; i < len(pInfo.dList); i++ {
		if port == "80" || port == "443" {
			hostList[pInfo.dList[i].Domain] = &Host{pInfo.dList[i].EchoData}
		} else {
			hostList[pInfo.dList[i].Domain+":"+port] = &Host{pInfo.dList[i].EchoData}
		}
		Lprintf(4, "[INFO] Host SET (%s)", pInfo.dList[i].Domain)

		if strings.HasPrefix(pInfo.dList[i].Domain, "www.") {
			domain := strings.TrimLeft(pInfo.dList[i].Domain, "www.")
			if port == "80" || port == "443" {
				hostList[domain] = &Host{pInfo.dList[i].EchoData}
			} else {
				hostList[domain+":"+port] = &Host{pInfo.dList[i].EchoData}
			}
			Lprintf(4, "[INFO] and Host SET (%s) too", domain)
		}
		// session
		pInfo.dList[i].EchoData.Use(session.Middleware(sessions.NewCookieStore([]byte("innogshydra"))))
	}

	e.Any("/*", func(c echo.Context) (err error) {
		req := c.Request()
		res := c.Response()
		host := hostList[req.Host]
		if host == nil {
			err = echo.ErrNotFound
		} else {
			host.Echo.ServeHTTP(res, req)
		}
		return
	})
	if login {
		e.Use(CheckLoginValid)
	}

	Lprintf(4, "[INFO] RUN domain WebServer port(%s) protocol(%s) start", port, pInfo.protocol)
	if pInfo.protocol == "HTTPS" {
		if httpsmode {
			e.Pre(middleware.HTTPSRedirect())
			go func(c *echo.Echo) {
				//e.Start(":80")
				e.StartH2CServer(":80",serverConf)
			}(e)
		}
		e.StartTLS(":"+port, pInfo.Cert1, pInfo.Cert2)
	} else {
		//e.Start(":" + port)
		e.StartH2CServer(":"+port,serverConf)
	}

	state <- port
}

func WebConf(fname string) []DomainInfo {
	v, r := GetTokenValue("SERVER_INFO", fname) // value & return
	if r == CONF_ERR {
		return nil
	}

	fmt.Println("SERVER INFO value : ", v)
	sCnt, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		fmt.Println("atoi error : ", err)
		return nil
	}

	v, r = GetTokenValue("LOGGER", fname) // value & return
	if r != CONF_ERR {
		LoggerYN = v
	} else {
		LoggerYN = "Y"
	}

	v, r = GetTokenValue("SESSION", fname) // value & return
	if r != CONF_ERR {
		tm, _ := strconv.Atoi(v)
		sessDuration = time.Duration(tm) * time.Minute
	} else {
		sessDuration = 10 * time.Minute
	}

	idMgmt = make(map[string]User)
	WebPortMap = make(map[string]PortInfo)
	domainList := make([]DomainInfo, sCnt)
	for i := 0; i < sCnt; i++ {
		t := fmt.Sprintf("SERVER_INF%02d", i) // token
		v, r = GetTokenValue(t, fname)
		if r == CONF_ERR {
			fmt.Println("can not find ")
			return nil
		}

		sts := strings.Split(v, ",") // split tokens <= line

		serPort := os.Getenv(fmt.Sprintf("SERVER_PORT%d",i))
		if len(serPort) > 0{
			domainList[i].Port = serPort
		}else{
			domainList[i].Port = strings.TrimSpace(sts[0])
		}

		domainList[i].Protocol = strings.TrimSpace(sts[1])
		domainList[i].Domain = strings.TrimSpace(sts[2])
		domainList[i].Cert1 = strings.TrimSpace(sts[3])
		domainList[i].Cert2 = strings.TrimSpace(sts[4])

		//	if domainList[i].Domain == "localhost" {
		//	domainList[i].Domain = get_eth_ip("eth0")
		//	}
	} // loop  all SERVER
	return domainList
}

func mappingPortProtocol(domainInfo []DomainInfo) bool {
	for v := range domainInfo {
		pInfo, exist := WebPortMap[domainInfo[v].Port]
		if exist { // add
			if pInfo.protocol != domainInfo[v].Protocol {
				Lprintf(1, "[FAIL] port(%s) use protocol (%s) protocol(%s)", domainInfo[v].Port, pInfo.protocol, domainInfo[v].Protocol)
				return false
			} else {
				Lprintf(4, "[INFO] add domain(%s) port(%s) protocol(%s)", domainInfo[v].Domain, domainInfo[v].Port, domainInfo[v].Protocol)
				nSub := SubInfo{domainInfo[v].Domain, domainInfo[v].EchoData}
				pInfo.dList = append(pInfo.dList, nSub)
				WebPortMap[domainInfo[v].Port] = pInfo
			}
		} else { // new
			Lprintf(4, "[INFO] new domain(%s) port(%s) protocol(%s)", domainInfo[v].Domain, domainInfo[v].Port, domainInfo[v].Protocol)
			var nInfo PortInfo
			nInfo.protocol = domainInfo[v].Protocol
			nSub := SubInfo{domainInfo[v].Domain, domainInfo[v].EchoData}
			nInfo.dList = append(pInfo.dList, nSub)
			if nInfo.protocol == "HTTPS" {
				nInfo.Cert1 = domainInfo[v].Cert1
				nInfo.Cert2 = domainInfo[v].Cert2
			}
			WebPortMap[domainInfo[v].Port] = nInfo
		}
	}
	return true
}

func GetASPCookie(c echo.Context) (echo.Context, string) {
	var value string
	cookie, err := c.Cookie("ASP.NET_SessionId")
	if err != nil {
		value = "not exist "
	} else {
		value = cookie.Value
	}
	return c, value
}


func GetParamJsonMap(c echo.Context) map[string]string {

	result := make(map[string]string)

	// get param
	/*
	queryString := c.QueryString()
	if len(queryString) > 0{
		keyValues := strings.Split(queryString, "&")
		for _,kv := range keyValues{
			params := strings.Split(kv, "=")
			if len(params) == 2{
				result[params[0]] = fmt.Sprintf("%s",params[1])
			}
		}
	}

	// get body json
	data := make(map[string]interface{})
	err := json.NewDecoder(c.Request().Body).Decode(&data)
	if err == nil{
		for k,v := range data{
			result[k] = fmt.Sprintf("%v",v)
		}
	}
	*/

	params := make(map[string]interface{})
	err := c.Bind(&params)
	if err == nil{
		for k,v := range params{
			result[k] = fmt.Sprintf("%v",v)
		}
	}

	// form submit (multipart/form-data, x-www-form-urlencoded)
	forPrams, err := c.FormParams()
	if err == nil{
		for k,v := range forPrams{
			result[k] = v[0]
		}
	}

	return result
}

// 파라메터로 들어온 값을 string 맵으로 전환 (첫번째 스트링만)해준다
func GetParamMap(c echo.Context) map[string]string {
	params := make(map[string]string)

	paramList, err := c.FormParams()
	if err != nil {
		return nil
	}

	for k, v := range paramList {
		params[k] = v[0]
	}

	return params
}
