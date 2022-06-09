package cls

import (
	"crypto/aes"
	"crypto/cipher"
	"database/sql"
	"encoding/base32"
	"fmt"
	"strings"

	"github.com/sigurn/crc16"
)

type UserInfo struct {
	iSid       int
	bMngYn     bool
	iSiteId    int
	bUseYn     bool
	sLoginId   string
	sFirstName string
	sLastName  string
	sUserEmail string
	iCurrency  int
}

var StdAlpabet string = "12345678abcdefghijklmnpqrstuvwxy"
var base *base32.Encoding = base32.NewEncoding(StdAlpabet)
var crcTable *crc16.Table = crc16.MakeTable(crc16.CRC16_ARC)

var aesKey []byte = []byte{109, 56, 85, 44, 248, 44, 18, 128, 236, 116, 13, 250, 243, 45, 122, 133, 199, 241, 124, 188, 188, 93, 65, 153, 214, 193, 127, 85, 132, 147, 193, 68}
var aesIV []byte = []byte{89, 93, 106, 165, 128, 137, 36, 38, 122, 121, 249, 59, 151, 133, 155, 148}

func EncryptAESCFB(src []byte) []byte {
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		fmt.Printf("error \n")
	}
	stream := cipher.NewCFBEncrypter(block, aesIV)
	// XORKeyStream can work in-place if the two arguments are the same.
	stream.XORKeyStream(src, src)
	//fmt.Printf("(%v)", src)

	return src

}

func DecryptAESCFB(src []byte) []byte {
	block, err := aes.NewCipher(aesKey)
	if err != nil {
		fmt.Printf("error \n")
	}
	stream := cipher.NewCFBDecrypter(block, aesIV)
	stream.XORKeyStream(src, src)

	return src
}

func DDecrypt(src string) []byte {

	src = strings.TrimSpace(src)
	// add padding
	padding := "======="
	ipad := len(src) % 8
	Lprintf(4, "(%d) ipad  (%d)", len(src), ipad)
	if ipad != 0 {
		ipad = 8 - ipad
		src += padding[:ipad]
	}

	// decoding
	dest, err := base.DecodeString(src)
	if err != nil {
		Lprintf(4, "error decode (%s)", err)
		dest[0] = 0x00
		return dest
	}

	//Lprintf(4, "[INFO] decode start (%x)", dest)

	return DecryptAESCFB(dest)
}

func EEncode(src []byte) string {

	enc := EncryptAESCFB(src)

	// Lprintf(4, "[INFO] encrypt result  (%x)", enc)

	// encoding
	dest := base.EncodeToString(enc)

	// delete padding
	dest = strings.Trim(dest, "=")

	return dest
}

func Decode32(src string) []byte {

	// add padding
	padding := "======="
	ipad := len(src) % 8
	if ipad != 0 {
		ipad = 8 - ipad
		src += padding[:ipad]
	}

	// decoding
	dest, err := base.DecodeString(src)
	if err != nil {
		Lprintf(4, "error decode (%s)", err)
		dest[0] = 0x00
		return dest
	}

	Lprintf(4, "[INFO] decode start (%x)", dest)

	return dest
}

/*
* CheckSum 데이터(4bytes)
* data : CheckSum 위한 원본 데이터
 */
func makeCheckSum(data []byte) (string, uint32) {
	var word16 uint16
	var sum uint32
	var result string

	Lprintf(4, "[INFO] checksum (%s)", string(data))
	dLen := len(data)

	// 모든 데이터를 덧셈
	for i := 0; i < dLen; i += 2 {
		word16 = (uint16(data[i]) << 8) & 0xFF00
		if (i + 1) < dLen {
			word16 += uint16(data[i+1]) & 0xFF
		}
		//fmt.Printf("[INFO] word16[%d] (%x) (%d)\n", i, word16, word16)
		sum += uint32(word16)
	}

	// 캐리 니블 버림
	sum = (sum & 0xFFFF) + (sum >> 16)
	// 2의 보수
	sum = ^sum
	result = fmt.Sprintf("%X", sum)
	return result[4:], sum
}

func MakeCname(siteid int, fqdn, hostid string) string {
	siteEncode := fmt.Sprintf("%d,%s", siteid, hostid)
	dest := base.EncodeToString([]byte(siteEncode))
	dest = strings.TrimRight(dest, "=") + "." + fqdn
	return fmt.Sprintf("kh%.5d-%s", crc16.Checksum([]byte(dest), crcTable), dest)
}

func MakeNodeList(aCnt int, rRatio []int, nodeList []string) []string {
	rCnt := len(rRatio)
	if rCnt != len(nodeList) {
		return nil
	}

	nList := make([][]string, rCnt)
	nLidx := make([]int, rCnt)
	for k := 0; k < rCnt; k++ {
		nList[k] = strings.Split(nodeList[k], ",")
	}

	retList := make([]string, aCnt)
	for i := 0; i < aCnt; {
		for k := 0; k < rCnt; k++ {
			idx := nLidx[k]
			for c := 0; c < rRatio[k]; c++ {
				retList[i] = nList[k][idx]
				idx++
				i++
			}
			nLidx[k] = idx
		}
	}

	return retList
}

func MakeNodeGroup(aCnt int, rRatio []int, mRatio []int, gRatio []int, nodeList []string) []string {
	Lprintf(4, "[INFO] make node group start (%d)", aCnt)
	allNodeList := MakeNodeList(aCnt, rRatio, nodeList)

	hRatio := make([]int, 8)
	for i := 0; i < 8; i++ {
		if i%2 == 0 {
			hRatio[i] = gRatio[i/2]
		} else {
			hRatio[i] = mRatio[i/2]
		}
	}
	Lprintf(4, "[INFO] ratio list (%v)", hRatio)

	retList := make([]string, 8)
	idx := 0
	for i := 0; i < 8; i++ {
		groupList := ""
		for c := 0; c < hRatio[i]; c++ {
			groupList += allNodeList[idx] + ","
			idx++
		}
		retList[i] = groupList
	}

	return retList
}

func SyncUserList(upDate string, dbc *sql.DB) int {
	// make query and sen
	setQuery := fmt.Sprintf("@xParamIp=0, @iSiteId=0, @iLoginId=1, @iServiceType=0, @SecurityKey=0xD1B154B1734C910A7FD499E31B1A5E279CD926, @dUpdDate='%s', @iLanguage=0", upDate)
	procName := fmt.Sprint("EXEC ps.sp_getPlusUserList " + setQuery)
	Lprintf(4, "[INFO] exec proc : (%s)\n", procName)

	// ms sql query
	row, err := QueryDB2(procName)
	if err != nil {
		Lprintf(1, "[ERROR] sql query(%s) error(%s)\n", procName, err)
		return 0
	}

	// ms sql db result
	var uInfoList []UserInfo
	for row.Next() {
		var uInfo UserInfo
		if err := row.Scan(&uInfo.iSid, &uInfo.bMngYn, &uInfo.iSiteId, &uInfo.bUseYn, &uInfo.iCurrency, &uInfo.sLoginId, &uInfo.sUserEmail, &uInfo.sFirstName, &uInfo.sLastName); err != nil {
			Lprintf(1, "[ERROR] sql scan error(%s)\n", err.Error())
			continue
		}

		if uInfo.bUseYn {
			uInfoList = append(uInfoList, uInfo)
		}
	}
	row.Close()

	cnt := 0
	for _, uInfo := range uInfoList {
		// make query and send
		if len(uInfo.sUserEmail) > 255 {
			uInfo.sUserEmail = uInfo.sUserEmail[:255]
		}
		setQuery := fmt.Sprintf("%d, 0, '%s', %d, '%s %s', '%s', %d", uInfo.iSid, uInfo.sLoginId, uInfo.iSiteId, uInfo.sFirstName, uInfo.sLastName, uInfo.sUserEmail, uInfo.iCurrency)
		if uInfo.bMngYn {
			setQuery = fmt.Sprintf("%d, 1, '%s', %d, '%s %s', '%s', %d", uInfo.iSid, uInfo.sLoginId, uInfo.iSiteId, uInfo.sFirstName, uInfo.sLastName, uInfo.sUserEmail, uInfo.iCurrency)
		}
		procName := fmt.Sprint("CALL PROC_BO_SETDNSUSER(" + setQuery + ");")
		Lprintf(4, "[INFO] procName : %s\n", procName)

		row, err := dbc.Query(procName)
		if err != nil {
			Lprintf(1, "[ERROR] sql query(%s) error(%s)\n", procName, err)
			row.Close()
			continue
		}

		resp := GetRespCode(row, procName)
		if resp != 0 {
			Lprintf(1, "[ERROR] proc(%s) error, resp (%d)\n", procName, resp)
			row.Close()
			continue
		}
		row.Close()
		cnt++
	}

	return cnt
}
