package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cardsales/cls"
)

// 수집결과 정상건수 조회
func getResultCnt(bsDt, restID, serID string) (int, int) {
	var statement string
	var rows *sql.Rows
	var err error

	if len(restID) == 0 {
		statement = "select COUNT(a.BIZ_NUM), ifnull(SUM(IF(RIGHT(a.MOD_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s') || RIGHT(a.REG_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s'),1,0)),0) " +
			"from cc_sync_inf a, cc_comp_inf b where a.BS_DT=? and a.ERR_CD=? and a.BIZ_NUM = b.BIZ_NUM and a.SITE_CD=? and b.SER_ID=?"
		rows, err = cls.QueryDBbyParam(statement, bsDt, "0000", siteCd, serID)
	}else if restID == "miss"{
		statement = "select COUNT(a.BIZ_NUM), SUM(IF(RIGHT(a.MOD_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s') || RIGHT(a.REG_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s'),1,0)) " +
			"from cc_sync_inf a, cc_comp_inf b where a.BS_DT=? and a.ERR_CD=? and a.BIZ_NUM = b.BIZ_NUM and a.SITE_CD=? and b.SER_ID=?"
		rows, err = cls.QueryDBbyParam(statement, bsDt, "0005", siteCd, serID)
	} else {
		statement = "select COUNT(a.BIZ_NUM), SUM(IF(RIGHT(a.MOD_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s') || RIGHT(a.REG_DT,6) > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 10 MINUTE), '%H%i%s'),1,0)) " +
			"from cc_sync_inf a, cc_comp_inf b where a.BS_DT=? and a.ERR_CD=? and a.BIZ_NUM = b.BIZ_NUM and a.SITE_CD=? and b.REST_ID=?"
		rows, err = cls.QueryDBbyParam(statement, bsDt, "0000", siteCd, restID)
	}

	if err != nil {
		lprintf(1, "[ERROR] getResultCnt: cls.QueryDBbyParam error(%s) \n", err.Error())
		return 0, 0
	}
	defer rows.Close()

	var sum, cnt int
	for rows.Next() {
		err := rows.Scan(&sum, &cnt)
		if err != nil {
			lprintf(1, "[ERROR] getResultCnt: rows.Scan error(%s) \n", err.Error())
			return 0, 0
		}
	}

	return sum, cnt
}

// 여신데이터 수집대상 가맹점 리스트 조회
func getCompInfos(serID, bsDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, IFNULL(a.SVC_OPEN_DT,''), a.LN_FIRST_YN, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? and b.SITE_CD=? " +
		"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, siteCd, serID, "1", "1") // 여신협회가입상태, 금결원가입상태
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			continue
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 여신데이터 수집대상 가맹점 신규 리스트 조회
func getCompInfosNew(serID, bsDt, openDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, IFNULL(a.SVC_OPEN_DT,''), a.LN_FIRST_YN, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		//"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? and b.SITE_CD=? " +
		//"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? and a.SVC_OPEN_DT=?;"
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.SITE_CD=? " +
		"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? GROUP BY a.biz_num HAVING COUNT(b.STS_CD) < 28;"

	//rows, err := cls.QueryDBbyParam(statement, bsDt, siteCd, serID, "1", "1", openDt) // 여신협회가입상태, 금결원가입상태, 가입일자
	rows, err := cls.QueryDBbyParam(statement, siteCd, serID, "1", "1") // 여신협회가입상태, 금결원가입상태, 가입일자
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			continue
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 여신데이터 수집대상 가맹점 신규 리스트 조회
func getCompInfosNew2(serID, bsDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, IFNULL(a.SVC_OPEN_DT,'') as SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? and b.SITE_CD=? " +
		"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? and a.biz_num='1807300300';"

	rows, err := cls.QueryDBbyParam(statement, bsDt, siteCd, serID, "1", "1") // 여신협회가입상태, 금결원가입상태, 가입일자
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			continue
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 여신데이터 수집대상 가맹점 신규 리스트 조회
func getCompInfosYear(serID, restID string) []CompInfoType {
	statement := "select a.BIZ_NUM, IFNULL(a.SVC_OPEN_DT,'') as SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.SITE_CD=? " +
		"where a.SER_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? and a.rest_id=? ORDER BY bs_dt DESC LIMIT 1;"

	rows, err := cls.QueryDBbyParam(statement, siteCd, serID, "1", "1", restID) // 여신협회가입상태, 금결원가입상태, 가입일자
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			continue
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 지정가맹점 여신데이터 수집 정보조회
func getCompInfosByRestID(restID, bsDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, a.SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? and b.SITE_CD=? " +
		"where a.REST_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, siteCd, restID, "1", "1") // 여신협회가입상태, 금결원가입상태
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return nil
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 지정 신규 가맹점 여신데이터 수집 정보조회
func getCompInfosByRestIDNew(restID, bsDt, openDt string) []CompInfoType {
	statement := "select a.BIZ_NUM, a.SVC_OPEN_DT, a.LN_FIRST_YN, a.LN_ID, a.LN_PSW, a.LN_JOIN_STS_CD, " +
		"IFNULL(b.BS_DT,'') as BS_DT, IFNULL(left(b.REG_DT,8),'') AS REG_DT, IFNULL(left(b.MOD_DT,8),'') AS MOD_DT, IFNULL(b.STS_CD,'') as STS_CD, IFNULL(b.ERR_CD,'') as ERRCD " +
		"from cc_comp_inf a left join cc_sync_inf b on a.BIZ_NUM=b.BIZ_NUM and b.BS_DT=? and b.SITE_CD=? " +
		"where a.REST_ID=? and a.COMP_STS_CD=? and a.LN_JOIN_STS_CD=? and a.SVC_OPEN_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bsDt, siteCd, restID, "1", "1", openDt) // 여신협회가입상태, 금결원가입상태
	if err != nil {
		lprintf(1, "[ERROR] getCompInfo: cls.QueryDBbyParam error(%s) \n", err.Error())
		return nil
	}
	defer rows.Close()

	var compInfos []CompInfoType
	for rows.Next() {
		var compInfo CompInfoType
		err := rows.Scan(&compInfo.BizNum, &compInfo.SvcOpenDt, &compInfo.LnFirstYn, &compInfo.LnID, &compInfo.LnPsw, &compInfo.LnJoinStsCd, &compInfo.BsDt, &compInfo.RegDt, &compInfo.ModDt, &compInfo.StsCd, &compInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR] getCompInfo: rows.Scan error(%s) \n", err.Error())
			return nil
		}
		compInfos = append(compInfos, compInfo)
	}

	return compInfos
}

// 여신협회자료받기최초실행여부 업데이트
func updateCompInfo(goID int, bizNum string) int {
	statememt := "update cc_comp_inf set LN_FIRST_YN='Y' where BIZ_NUM=?;"

	var params []interface{}
	params = append(params, bizNum)
	ret, err := cls.ExecDBbyParam(statememt, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 가맹점 Sync 데이터 조회
func selectSync(goID int, bizNum, startDt, endDt string) map[string]SyncInfoType {
	statement := "select BIZ_NUM, BS_DT, SITE_CD, APRV_CNT, APRV_AMT, PCA_CNT, PCA_AMT, PAY_CNT, PAY_AMT, IFNULL(REG_DT,'') as REG_DT, IFNULL(MOD_DT,'') as MOD_DT, STS_CD, ERR_CD from cc_sync_inf where BIZ_NUM=? and BS_DT >= ? and BS_DT <= ? and SITE_CD='1' order by BS_DT desc;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, startDt, endDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectSync: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	var syncInfos map[string]SyncInfoType
	syncInfos = make(map[string]SyncInfoType)
	for rows.Next() {
		var syncInfo SyncInfoType
		err := rows.Scan(&syncInfo.BizNum, &syncInfo.BsDt, &syncInfo.SiteCd, &syncInfo.AprvCnt, &syncInfo.AprvAmt, &syncInfo.PcaCnt, &syncInfo.PcaAmt, &syncInfo.PayCnt, &syncInfo.PayAmt, &syncInfo.RegDt, &syncInfo.ModDt, &syncInfo.StsCd, &syncInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectSync: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
		syncInfos[syncInfo.BsDt] = syncInfo
	}

	return syncInfos
}

// Sync 결과 DB 저장
func insertSync(goID int, syncData SyncInfoType) int {
	var params []interface{}
	var fields []string
	var wheres []string
	var inserts []string
	var statement string

	// SYNC 결과데이터 DB조회
	statement = "select BIZ_NUM, BS_DT, SITE_CD, APRV_CNT, APRV_AMT, PCA_CNT, PCA_AMT, PAY_CNT, PAY_AMT, IFNULL(REG_DT,'') as REG_DT, IFNULL(MOD_DT,'') as MOD_DT, STS_CD, ERR_CD from cc_sync_inf where BIZ_NUM=? and BS_DT=? and SITE_CD=?;"

	rows, err := cls.QueryDBbyParam(statement, syncData.BizNum, syncData.BsDt, "1")
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}
	defer rows.Close()

	var syncInfo SyncInfoType
	for rows.Next() {
		err := rows.Scan(&syncInfo.BizNum, &syncInfo.BsDt, &syncInfo.SiteCd, &syncInfo.AprvCnt, &syncInfo.AprvAmt, &syncInfo.PcaCnt, &syncInfo.PcaAmt, &syncInfo.PayCnt, &syncInfo.PayAmt, &syncInfo.RegDt, &syncInfo.ModDt, &syncInfo.StsCd, &syncInfo.ErrCd)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] rows.Scan error(%s) \n", goID, err.Error())
			return -1
		}
	}
	lprintf(4, "[INFO][go-%d] syncInfo (%v) \n", goID, syncInfo)

	if syncData.AprvAmt == syncInfo.AprvAmt && syncData.AprvCnt == syncInfo.AprvCnt &&
		syncData.PayAmt == syncInfo.PayAmt && syncData.PayCnt == syncInfo.PayCnt &&
		syncData.PcaAmt == syncInfo.PcaAmt && syncData.PcaCnt == syncInfo.PcaCnt && syncData.ErrCd == syncInfo.ErrCd {
		// 값이나 조회 결과의 변동은 없으면 send_dt 설정 안함
		lprintf(4, "[INFO][go-%d] result success but there is not any change (%v) \n", goID, syncInfo)
		syncData.SendDt = ""
	}

	// Sync 데이터 저장/업데이트
	elements := reflect.ValueOf(&syncData).Elem()
	if len(syncInfo.BizNum) == 0 {
		for k := 0; k < elements.NumField(); k++ {
			mValue := elements.Field(k)
			value := fmt.Sprint(mValue.Interface())
			if len(value) > 0 {
				mType := elements.Type().Field(k)
				tag := mType.Tag

				fields = append(fields, tag.Get("db"))
				inserts = append(inserts, "?")
				params = append(params, value)
			}
		}

		// "insert into cc_sync_inf (BIZ_NUM, BS_DT, SITE_CD, APRV_CNT, PCA_CNT, PAY_CNT, REG_DTM, STS_CD, ERR_CD) values (?,?,?,?,?,?,?,?,?)"
		statement = "insert into cc_sync_inf (" + strings.Join(fields, ", ") + ") values (" + strings.Join(inserts, ", ") + ")"
	} else if syncData.ErrCd != "0000" && syncInfo.ErrCd == "0000" {
		// 정상이였던 데이터를 재수집 하다가 에러가 난 경우 sync update 안함
		lprintf(4, "[INFO][go-%d] result fail but earlier result was success -> do not change (%v) \n", goID, syncInfo)
		return -1
	} else {
		var params2 []interface{}
		for k := 0; k < elements.NumField(); k++ {
			mValue := elements.Field(k)
			value := fmt.Sprint(mValue.Interface())
			if len(value) > 0 {
				mType := elements.Type().Field(k)
				tag := mType.Tag

				field := tag.Get("db")
				if len(field) > 0 {
					if strings.Compare(field, "BIZ_NUM") == 0 || strings.Compare(field, "BS_DT") == 0 || strings.Compare(field, "SITE_CD") == 0 {
						wheres = append(wheres, fmt.Sprint(field, "=?"))
						params2 = append(params2, value)
					} else if strings.Compare(field, "APRV_CNT") == 0 || strings.Compare(field, "APRV_AMT") == 0 ||
						strings.Compare(field, "PCA_CNT") == 0 || strings.Compare(field, "PCA_AMT") == 0 ||
						strings.Compare(field, "PAY_CNT") == 0 || strings.Compare(field, "PAY_AMT") == 0 ||
						strings.Compare(field, "STS_CD") == 0 || strings.Compare(field, "ERR_CD") == 0 ||
						strings.Compare(field, "SEND_DT") == 0 {
						fields = append(fields, fmt.Sprint(field, "=?"))
						params = append(params, value)
					} else {
						continue
					}
				}
				// fmt.Printf("%10s:%10s=%10v, db: %10s\n",
				// 	mType.Name, mType.Type, mValue.Interface(), tag.Get("db")) // 이름, 타입, 값, 태그
			}
		}
		fields = append(fields, "MOD_DT=?")
		params = append(params, time.Now().Format("20060102150405"))

		for _, p := range params2 {
			params = append(params, p)
		}

		// "update cc_sync_inf set APRV_CNT=?, APRV_AMT=?, PCA_CNT=?, PCA_AMT=?, PAY_CNT=?, PAY_AMT=?, MOD_DT=?, STS_CD=?, ERR_CODE=? where BIZ_NUM=? and BS_DT=? and SITE_CD=?"
		statement = "update cc_sync_inf set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	}

	ret, err := cls.ExecDBbyParam(statement, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret

}

// 가맹점 Sync 데이터 삭제
func deleteSync(goID int, bizNum, bsDt string) int {
	statement := "delete from cc_sync_inf where BIZ_NUM=? and BS_DT=? and SITE_CD='1';"

	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)
	ret, err := cls.ExecDBbyParam(statement, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 승인 데이터 조회
func selectApprSum(goID int, bizNum, bsDt string) *ResultSumType {
	statement := "select TOT_CNT, TOT_AMT, APRV_CNT, APRV_AMT, CAN_CNT, CAN_AMT from cc_aprv_sum where BIZ_NUM=? and BS_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectApprSumData: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	resultSum := new(ResultSumType)
	for rows.Next() {
		err := rows.Scan(&resultSum.TotTrnsCnt, &resultSum.TotTrnsAmt, &resultSum.TotAuthCnt, &resultSum.TotAuthAmt, &resultSum.TotCnclCnt, &resultSum.TotCnclAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectApprSumData: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
	}

	return resultSum
}

// 매입 데이터 조회
func selectPcaSum(goID int, bizNum, bsDt string) *ResultPurSumType {
	statement := "select PCA_CNT, PCA_AMT, MER_FEE, PNT_FEE, ETC_FEE, TOT_FEE, VAT_AMT, OUTP_EXPT_AMT from cc_pca_sum where BIZ_NUM=? and BS_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectPcaSumData: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	resultSum := new(ResultPurSumType)
	for rows.Next() {
		err := rows.Scan(&resultSum.PcaCnt, &resultSum.PcaScdAmt, &resultSum.MerFee, &resultSum.PntFee, &resultSum.EtcFee, &resultSum.TotFee, &resultSum.VatAmt, &resultSum.OuptExptAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectPcaSumData: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
	}

	return resultSum
}

// 입금 데이터 조회
func selectPaySum(goID int, bizNum, bsDt string) *PaymentResultListType {
	statement := "select PAY_DT, PCA_CNT, PCA_AMT, REAL_PAY_AMT from cc_pay_lst where BIZ_NUM=? and BS_DT=?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] selectPcaSumData: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	resultSum := new(PaymentResultListType)
	for rows.Next() {
		err := rows.Scan(&resultSum.PayDt, &resultSum.PcaCnt, &resultSum.PcaAmt, &resultSum.PayAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] selectPcaSumData: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
	}

	return resultSum
}

// 승인/매입/입금 데이터 Insert Query 생성 & 실행
func insertMonthData(goID int, bizNum, bsDt string) int {
	delQuery := "delete from cc_aprv_sum_month where biz_num = '" + bizNum + "' and bs_dt ='" + bsDt[:6] + "';"
	insQuery := "insert into cc_aprv_sum_month (`BIZ_NUM`,`BS_DT`,`TOT_CNT`,`TOT_AMT`,`APRV_CNT`,`APRV_AMT`,`CAN_CNT`,`CAN_AMT`,`WRT_DT`) "
	insQuery += "select biz_num, left(bs_dt, 6), sum(tot_cnt), sum(tot_amt), sum(aprv_cnt), sum(aprv_amt), sum(can_cnt), sum(can_amt), '" + bsDt + "' "
	insQuery += "from cc_aprv_sum where biz_num = '" + bizNum + "' and left(bs_dt, 6) = '" + bsDt[0:6] + "' group by left(bs_dt, 6), biz_num"

	// transation begin
	tx, err := cls.DBc.Begin()
	if err != nil {
		return -1
	}

	// 오류 처리
	defer func() {
		if err != nil {
			// transaction rollback
			lprintf(1, "[ERROR][go-%d] month insert do rollback \n")
			tx.Rollback()
		}
	}()

	// transation exec
	_, err = tx.Exec(delQuery)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] del Query(%s) -> error (%s) \n", goID, delQuery, err)
		return -2
	}
	// transation exec
	_, err = tx.Exec(insQuery)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] ins Query(%s) -> error (%s) \n", goID, delQuery, err)
		return -2
	}

	// transaction commit
	err = tx.Commit()
	if err != nil {
		lprintf(1, "[ERROR][go-%d] insert month Query commit error (%s) \n", goID, err)
		return -3
	}
	return 0
}

// 승인/매입/입금 데이터 Insert Query 생성 & 실행
func insertData(goID, queryTy int, paramPtr []string, dataTy interface{}) int {

	var statement string
	var fields []string
	var inserts []string
	var params []interface{}

	fields = append(fields, "BIZ_NUM")
	inserts = append(inserts, "?")
	fields = append(fields, "BS_DT")
	inserts = append(inserts, "?")

	elements := reflect.ValueOf(dataTy).Elem()
	for k := 0; k < elements.NumField(); k++ {
		mValue := elements.Field(k)
		mType := elements.Type().Field(k)
		tag := mType.Tag
		// fmt.Printf("%10s:%10s=%10v, db: %10s\n",
		// 	mType.Name, mType.Type, mValue.Interface(), tag.Get("db")) // 이름, 타입, 값, 태그

		if len(tag.Get("db")) == 0 {
			continue
		}

		fields = append(fields, tag.Get("db"))
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, fmt.Sprint(mValue.Interface()))
	}

	switch queryTy {
	case ApprovalSum: // 승인내역 합계
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_aprv_sum (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case ApprovalList: // 승인내역 합계 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_aprv_lst (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case ApprovalDetail: // 승인내역 상세 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))

		// 조회기준일을 Time 값으로 변경
		timeBsDt, err := time.Parse("20060102", paramPtr[1])
		if err != nil {
			lprintf(1, "[ERROR][go-%d] time.Parse (%s) \n", goID, err.Error())
			return -2
		}
		weekend := "WD"
		week := timeBsDt.Weekday()
		if week == 0 || week == 6 {
			weekend = "HD"
		}

		fields = append(fields, "WEEK_END")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, weekend)

		statement = "insert into cc_aprv_dtl_temp (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PurchaseSum: // 매입내역 합계
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_pca_sum (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PurchaseList: // 매입내역 합계 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_pca_lst (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PurchaseDetail: // 매입내역 상세 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))

		statement = "insert into cc_pca_dtl_temp (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PaymentList: // 입금내역 합계 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))
		fields = append(fields, "COLLECTION_STAT_DIV")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, "1")

		statement = "insert into cc_pay_lst (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	case PaymentDetail: // 입금내역 상세 리스트
		fields = append(fields, "WRT_DT")
		inserts = append(inserts, "?")
		paramPtr = append(paramPtr, time.Now().Format("20060102150405"))

		statement = "insert into cc_pay_dtl_temp (" +
			strings.Join(fields, ", ") +
			") values (" + strings.Join(inserts, ", ") + ")"

	default:
		lprintf(1, "[ERROR][go-%d] unknown query type (%s) \n", goID, queryTy)
		return -1
	}

	for _, str := range paramPtr {
		params = append(params, str)
	}

	ret, err := cls.ExecDBbyParam(statement, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 승인/매입/입금 데이터 삭제
func deleteData(goID, ty int, bizNum, bsDt string) int {
	var statememt []string

	if ty == ApprovalTy {
		statememt = append(statememt, "delete from cc_aprv_sum where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_aprv_lst where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_aprv_dtl where BIZ_NUM=? and BS_DT=?;")
	} else if ty == PurchaseTy {
		statememt = append(statememt, "delete from cc_pca_sum where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_pca_lst where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_pca_dtl where BIZ_NUM=? and BS_DT=?;")
	} else {
		statememt = append(statememt, "delete from cc_pay_lst where BIZ_NUM=? and BS_DT=?;")
		statememt = append(statememt, "delete from cc_pay_dtl where BIZ_NUM=? and BS_DT=?;")
	}

	var ret int
	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)
	for _, query := range statememt {
		// lprintf(4, "[INFO] statement=%s \n", query)
		cnt, err := cls.ExecDBbyParam(query, params)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
			return -1
		}
		ret = ret + cnt
	}

	return ret
}

// 승인/매입/입금 데이터 저장일 기준 이전 것 삭제
func deleteDataTemp(goID, ty int, bizNum, bsDt string) int {
	var statememt []string

	if ty == ApprovalTy {
		statememt = append(statememt, "delete from cc_aprv_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else if ty == PurchaseTy {
		statememt = append(statememt, "delete from cc_pca_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else {
		statememt = append(statememt, "delete from cc_pay_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	}

	var ret int
	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)

	for _, query := range statememt {
		// lprintf(4, "[INFO] statement=%s \n", query)
		cnt, err := cls.ExecDBbyParam(query, params)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
			return -1
		}
		ret = ret + cnt
	}

	return ret
}

// 승인/매입/입금 데이터 삭제
func moveData(goID, ty int, bizNum, bsDt string) int {
	var statememt []string

	if ty == ApprovalTy {
		statememt = append(statememt, "insert into cc_aprv_dtl select * from cc_aprv_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else if ty == PurchaseTy {
		statememt = append(statememt, "insert into cc_pca_dtl select * from cc_pca_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	} else {
		statememt = append(statememt, "insert into cc_pay_dtl select * from cc_pay_dtl_temp where BIZ_NUM=? and BS_DT=?;")
	}

	var ret int
	var params []interface{}
	params = append(params, bizNum)
	params = append(params, bsDt)
	for _, query := range statememt {
		// lprintf(4, "[INFO] statement=%s \n", query)
		cnt, err := cls.ExecDBbyParam(query, params)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
			return -1
		}
		ret = ret + cnt
	}

	return ret
}

// update sum info
func updateSum(goID, queryTy int, fields, wheres, values []string) int {
	for i := 0; i < len(fields); i++ {
		fields[i] = fields[i] + "=?"
	}
	for i := 0; i < len(wheres); i++ {
		wheres[i] = wheres[i] + "=?"
	}

	var statememt string
	if queryTy == ApprovalSum {
		statememt = "update cc_aprv_sum set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else if queryTy == PurchaseSum {
		statememt = "update cc_pca_sum set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else if queryTy == PaymentList {
		statememt = "update cc_pay_lst set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else {
		lprintf(1, "[ERROR][go-%d] unknown query type (%s) \n", goID, queryTy)
		return -1
	}

	var params []interface{}
	for _, value := range values {
		params = append(params, value)
	}

	// lprintf(4, "[INFO][go-%d] statement=%s \n", goID, statememt)
	ret, err := cls.ExecDBbyParam(statememt, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// update detail info
func updateDetail(goID, queryTy int, fields, wheres, values []string) int {
	for i := 0; i < len(fields); i++ {
		fields[i] = fields[i] + "=?"
	}
	for i := 0; i < len(wheres); i++ {
		wheres[i] = wheres[i] + "=?"
	}

	var statememt string
	if queryTy == ApprovalDetail {
		statememt = "update cc_aprv_dtl set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else if queryTy == PurchaseDetail {
		statememt = "update cc_pca_dtl set " + strings.Join(fields, ", ") + " where " + strings.Join(wheres, " and ")
	} else {
		lprintf(1, "[ERROR][go-%d] unknown query type (%s) \n", goID, queryTy)
		return -1
	}

	var params []interface{}
	for _, value := range values {
		params = append(params, value)
	}

	// lprintf(4, "[INFO][go-%d] statement=%s \n", goID, statememt)
	ret, err := cls.ExecDBbyParam(statememt, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}

	return ret
}

// 매입취소건 승인테이블 실거래일자 조회(매입취소건의 거래일자는 승인의 거래일자가 전송되므로 승인취소의 거래일자를 조회함)
func getRealTrDt(goID int, bizNum string, purData PurchaseDetailType) string {
	statement := "select TR_DT from cc_aprv_dtl where BIZ_NUM=? AND APRV_NO=? AND CARD_NO=? AND APRV_CLSS=? AND APRV_AMT=?"

	rows, err := cls.QueryDBbyParam(statement, bizNum, purData.AuthNo, purData.CardNo, "1", purData.PcaAmt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var orgTrDt string
	for rows.Next() {
		err := rows.Scan(&orgTrDt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getRealTrDt: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return orgTrDt
}

// 승인취소건 원거래일자 조회(승인일자 조회)
func getOrgTrDt(goID int, bizNum string, aprvData ApprovalDetailType) string {
	statement := "select TR_DT from cc_aprv_dtl where BIZ_NUM=? AND APRV_NO=? AND CARD_NO=? AND APRV_CLSS=? AND APRV_AMT=?"

	authAmt := strings.Replace(aprvData.AuthAmt, "-", "", 1)
	rows, err := cls.QueryDBbyParam(statement, bizNum, aprvData.AuthNo, aprvData.CardNo, "0", authAmt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var orgTrDt string
	for rows.Next() {
		err := rows.Scan(&orgTrDt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getOrgTrDt: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return orgTrDt
}

// PUSH 여부를 결정 확인
func checkPushState(goID int, bizNum, yesterDay string) bool {
	var retOk bool
	var stsCd, pushDate string

	statement := `SELECT a.STS_CD, IFNULL(b.PUSH_DT, "") FROM cc_sync_inf a, cc_comp_inf b WHERE a.BIZ_NUM=? AND a.BS_DT =? AND a.BIZ_NUM = b.BIZ_NUM`

	rows, err := cls.QueryDBbyParam(statement, bizNum, yesterDay)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] checkPushState: cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return retOk
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&stsCd, &pushDate)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] checkPushState: rows.Scan error(%s) \n", goID, err.Error())
			return retOk
		}
	}

	// 늦은 시간에는 push 하지 않는다.
	nowTime := time.Now().Format("150405")
	if nowTime >= "18" {
		lprintf(4, "[INFO][go-%d] time is late: no push(%s) \n", goID, nowTime)
		return false
	}

	// compare bs_dt
	if stsCd == "1" && pushDate != yesterDay {
		retOk = true
	}

	return retOk
}

// PUSH 전송 후 테이블을 업뎃
func updatePushState(goID int, bizNum, sendDt string) {
	query := "update cc_comp_inf set PUSH_DT = '" + sendDt + "' where biz_num = '" + bizNum + "'"

	row, err := cls.QueryDB(query)
	if err != nil {
		sendChannel("PUSH 쿼리 에러", "push save query error ["+sendDt+"]", "655403")
		cls.Lprintf(1, "[error][go-%d] %s\n", goID, err.Error())
		return
	}
	defer row.Close()
}

// 매입처리 결과 조회
func getPcaResult(goID int, bizNum string, aprvData ApprovalDetailType) int {
	statement := "select COUNT(*) from cc_pca_dtl where BIZ_NUM=? AND ORG_TR_DT=? AND APRV_NO=? AND CARD_NO=? AND APRV_CLSS=? AND PCA_AMT=?"

	authAmt := strings.Replace(aprvData.AuthAmt, "-", "", 1)
	rows, err := cls.QueryDBbyParam(statement, bizNum, aprvData.TrnsDate, aprvData.AuthNo, aprvData.CardNo, "0", authAmt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return -1
	}
	defer rows.Close()

	var cnt int
	for rows.Next() {
		err := rows.Scan(&cnt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getPcaResult: rows.Scan error(%s) \n", goID, err.Error())
			return -1
		}
	}

	return cnt
}

// 매출조회 대상 가맹점 조회
func getDaySumStoreList(goID int, serID string) []string {
	statement := "SELECT biz_num FROM cc_comp_inf WHERE ser_id=? AND comp_sts_cd=? AND (ln_join_sts_cd=? OR hometax_join_sts_cd=?)"
	//statement := "SELECT biz_num FROM cc_comp_inf WHERE biz_num='4262200621' or biz_num='1807300300'"
	rows, err := cls.QueryDBbyParam(statement, serID, "1", "1", "1")
	//rows, err := cls.QueryDBbyParam(statement)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	var bizList []string
	for rows.Next() {
		var bizNum string
		err := rows.Scan(&bizNum)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getDaySumStoreList: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
		bizList = append(bizList, bizNum)
	}

	return bizList
}

// 매출 예측 추가대상 가맹점
func getDaySumPlusList(goID int) []string {
	//statement := "SELECT biz_num FROM cc_day_sale_sum GROUP BY biz_num HAVING COUNT(*) = 24;"
	statement := "SELECT biz_num from cc_day_sale_sum WHERE tr_dt='20210424';"

	rows, err := cls.QueryDBbyParam(statement)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return nil
	}
	defer rows.Close()

	var bizList []string
	for rows.Next() {
		var bizNum string
		err := rows.Scan(&bizNum)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getDaySumPlusList: rows.Scan error(%s) \n", goID, err.Error())
			return nil
		}
		bizList = append(bizList, bizNum)
	}

	return bizList
}

// 어제 매출합계 및 4주간 오늘 예상매출 조회
func getDaySumData(goID int, bizNum, startDt, endDt string) (string, string) {
	statement := "SELECT SUM(z.lastAmt) AS lastAmt, SUM(z.expectAmt) AS expectAmt FROM (" +
		"SELECT IFNULL((SELECT SUM(b.aprv_amt) FROM cc_aprv_dtl b WHERE b.biz_num=a.biz_num AND b.tr_dt = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y%m%d')),0) AS lastAmt, " +
		"IFNULL(ROUND(SUM(a.aprv_amt)/4, 0),0) AS expectAmt FROM cc_aprv_dtl a " +
		"WHERE a.biz_num=? AND a.tr_dt BETWEEN ? AND ? AND DAYOFWEEK(a.tr_dt) = DAYOFWEEK(DATE_FORMAT(NOW(), '%Y%m%d')) " +
		"UNION ALL " +
		"SELECT IFNULL((SELECT SUM(b.tot_amt) FROM cc_cash_dtl b WHERE b.biz_num=a.biz_num AND b.tr_dt = DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y%m%d')),0) AS lastAmt, " +
		"IFNULL(ROUND(SUM(a.tot_amt)/4, 0),0) AS expectAmt FROM cc_cash_dtl a " +
		"WHERE a.biz_num=? AND a.tr_dt BETWEEN ? AND ? AND DAYOFWEEK(a.tr_dt) = DAYOFWEEK(DATE_FORMAT(NOW(), '%Y%m%d')) " +
		") z"
	rows, err := cls.QueryDBbyParam(statement, bizNum, startDt, endDt, bizNum, startDt, endDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return "", ""
	}
	defer rows.Close()

	var lastAmt, expectAmt string
	for rows.Next() {
		err := rows.Scan(&lastAmt, &expectAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getDaySumData: rows.Scan error(%s) \n", goID, err.Error())
			return "", ""
		}
	}

	return lastAmt, expectAmt
}

// 오늘 매출합계 및 4주간 내일 예상매출 조회
func getDaySumDataTomorrow(goID int, bizNum, startDt, endDt string) (string, string) {
	statement := "SELECT SUM(z.lastAmt) AS lastAmt, SUM(z.expectAmt) AS expectAmt FROM (" +
		"SELECT IFNULL((SELECT SUM(b.aprv_amt) FROM cc_aprv_dtl b WHERE b.biz_num=a.biz_num AND b.tr_dt = DATE_FORMAT(NOW(), '%Y%m%d')),0) AS lastAmt, " +
		"IFNULL(ROUND(SUM(a.aprv_amt)/4, 0),0) AS expectAmt FROM cc_aprv_dtl a " +
		"WHERE a.biz_num=? AND a.tr_dt BETWEEN ? AND ? AND DAYOFWEEK(a.tr_dt) = DAYOFWEEK(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 1 DAY), '%Y%m%d')) " +
		"UNION ALL " +
		"SELECT IFNULL((SELECT SUM(b.tot_amt) FROM cc_cash_dtl b WHERE b.biz_num=a.biz_num AND b.tr_dt = DATE_FORMAT(NOW(), '%Y%m%d')),0) AS lastAmt, " +
		"IFNULL(ROUND(SUM(a.tot_amt)/4, 0),0) AS expectAmt FROM cc_cash_dtl a " +
		"WHERE a.biz_num=? AND a.tr_dt BETWEEN ? AND ? AND DAYOFWEEK(a.tr_dt) = DAYOFWEEK(DATE_FORMAT(DATE_ADD(NOW(), INTERVAL 1 DAY), '%Y%m%d')) " +
		") z"
	rows, err := cls.QueryDBbyParam(statement, bizNum, startDt, endDt, bizNum, startDt, endDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return "", ""
	}
	defer rows.Close()

	var lastAmt, expectAmt string
	for rows.Next() {
		err := rows.Scan(&lastAmt, &expectAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getDaySumData: rows.Scan error(%s) \n", goID, err.Error())
			return "", ""
		}
	}

	return lastAmt, expectAmt
}


func getDaySumData2(goID int, bizNum, startDt, endDt, bsDt string) (string, string) {
	statement := "SELECT SUM(z.lastAmt) AS lastAmt, SUM(z.expectAmt) AS expectAmt FROM (" +
		"SELECT IFNULL((SELECT SUM(b.aprv_amt) FROM cc_aprv_dtl b WHERE b.biz_num=a.biz_num AND b.tr_dt = ?),0) AS lastAmt, " +
		"IFNULL(ROUND(SUM(a.aprv_amt)/4, 0),0) AS expectAmt FROM cc_aprv_dtl a " +
		"WHERE a.biz_num=? AND a.tr_dt BETWEEN ? AND ? AND DAYOFWEEK(a.tr_dt) = DAYOFWEEK(?) " +
		"UNION ALL " +
		"SELECT IFNULL((SELECT SUM(b.tot_amt) FROM cc_cash_dtl b WHERE b.biz_num=a.biz_num AND b.tr_dt = ?),0) AS lastAmt, " +
		"IFNULL(ROUND(SUM(a.tot_amt)/4, 0),0) AS expectAmt FROM cc_cash_dtl a " +
		"WHERE a.biz_num=? AND a.tr_dt BETWEEN ? AND ? AND DAYOFWEEK(a.tr_dt) = DAYOFWEEK(?) " +
		") z"
	rows, err := cls.QueryDBbyParam(statement, bsDt,bizNum, startDt, endDt, bsDt,bsDt,bizNum, startDt, endDt,bsDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return "", ""
	}
	defer rows.Close()

	var lastAmt, expectAmt string
	for rows.Next() {
		err := rows.Scan(&lastAmt, &expectAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getDaySumData: rows.Scan error(%s) \n", goID, err.Error())
			return "", ""
		}
	}

	return lastAmt, expectAmt
}

// 과거 7일의 매출 예측을 반영한 새로운 예측
func getExpectAmtLastWeek(goID int, bizNum, tr_dt string) (string) {
	statement := "SELECT ifnull(y.expect_amt+y.diff_avg,0) as new_expect_amt FROM (SELECT tr_dt AS tt, real_amt, expect_amt, real_amt-expect_amt AS diff, " +
		"(SELECT round(SUM(z.diff)/7,0) AS dff_avg FROM(SELECT tr_dt, real_amt, expect_amt, real_amt-expect_amt AS diff " +
		"FROM cc_day_sale_sum WHERE biz_num= ? ) z WHERE z.tr_dt BETWEEN DATE_FORMAT(DATE_SUB(tt, INTERVAL 7 DAY), '%Y%m%d') AND DATE_FORMAT(DATE_SUB(tt, INTERVAL 1 DAY), '%Y%m%d')) AS diff_avg " +
		"FROM cc_day_sale_sum WHERE biz_num= ? ORDER BY tt DESC) Y WHERE y.tt = ?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bizNum, tr_dt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var expectAmt string
	for rows.Next() {
		err := rows.Scan(&expectAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getExpectAmt7: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return expectAmt
}

// 과거 14일의 매출 예측을 반영한 새로운 예측
func getExpectAmtLastTwoWeek(goID int, bizNum, tr_dt string) (string) {
	statement := "SELECT ifnull(y.expect_amt+ROUND((y.diff_avg*0.6),0)+ROUND((y.diff_avg2*0.4),0),0) AS new_expect_amt FROM (" +
		"SELECT tr_dt AS tt, real_amt, expect_amt, real_amt-expect_amt AS diff, (SELECT round(SUM(z.diff)/7,0) AS dff_avg" +
		" FROM(" +
		"SELECT tr_dt, real_amt, expect_amt, real_amt-expect_amt AS diff FROM cc_day_sale_sum WHERE biz_num=?" +
		") z " +
		"WHERE z.tr_dt BETWEEN DATE_FORMAT(DATE_SUB(tt, INTERVAL 7 DAY), '%Y%m%d') AND DATE_FORMAT(DATE_SUB(tt, INTERVAL 1 DAY), '%Y%m%d')) AS diff_avg, (" +
		"SELECT round(SUM(z.diff)/7,0) AS dff_avg" +
		" FROM(" +
		"SELECT tr_dt, real_amt, expect_amt, real_amt-expect_amt AS diff FROM cc_day_sale_sum WHERE biz_num=?" +
		") z " +
		"WHERE z.tr_dt BETWEEN DATE_FORMAT(DATE_SUB(tt, INTERVAL 14 DAY), '%Y%m%d') AND DATE_FORMAT(DATE_SUB(tt, INTERVAL 8 DAY), '%Y%m%d')) AS diff_avg2 " +
		"FROM cc_day_sale_sum WHERE biz_num=? ORDER BY tt DESC" +
		") y WHERE y.tt = ?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bizNum, bizNum, tr_dt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var expectAmt string
	for rows.Next() {
		err := rows.Scan(&expectAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getExpectAmt7: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return expectAmt
}

// 과거 한달 매출 예측을 반영한 새로운 예측
func getExpectAmtLastMonth(goID int, bizNum, tr_dt string) (string) {
	statement := "SELECT ifnull(ROUND(y.last1week*0.3,0)+ROUND(y.last2week*0.3,0)+ROUND(y.last3week*0.25,0)+ROUND(y.last4week*0.15,0)+y.expect_amt,0) AS new_expect_amt FROM (" +
		"SELECT z.tr_dt, z.real_amt, z.expect_amt, z.diff," +
		"(SELECT real_amt-expect_amt FROM cc_day_sale_sum WHERE biz_num=? AND tr_dt = DATE_FORMAT(DATE_SUB(z.tr_dt, INTERVAL 7 DAY), '%Y%m%d')) AS last1week," +
		"(SELECT real_amt-expect_amt FROM cc_day_sale_sum WHERE biz_num=? AND tr_dt = DATE_FORMAT(DATE_SUB(z.tr_dt, INTERVAL 14 DAY), '%Y%m%d')) AS last2week," +
		"(SELECT real_amt-expect_amt FROM cc_day_sale_sum WHERE biz_num=? AND tr_dt = DATE_FORMAT(DATE_SUB(z.tr_dt, INTERVAL 21 DAY), '%Y%m%d')) AS last3week," +
		"(SELECT real_amt-expect_amt FROM cc_day_sale_sum WHERE biz_num=? AND tr_dt = DATE_FORMAT(DATE_SUB(z.tr_dt, INTERVAL 28 DAY), '%Y%m%d')) AS last4week " +
		"FROM (" +
		"SELECT tr_dt, real_amt, expect_amt, real_amt-expect_amt AS diff from cc_day_sale_sum WHERE biz_num=? ORDER BY tr_dt desc" +
		") z" +
		") Y WHERE y.tr_dt = ?;"

	rows, err := cls.QueryDBbyParam(statement, bizNum, bizNum, bizNum, bizNum, bizNum, tr_dt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var expectAmt string
	for rows.Next() {
		err := rows.Scan(&expectAmt)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getExpectAmt7: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return expectAmt
}

// 매출합계 데이터 유무 체크
func getDaySumCheck(goID int, bizNum, trDt string) string {
	statement := "SELECT biz_num FROM cc_day_sale_sum WHERE biz_num=? AND tr_dt=?"
	rows, err := cls.QueryDBbyParam(statement, bizNum, trDt)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.QueryDBbyParam error(%s) \n", goID, err.Error())
		return ""
	}
	defer rows.Close()

	var ret string
	for rows.Next() {
		err := rows.Scan(&ret)
		if err != nil {
			lprintf(1, "[ERROR][go-%d] getDaySumCheck: rows.Scan error(%s) \n", goID, err.Error())
			return ""
		}
	}

	return ret
}

// 예상매출 저장
func insertDaySum(goID int, bizNum, trDt, realAmt, expect_amt string) {
	statememt := "INSERT INTO cc_day_sale_sum (biz_num, tr_dt, real_amt, expect_amt, reg_dt) VALUES (?,?,?,?,?)"

	var params []interface{}
	params = append(params, bizNum)
	params = append(params, trDt)
	params = append(params, realAmt)
	params = append(params, expect_amt)
	params = append(params, time.Now().Format("20060102150405"))

	_, err := cls.ExecDBbyParam(statememt, params)
	if err != nil {
		lprintf(1, "[ERROR][go-%d] cls.ExecDBbyParam error(%s) \n", goID, err.Error())
		return
	}
}

// 매출합계 수정
func updateDaySum(goID int, bizNum, trDt, realAmt, expectAmt string) {
	statememt := "UPDATE cc_day_sale_sum SET mod_dt= DATE_FORMAT(NOW(), '%Y%m%d%H%i%s')"

	if realAmt != "" {
		statememt = statememt + ", real_amt = " + realAmt
	}
	if expectAmt != "" {
		statememt = statememt + ", expect_amt = " + expectAmt
	}

	statememt = statememt + " WHERE biz_num = '" + bizNum + "' AND tr_dt = '" + trDt + "'"

	row, err := cls.QueryDB(statememt)
	if err != nil {
		cls.Lprintf(1, "[ERROR][go-%d] %s\n", goID, err.Error())
		return
	}
	defer row.Close()
}

// 매출 예측 수정
func updateExpectAmt(goID int, bizNum, trDt, expectAmt string, expectFlag EXPECTAMT) {
	
	statememt := "UPDATE cc_day_sale_sum SET mod_dt= DATE_FORMAT(NOW(), '%Y%m%d%H%i%s')"

	switch expectFlag {
	case ExpectAmtLastWeek:
		statememt += ", expect_amt_7 = " + expectAmt
	case ExpectAmtLastTwoWeek:
		statememt += ", expect_amt_14 = " + expectAmt
	case ExpectAmtLastMonth:
		statememt += ", expect_amt_28 = " + expectAmt
	default:
		return
	}

	statememt += " WHERE biz_num = '" + bizNum + "' AND tr_dt = '" + trDt + "'"

	row, err := cls.QueryDB(statememt)
	if err != nil {
		cls.Lprintf(1, "[ERROR][go-%d] %s\n", goID, err.Error())
		return
	}
	defer row.Close()
}
