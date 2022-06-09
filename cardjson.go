package main

import (
	"net/http"
)

type LoginResponse struct {
	Cookie []*http.Cookie
}

type GrpIdType struct {
	Code     string `json:"code"`     // KFTC 그룹아이디
	CodeName string `json:"codeName"` // 그룹명
	UseYN    string `json:"useYN"`
	Etc1     string `json:"etc1"`
}

type CompInfoType struct {
	BizNum      string `db:"BIZ_NUM"`        // 사업자번호
	SvcOpenDt   string `db:"SVC_OPEN_DT"`    // 서비스개시일
	LnFirstYn   string `db:"LN_FIRST_YN"`    // 여신협회자료받기최초실행여부
	//LnJoinTy    string `db:"LN_JOIN_TY"`     // 여신협회가입방식 (0=미정,1=직접가입,2=가입대행,3=기가입)
	LnID        string `db:"LN_ID"`          // 접속ID
	LnPsw       string `db:"LN_PSW"`         // 접속PWD
	LnJoinStsCd string `db:"LN_JOIN_STS_CD"` // 여신협회가입상태 (0=가입요청,1=가입완료,2=가입실패)
	BsDt        string `db:"BS_DT"`          // 조회일자
	RegDt       string `db:"REG_DT"`         // 등록일자
	ModDt       string `db:"MOD_DT"`         // 수정일자
	StsCd       string `db:"STS_CD"`         // 조회결과상태 (=가입요청,1=가입완료,2=가입실패)
	ErrCd       string `db:"ERR_CD"`         // 조회결과코드
}

type SyncInfoType struct {
	BizNum  string `db:"BIZ_NUM"`  // 사업자번호
	BsDt    string `db:"BS_DT"`    // 조회일자
	SiteCd  string `db:"SITE_CD"`  // 조회사이트정보
	AprvCnt string `db:"APRV_CNT"` // 승인건수
	AprvAmt string `db:"APRV_AMT"` // 승인금액
	PcaCnt  string `db:"PCA_CNT"`  // 매입건수
	PcaAmt  string `db:"PCA_AMT"`  // 매입금액
	PayCnt  string `db:"PAY_CNT"`  // 입금건수
	PayAmt  string `db:"PAY_AMT"`  // 입금금액
	RegDt   string `db:"REG_DT"`   // 등록일시
	ModDt   string `db:"MOD_DT"`   // 수정일시
	StsCd   string `db:"STS_CD"`   // Sync실행상태
	ErrCd   string `db:"ERR_CD"`   // 에러코드
	SendDt  string `db:"SEND_DT"`  // 전송일자
}

type ApprovalSumType struct {
	ResultSum  ResultSumType    `json:"resultSum"`  // 페이지 리스트
	ResultList []ResultListType `json:"resultList"` // 페이지 리스트
	Error_msg  string           `json:"error_msg"`
}

type ResultSumType struct {
	TotTrnsCnt string `json:"totTrnsCnt" db:"TOT_CNT"`  // 거래 건수
	TotTrnsAmt string `json:"totTrnsAmt" db:"TOT_AMT"`  // 거래 합계
	TotAuthCnt string `json:"totAuthCnt" db:"APRV_CNT"` // 승인 건수
	TotAuthAmt string `json:"totAuthAmt" db:"APRV_AMT"` // 승인 합계
	TotCnclCnt string `json:"totCnclCnt" db:"CAN_CNT"`  // 취소 건수
	TotCnclAmt string `json:"totCnclAmt" db:"CAN_AMT"`  // 취소 합계
}

func (r ResultSumType) compare(data ResultSumType) int {
	if r.TotTrnsCnt != data.TotTrnsCnt {
		return 0
	}
	if r.TotTrnsAmt != data.TotTrnsAmt {
		return 0
	}
	if r.TotAuthCnt != data.TotAuthCnt {
		return 0
	}
	if r.TotAuthAmt != data.TotAuthAmt {
		return 0
	}
	if r.TotCnclCnt != data.TotCnclCnt {
		return 0
	}
	if r.TotCnclAmt != data.TotCnclAmt {
		return 0
	}

	return 1
}

type ResultListType struct {
	CardCo      string `json:"cardCo"      db:"CARD_CD"`  // 카드사 번호
	CardNm      string `json:"cardNm"      db:"CARD_NM"`  // 카드사 이름
	TrnsCnt     string `json:"trnsCnt"     db:"TOT_CNT"`  // 거래 건수
	TrnsAmt     string `json:"trnsAmt"     db:"TOT_AMT"`  // 거래 금액
	AuthCnt     string `json:"authCnt"     db:"APRV_CNT"` // 승인 건수
	AuthAmt     string `json:"authAmt"     db:"APRV_AMT"` // 승인 금액
	CNClCnt     string `json:"cnclCnt"     db:"CAN_CNT"`  // 취소 건수
	CnclAmt     string `json:"cnclAmt"     db:"CAN_AMT"`  // 취소 금액
	StdDate     string `json:"stdDate"`                   // 기준일
	MerNo       string `json:"merNo"`                     // 가맹점 번호
	CardNo      string `json:"cardNo"`                    // 카드번호
	CardRealNo  string `json:"cardRealNo"`                // 실제 카드 번호
	AuthNo      string `json:"authNo"`                    // 승인 번호
	AuthClss    string `json:"authClss"`                  // 승인 종류
	AuthClssNm  string `json:"authClssNm"`                // 승인 종류 이름
	BuzNo       string `json:"buzNo"`                     // 사업자번호
	CardKnd     string `json:"cardKnd"`                   // 카드 종류
	TrnsDate    string `json:"trnsDate"`                  // 거래 일자
	TrnsTime    string `json:"trnsTime"`                  // 거래 시간
	InsTrm      string `json:"insTrm"`                    // 할부개월
	InsTrmNm    string `json:"insTrmNm"`
	WrkAton     string `json:"wrkAton"`
	PcaGubun    int    `json:"pcaGubun"`
	PcaGubunNm  string `json:"pcaGubunNm"`
	AffiCardCo  string `json:"affiCardCo"`
	AffiCardNm  string `json:"affiCardNm"`
	SumTrnsAmt  string `json:"sumTrnsAmt"` // 합계 거래 금액
	SumTrnsCnt  string `json:"sumTrnsCnt"` // 합계 거래 건수
	SumAuthAmt  string `json:"sumAuthAmt"` // 합계 승인 금액
	SumAuthCnt  string `json:"sumAuthCnt"` // 합계 승인 건수
	SumCnclAmt  string `json:"sumCnclAmt"` // 합계 취소 금액
	SumCnclCnt  string `json:"sumCnclCnt"` // 합계 취소 건수
	StdYm       string `json:"stdYm"`
	PositionIdx int    `json:"positionIdx"`
}

type ApprovalDetailType struct {
	Rnum        int    `json:"rnum"        db:"SEQ_NO"`
	TrnsDate    string `json:"trnsDate"    db:"TR_DT"`        // 거래일자
	TrnsTime    string `json:"trnsTime"    db:"TR_TM"`        // 거래시간
	CardCo      string `json:"cardCo"      db:"CARD_CD"`      // 카드사코드
	CardNm      string `json:"cardNm"      db:"CARD_NM"`      // 카드사명
	MerNo       string `json:"merNo"       db:"MER_NO"`       // 가맹점번호
	CardNo      string `json:"cardNo"      db:"CARD_NO"`      // 카드번호
	CardKnd     string `json:"cardKnd"     db:"CARD_KND"`     // 카드종류(1:신용 2:체크 6:선불 9:기타)
	AuthAmt     string `json:"AuthAmt"     db:"APRV_AMT"`     // 승인금액
	AuthNo      string `json:"authNo"      db:"APRV_NO"`      // 승인번호
	AuthClss    string `json:"authClss"    db:"APRV_CLSS"`    // 승인결과코드
	AuthClssNm  string `json:"authClassNm" db:"APRV_CLSS_NM"` // 승인결과명
	StsCd       string `db:"STS_CD"`                          // 승인상태코드(1=승인,2=원거래취소,3=취소)
	OrgTrDt     string `db:"ORG_TR_DT"`                       // 취소시 원거래일자
	PcaYn       string `db:"PCA_YN"`                          // 매입상태(N=미매입,Y=매입)
	InsTrm      string `json:"insTrm"      db:"INST_TRM"`     // 할부개월
	PcaGubun    string `json:"pcaGubun"`                      // 매입구분
	BuzNo       string `json:"buzNo"`                         // 사업자번호
	CardRealNo  string `json:"cardRealNo"`                    // 실제 카드 번호
	InsTrmNm    string `json:"insTrmNm"`
	WrkAton     string `json:"wrkAton"`
	AffiCardCo  string `json:"affiCardCo"`
	AffiCardNm  string `json:"affiCardNm"`
	SelectAmt   string `json:"selectAmt"`  // 선택 금액
	TotalCnt    string `json:"totalCnt"`   // 합계 거래 건수
	SumTrnsCnt  string `json:"sumTrnsCnt"` // 합계 거래 건수
	SumTrnsAmt  string `json:"sumTrnsAmt"` // 합계 거래 금액
	SumAuthCnt  string `json:"sumAuthCnt"` // 합계 승인 건수
	SumAuthAmt  string `json:"sumAuthAmt"` // 합계 승인 금액
	SumCnclCnt  string `json:"sumCnclCnt"` // 합계 취소 건수
	SumCnclAmt  string `json:"sumCnclAmt"` // 합계 취소 금액
	PositionIdx int    `json:"positionIdx"`
	InsTrmNmStr string `json:"insTrmNmStr"`
}

// PurchaseSumType : 매입 조회. 임의로 모델 작성
type PurchaseSumType struct {
	ResultSum  ResultPurSumType    `json:"resultSum"`  // 페이지 리스트
	ResultList []ResultPurListType `json:"resultList"` // 페이지 리스트
	//Error_msg  string              `json:"error_msg"`
}

// 매입 합계
type ResultPurSumType struct {
	PcaCnt      string `json:"pcaCnt"    db:"PCA_CNT"`       // 매입 건수
	PcaScdAmt   string `json:"pcaScdAmt" db:"PCA_AMT"`       // 매입 합계
	MerFee      string `json:"basicFee"  db:"MER_FEE"`       // 가맹점 수수료
	PntFee      string `json:"pointFee"  db:"PNT_FEE"`       // 포인트 수수료
	EtcFee      string `json:"etcFee"    db:"ETC_FEE"`       // 기타 수수료
	TotFee      string `json:"fee"       db:"TOT_FEE"`       // 수수료 합계
	VatAmt      string `json:"vatFee"    db:"VAT_AMT"`       // 부가세 대리 납부 금액
	OuptExptAmt string `json:"pymScdAmt" db:"OUTP_EXPT_AMT"` // 지급 예정 합계
}

func (r ResultPurSumType) compare(data ResultPurSumType) int {
	if r.PcaCnt != data.PcaCnt {
		return 0
	}
	if r.PcaScdAmt != data.PcaScdAmt {
		return 0
	}
	if r.MerFee != data.MerFee {
		return 0
	}
	if r.PntFee != data.PntFee {
		return 0
	}
	if r.EtcFee != data.EtcFee {
		return 0
	}
	if r.TotFee != data.TotFee {
		return 0
	}
	if r.VatAmt != data.VatAmt {
		return 0
	}
	if r.OuptExptAmt != data.OuptExptAmt {
		return 0
	}

	return 1
}

// 매입 카드사별 합계 리스트
type ResultPurListType struct {
	CardCo      string `json:"cardCo"    db:"CARD_CD"`       // 카드사 번호
	CardNm      string `json:"cardNm"    db:"CARD_NM"`       // 카드사
	PcaCnt      string `json:"pcaCnt"    db:"PCA_CNT"`       // 매입 건수
	PcaScdAmt   string `json:"pcaScdAmt" db:"PCA_AMT"`       // 매입 합계
	MerFee      string `json:"basicFee"  db:"MER_FEE"`       // 가맹점 수수료
	PntFee      string `json:"pointFee"  db:"PNT_FEE"`       // 포인트 수수료
	EtcFee      string `json:"etcFee"    db:"ETC_FEE"`       // 기타 수수료
	TotFee      string `json:"fee"       db:"TOT_FEE"`       // 수수료 합계
	VatAmt      string `json:"vatFee"    db:"VAT_AMT"`       // 부가세 대리 납부 금액
	OuptExptAmt string `json:"pymScdAmt" db:"OUTP_EXPT_AMT"` // 지급 예정 합계
}

// 매입 상세 조회
type PurchaseDetailType struct {
	Rnum        int    `json:"rnum"         db:"SEQ_NO"`       // 번호
	TrnsDate    string `json:"trnsDate"     db:"TR_DT"`        // 거래일자(원승인일자)
	OrgTrDt     string `db:"ORG_TR_DT"`                        // 실거래일자
	MerNo       string `json:"merNo"        db:"MER_NO"`       // 가맹점번호
	AuthClss    string `json:"authClss"     db:"APRV_CLSS"`    // 승인종류
	StsCd       string `db:"STS_CD"`                           // 매입상태코드
	PcaDate     string `json:"pcaDate"      db:"PCA_DT"`       // 매입일자
	PcaAmt      string `json:"pcaAmt"       db:"PCA_AMT"`      // 매입금액
	AuthNo      string `json:"authNo"       db:"APRV_NO"`      // 승인번호
	MerFee      string `json:"basicFee"     db:"MER_FEE"`      // 가맹점 수수료
	PntFee      string `json:"pointFee"     db:"PNT_FEE"`      // 포인트 수수료
	PntFeeRate  string `json:"pointFeeRate" db:"PNT_FEE_RT"`   // 포인트 수수료 비율
	EtcFee      string `json:"etcFee"       db:"ETC_FEE"`      // 기타 수수료
	TotFee      string `json:"fee"          db:"TOT_FEE"`      // 수수료 합계
	SvcFee      string `json:"svcFee"       db:"SVC_FEE"`      // 봉사료
	VatAmt      string `json:"vatFee"       db:"VAT_AMT"`      // 부가세 대리 납부 금액
	PayAmt      string `json:"pymAmt"       db:"PAY_AMT"`      // 지금금액
	OutpExptDt  string `json:"pymScdDate"   db:"OUTP_EXPT_DT"` // 지금예정일
	CardCo      string `json:"cardCo"       db:"CARD_CD"`      // 카드사번호
	CardNm      string `json:"cardNm"       db:"CARD_NM"`      // 카드사
	CardNo      string `json:"cardNo"       db:"CARD_NO"`      // 카드번호
	CardClss    string `json:"cardClss"     db:"CARD_KND"`     // 카드종류 (1:신용 2:체크 6:선불 9:기타)
	TradeGubun  string `json:"tradeGubun"   db:"TRADE_CD"`     // MPM QR 결제여부* (tradeGubun 이 "08" 일때 true, 아닐때 false
	FraudYn     string `db:"FRAUD_YN"`                         // 부정거래 의심대상 유무
	AffiCardCo  string `json:"affiCardCo"`                     // 제휴 카드사 번호
	AffiCardNm  string `json:"affiCardNm"`                     // 제휴 카드사
	PositionIdx int    `json:"positionIdx"`                    // 역순번호 (1 -> 87, 2 -> 86, 3 -> 85...)
	BuzNo       string `json:"buzNo"`                          // 사업자번호
	WrkAton     string `json:"wrkAton"`
}

// PaymentSumType : 입금 조회
type PaymentSumType struct {
	ResultList []PaymentResultListType `json:"resultList"` // 페이지 리스트
}

type PaymentResultListType struct {
	PayDt       string `json:"dt"        db:"PAY_DT"`       // 입금일자
	PcaCnt      string `json:"pcaCnt"    db:"PCA_CNT"`      // 매출건수
	PcaAmt      string `json:"pcaAmt"    db:"PCA_AMT"`      // 매출합계
	PayAmt      string `json:"rcpScdAmt" db:"REAL_PAY_AMT"` // 입금합계
	CardCo      string `json:"cardCo"`
	UnpdClss    string `json:"unpdClss"`
	WrkAton     string `json:"wrkAton"`
	Ym          string `json:"ym"`
	PositionIdx int    `json:"positionIdx"`
}

func (r PaymentResultListType) compare(data PaymentResultListType) int {
	if r.PayDt != data.PayDt {
		return 0
	}
	if r.PcaCnt != data.PcaCnt {
		return 0
	}
	if r.PcaAmt != data.PcaAmt {
		return 0
	}
	if r.PayAmt != data.PayAmt {
		return 0
	}

	return 1
}

// PaymentDetailType : 입금 상세 조회
type PaymentDetailType struct {
	Num         string `json:"num"       db:"SEQ_NO"`       // 번호
	PayDt       string `json:"pymDate"   db:"PAY_DT"`       // 입금일자
	CardNm      string `json:"cardNm"    db:"CARD_NM"`      // 카드사
	MerNo       string `json:"merNo"     db:"MER_NO"`       // 가맹점번호
	StlBankNm   string `json:"stlBankNm" db:"STL_BANK_NM"`  // 결제은행
	StlAcctNo   string `json:"stlAcctNo" db:"STL_ACCT_NO"`  // 결제계좌
	PcaCnt      string `json:"pcaCnt"    db:"PCA_CNT"`      // 매출건수
	PcaAmt      string `json:"pcaAmt"    db:"PCA_AMT"`      // 매출금액
	RsvAmt      string `json:"rsvAmt"    db:"RSV_AMT"`      // 보류금액
	VatAmt      string `json:"vatFeeAmt" db:"VAT_AMT"`      // 부가세 대리 납부 금액
	EtcAmt      string `json:"etcAmt"    db:"ETC_AMT"`      // 기타입금
	PayAmt      string `json:"rcpAmt"    db:"REAL_PAY_AMT"` // 실입금
	RcpScdAmt   string `json:"rcpScdAmt"`                   // 실입금 (rcpAmt 와 일치함)
	TotalCnt    string `json:"totalCnt"`                    // 묶여서 조회된 건수
	SelectAmt   string `json:"selectAmt"`                   // 선택한 입금 일자의 입금 합계
	PositionIdx int    `json:"positionIdx"`                 // 역순 번호 (1 -> 87, 2 -> 86, 3 -> 85...)
	Dt          string `json:"dt"`
}
