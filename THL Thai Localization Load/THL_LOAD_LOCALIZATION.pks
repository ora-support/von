CREATE OR REPLACE PACKAGE APPS."THL_LOAD_LOCALIZATION"
IS
/*==================================================================
  PROCEDURE NAME: THL_LOAD_TAXINV_GOODS

  DESCRIPTION:    This Api for load data from Tax Invoices Goods to
                Thai Localization

  PARAMETERS:  P_CONC_REQUEST_ID  IN    NUMBER,
      P_INVOICE_ID    IN    NUMBER,

  DESIGN
  REFERENCES:

  CHANGE       Created     01-OCT-2008 DAMRONG TANTIWIWATSAKUL
  HISTORY:

=======================================================================*/
   PROCEDURE thl_load_taxinv_goods (
      p_conc_request_id   IN   NUMBER,
      p_invoice_id        IN   NUMBER
   );

/*==================================================================
  PROCEDURE NAME: THL_LOAD_TAXINV_SERVICE

  DESCRIPTION:    This Api for load data from Tax Invoices Service to
                Thai Localization

  PARAMETERS:  P_CONC_REQUEST_ID  IN    NUMBER,
      P_CHECK_ID   IN    NUMBER,

  DESIGN
  REFERENCES:

  CHANGE       Created     01-OCT-2008 DAMRONG TANTIWIWATSAKUL
  HISTORY:

=======================================================================*/
   PROCEDURE thl_load_taxinv_service (
      p_conc_request_id   IN   NUMBER,
      p_check_id          IN   NUMBER
   );

/*==================================================================
  PROCEDURE NAME: THL_LOAD_WHT_M

  DESCRIPTION:    This Api for load data from Withholding Tax Type Manual to
                Thai Localization

  PARAMETERS:  P_CONC_REQUEST_ID  IN    NUMBER,
      P_INVOICE_ID    IN    NUMBER,

  DESIGN
  REFERENCES:

  CHANGE       Created     01-OCT-2008 DAMRONG TANTIWIWATSAKUL
  HISTORY:

=======================================================================*/
   PROCEDURE thl_load_wht_m (
      p_conc_request_id   IN   NUMBER,
      p_invoice_id        IN   NUMBER
   );

/*==================================================================
  PROCEDURE NAME: THL_LOAD_WHT_A

  DESCRIPTION:    This Api for load data from Withholding Tax Type Auto to
                Thai Localization

  PARAMETERS:  P_CONC_REQUEST_ID  IN    NUMBER,
      P_CHECK_ID   IN    NUMBER,

  DESIGN
  REFERENCES:

  CHANGE       Created     01-OCT-2008 DAMRONG TANTIWIWATSAKUL
  HISTORY:

=======================================================================*/
   PROCEDURE thl_load_wht_a (p_conc_request_id IN NUMBER, p_check_id IN NUMBER);

/*==================================================================
  PROCEDURE NAME: write_log

  DESCRIPTION:    This Api for write message to log file

  PARAMETERS:  param_msg    IN    VARCHAR2

  DESIGN
  REFERENCES:

  CHANGE       Created     01-OCT-2008 DAMRONG TANTIWIWATSAKUL
  HISTORY:

=======================================================================*/
   PROCEDURE write_log (param_msg VARCHAR2);
END thl_load_localization;

/