/* Formatted on 10/6/2017 11:12:35 AM (QP5 v5.256.13226.35538)

Step before run fix

             
UPDATE ap_invoices_all SET attribute4 = 'Y' WHERE invoice_id = :p_invoice_id; 

 */
DECLARE
   p_conc_request_id               NUMBER := 0;
   p_check_id                      NUMBER := 1281127;
   user_id                         NUMBER := 1091;
   appl_id                         NUMBER;
   org_id                          NUMBER;
   v_head_id                       NUMBER;
   v_line_id                       NUMBER;
   v_base_amt                      NUMBER;
   v_non_rec_tax                   NUMBER;
   v_rec_tax                       NUMBER;
   v_non_rec_amt                   NUMBER;
   v_pi_result                     NUMBER (13, 10);
   v_rec_amt                       NUMBER;
   v_tax_amt                       NUMBER;
   v_payment_amt                   NUMBER;
   v_invoice_amt                   NUMBER;
   v_project_id                    NUMBER;
   v_bproject                      BOOLEAN;
   v_insert                        BOOLEAN;
   v_sts                           BOOLEAN;
   v_paid_percent                  NUMBER;
   v_paid_base_inv                 NUMBER;
   v_paid_rec_tax                  NUMBER;
   v_paid_nonrec_tax               NUMBER;
   v_check_base_inv                NUMBER;
   v_check_rec_tax                 NUMBER;
   v_check_nonrec_tax              NUMBER;
   v_pay_act                       NUMBER;
   v_query                         VARCHAR2 (800);
   cursor1                         INTEGER;
   rows_processed                  INTEGER;
   v_index                         NUMBER;
   v_ai_invoice_id                 NUMBER (15);
   v_ai_invoice_amount             NUMBER;
   v_ai_payment_status_flag        VARCHAR2 (1 BYTE);
   v_ai_invoice_type_lookup_code   VARCHAR2 (25 BYTE);
   v_nperiod                       VARCHAR (10);

   -- Cursor for Load Data to Table : thl_taxinvoices_header
   CURSOR cursor_head
   IS
        SELECT ac.check_id transaction_id,
               ac.check_number transaction_number,
               ac.check_date transaction_date,
               'S' type_tax,
               ail.tax_classification_code tax_code,
               ac.doc_sequence_value voucher_number,
               NVL (atc.percentage_rate, 0) tax_recovery_rate,
               hla.location_id location_id,
               cba.bank_account_num bank_acct_number,
               ac.bank_account_name bank_acct_name,
               ac.vendor_name supplier_name,
               1091 created_by,
               SYSDATE creation_date,
               1091 last_updated_by,
               SYSDATE last_update_date,
               1091 last_update_login,
               thl_load_get_taxrate(ai.invoice_id)  tax_rate,  /*modify by spw@ice*/
               ai.gl_date rcp_tax_inv_date
          FROM ap_invoices_all ai,
               ap_invoice_lines_all ail,
               ap_invoice_distributions_all aid,
               (  SELECT a.tax_rate_code tax,
                         NVL (b.percentage_rate, 0) percentage_rate
                    FROM zx_rates_b a, zx_rates_b b
                   WHERE     a.active_flag = 'Y'
                         AND NVL (a.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND a.default_rec_rate_code = b.tax_rate_code(+)
                         AND b.rate_type_code(+) = 'RECOVERY'
                         AND b.active_flag(+) = 'Y'
                         AND b.effective_from(+) <= SYSDATE
                         AND NVL (b.effective_to(+), SYSDATE) >= SYSDATE
                GROUP BY a.tax_rate_code, b.percentage_rate) atc,
               zx_rates_b zrb,
               ap_invoice_payments aip,
               ap_checks ac,
               ce_bank_accounts cba,
               ce_bank_acct_uses_all cbau,
               ce_bank_branches_v cbb,
               hr_locations hla,
--               (  SELECT invoice_id,
--                         tax_rate_id,
--                         tax_rate_code,
--                         tax_rate
--                    FROM ap_invoice_lines_all
--                   WHERE line_type_lookup_code = 'TAX'
--                GROUP BY invoice_id,
--                         tax_rate_id,
--                         tax_rate_code,
--                         tax_rate) inv_tax,
               xla_distribution_links xla
         WHERE     ai.invoice_id = ail.invoice_id
               AND ail.invoice_id = aid.invoice_id
               AND ail.line_number = aid.invoice_line_number
               AND ail.tax_classification_code = atc.tax(+)
               AND ail.tax_classification_code = zrb.tax_rate_code
               AND zrb.active_flag = 'Y'
               AND zrb.def_rec_settlement_option_code = 'DEFERRED'
               AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
               AND ail.line_type_lookup_code IN ('ITEM')
               AND ai.invoice_id IN (SELECT invoice_id
                                       FROM ap_invoice_lines_all
                                      WHERE     invoice_id = ai.invoice_id
                                            AND line_type_lookup_code = 'TAX')
               AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y',
                                                                          'P')
               AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
               AND ap_invoices_pkg.get_approval_status (
                      ai.invoice_id,
                      ai.invoice_amount,
                      ai.payment_status_flag,
                      ai.invoice_type_lookup_code) IN ('APPROVED',
                                                       'CANCELLED',
                                                       'AVAILABLE')
               AND ( (aip.invoice_payment_id,
                      0,
                      'AP_INVOICE_PAYMENTS_SERVICE',
                      ac.check_id) IN (SELECT tidld.reference_id,  /*modify by spw@ice 'NOT IN' to 'IN'*/
                                                  0,
                                                  tidld.reference_type,
                                                  tidld.check_id
                                             FROM thl_data_loaded tidld
                                            WHERE     tidld.reference_type =
                                                         'AP_INVOICE_PAYMENTS_SERVICE'
                                                  AND tidld.reference_id =
                                                         aip.invoice_payment_id))
               AND ai.invoice_id = aip.invoice_id
               AND aip.check_id = ac.check_id
               AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
               AND cbau.bank_account_id = cba.bank_account_id
               AND cbb.branch_party_id = cba.bank_branch_id
--               AND ai.invoice_id = inv_tax.invoice_id
--               AND ail.tax_classification_code = inv_tax.tax_rate_code
--               AND zrb.tax_rate_id = inv_tax.tax_rate_id
               AND xla.source_distribution_id_num_1 =
                      aid.invoice_distribution_id
               AND xla.rounding_class_code = 'LIABILITY'
               AND xla.source_distribution_type = 'AP_INV_DIST'
--               AND (xla.event_id,
--                    0,
--                    'XLA_DISTRIBUTION_LINKS_SERVICE',
--                    ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                0,
--                                                tidld.reference_type,
--                                                tidld.check_id
--                                           FROM thl_data_loaded tidld
--                                          WHERE     tidld.reference_type =
--                                                       'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                AND tidld.reference_id =
--                                                       xla.event_id)
               AND ac.check_id = p_check_id
               AND zrb.description = hla.location_code
      GROUP BY ac.check_id,
               ac.check_date,
               ail.tax_classification_code,
               hla.location_id,
               ac.doc_sequence_value,
               cba.bank_account_num,
               ac.bank_account_name,
               ac.vendor_name,
               atc.percentage_rate,
               ac.check_number,
               thl_load_get_taxrate(ai.invoice_id),  /*modify by spw@ice*/
               ai.gl_date
      ORDER BY ac.doc_sequence_value;

   rec_head                        cursor_head%ROWTYPE;

   -- Cursor for Load Data to Table : thl_taxinvoices_line
   CURSOR cursor_invoice (
      v_transaction_id      NUMBER,
      v_tax_code            VARCHAR2,
      v_voucher_number      NUMBER,
      v_location_id         NUMBER,
      v_bank_acct_number    VARCHAR2,
      v_supplier_name       VARCHAR2)
   IS
        SELECT ac.check_id transaction_id,
               ac.check_number transaction_number,
               ac.check_date transaction_date,
               ai.invoice_id,
               ai.invoice_num,
               'S' type_tax,
               ail.tax_classification_code tax_code,
               ac.doc_sequence_value voucher_number,
               NVL (atc.percentage_rate, 0),
               hla.location_id location_id,
               cba.bank_account_num bank_acct_number,
               ac.bank_account_name bank_acct_name,
               ail.project_id,
               ac.vendor_name supplier_name,
               1091 created_by,
               SYSDATE creation_date,
               1091 last_updated_by,
               SYSDATE last_update_date,
               1091 last_update_login
          FROM ap_invoices_all ai,
               ap_invoice_lines_all ail,
               ap_invoice_distributions_all aid,
               (  SELECT a.tax_rate_code tax,
                         NVL (b.percentage_rate, 0) percentage_rate
                    FROM zx_rates_b a, zx_rates_b b
                   WHERE     a.active_flag = 'Y'
                         AND NVL (a.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND a.default_rec_rate_code = b.tax_rate_code(+)
                         AND b.rate_type_code(+) = 'RECOVERY'
                         AND b.active_flag(+) = 'Y'
                         AND b.effective_from(+) <= SYSDATE
                         AND NVL (b.effective_to(+), SYSDATE) >= SYSDATE
                GROUP BY a.tax_rate_code, b.percentage_rate) atc,
               zx_rates_b zrb,
               ap_invoice_payments aip,
               ap_checks ac,
               ce_bank_accounts cba,
               ce_bank_acct_uses_all cbau,
               ce_bank_branches_v cbb,
               hr_locations hla,
               (  SELECT invoice_id, tax_rate_id, tax_rate_code
                    FROM ap_invoice_lines_all
                   WHERE line_type_lookup_code = 'TAX'
                GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
               xla_distribution_links xla
         WHERE     ai.invoice_id = ail.invoice_id
               AND ail.invoice_id = aid.invoice_id
               AND ail.line_number = aid.invoice_line_number
               AND ail.tax_classification_code = atc.tax(+)
               AND ail.tax_classification_code = zrb.tax_rate_code
               AND zrb.def_rec_settlement_option_code = 'DEFERRED'
               AND zrb.active_flag = 'Y'
               AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
               AND ail.line_type_lookup_code IN ('ITEM')
               AND ai.invoice_id IN (SELECT invoice_id
                                       FROM ap_invoice_lines_all
                                      WHERE     invoice_id = ai.invoice_id
                                            AND line_type_lookup_code = 'TAX')
               AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y',
                                                                          'P')
               AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
               AND ap_invoices_pkg.get_approval_status (
                      ai.invoice_id,
                      ai.invoice_amount,
                      ai.payment_status_flag,
                      ai.invoice_type_lookup_code) IN ('APPROVED',
                                                       'CANCELLED',
                                                       'AVAILABLE')
               AND ( ( (aip.invoice_payment_id,
                        0,
                        'AP_INVOICE_PAYMENTS_SERVICE',
                        ac.check_id) IN (SELECT tidld.reference_id,      /*modify by spw@ice 'NOT IN' to 'IN'*/
                                                    0,
                                                    tidld.reference_type,
                                                    tidld.check_id
                                               FROM thl_data_loaded tidld
                                              WHERE     tidld.reference_type =
                                                           'AP_INVOICE_PAYMENTS_SERVICE'
                                                    AND tidld.reference_id =
                                                           aip.invoice_payment_id)))
               AND ai.invoice_id = aip.invoice_id
               AND aip.check_id = ac.check_id
               AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
               AND cbau.bank_account_id = cba.bank_account_id
               AND cbb.branch_party_id = cba.bank_branch_id
               AND ai.invoice_id = inv_tax.invoice_id
               AND ail.tax_classification_code = inv_tax.tax_rate_code
               AND zrb.tax_rate_id = inv_tax.tax_rate_id
               AND xla.source_distribution_id_num_1 =
                      aid.invoice_distribution_id
               AND xla.rounding_class_code = 'LIABILITY'
               AND xla.source_distribution_type = 'AP_INV_DIST'
--               AND (xla.event_id,
--                    0,
--                    'XLA_DISTRIBUTION_LINKS_SERVICE',
--                    ac.check_id) NOT IN (SELECT tidld.reference_id,     
--                                                0,
--                                                tidld.reference_type,
--                                                tidld.check_id
--                                           FROM thl_data_loaded tidld
--                                          WHERE     tidld.reference_type =
--                                                       'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                AND tidld.reference_id =
--                                                       xla.event_id)
               AND ac.check_id = p_check_id
               AND zrb.description = hla.location_code
               AND NVL (ac.check_id, 0) = NVL (v_transaction_id, 0)
               AND NVL (ail.tax_classification_code, '0') =
                      NVL (v_tax_code, '0')
               AND NVL (ac.doc_sequence_value, '0') =
                      NVL (v_voucher_number, '0')
               AND NVL (hla.location_id, 0) = NVL (v_location_id, 0)
               AND NVL (cba.bank_account_num, '0') =
                      NVL (v_bank_acct_number, '0')
               AND NVL (ac.vendor_name, '0') = NVL (v_supplier_name, '0')
      GROUP BY ac.check_id,
               ac.check_date,
               ai.invoice_id,
               ai.invoice_num,
               ail.tax_classification_code,
               hla.location_id,
               ac.doc_sequence_value,
               cba.bank_account_num,
               ac.bank_account_name,
               ail.project_id,
               ac.vendor_name,
               atc.percentage_rate,
               ac.check_number
      ORDER BY ac.doc_sequence_value;

   rec_invoice                     cursor_invoice%ROWTYPE;

   -- Cursor for Load Data to Table Line : thl_taxinvoices_line
   CURSOR cursor_invoice_line (
      p_invoice_id    NUMBER)
   IS
        SELECT ac.check_id transaction_id,
               ac.check_number transaction_number,
               ac.check_date transaction_date,
               ai.invoice_id,
               ai.invoice_num,
               'S' type_tax,
               ail.tax_classification_code tax_code,
               ac.doc_sequence_value voucher_number,
               NVL (atc.percentage_rate, 0),
               hla.location_id location_id,
               cba.bank_account_num bank_acct_number,
               ac.bank_account_name bank_acct_name,
               ail.project_id,
               NVL (ail.attribute2, ac.vendor_name) supplier_name,
               1091 created_by,
               SYSDATE creation_date,
               1091 last_updated_by,
               SYSDATE last_update_date,
               1091 last_update_login,
               DECODE (
                  INSTR (ail.tax_classification_code, '36'),
                  0, NVL (
                          NVL (xla.unrounded_accounted_cr, 0)
                        - NVL (xla.unrounded_accounted_dr, 0),
                        0),
                  NVL (
                       NVL (xla.unrounded_entered_cr, 0)
                     - NVL (xla.unrounded_entered_dr, 0),
                     0))
                  base_amt,
               DECODE (
                  INSTR (ail.tax_classification_code, '36'),
                  0,   ROUND (
                            ROUND (
                                 ROUND (
                                      ROUND (
                                           NVL (
                                                NVL (
                                                   xla.unrounded_accounted_cr,
                                                   0)
                                              - NVL (
                                                   xla.unrounded_accounted_dr,
                                                   0),
                                              0)
                                         * atc.tax_rate,
                                         2)
                                    / 100,
                                    2)
                               * atc.percentage_rate,
                               2)
                          / 100,
                          2)
                     + ROUND (
                            ROUND (
                                 ROUND (
                                      ROUND (
                                           NVL (
                                                NVL (
                                                   xla.unrounded_accounted_cr,
                                                   0)
                                              - NVL (
                                                   xla.unrounded_accounted_dr,
                                                   0),
                                              0)
                                         * atc.tax_rate,
                                         2)
                                    / 100,
                                    2)
                               * (100 - atc.percentage_rate),
                               2)
                          / 100,
                          2),
                    ROUND (
                         ROUND (
                              ROUND (
                                   ROUND (
                                        NVL (
                                             NVL (xla.unrounded_entered_cr, 0)
                                           - NVL (xla.unrounded_entered_dr, 0),
                                           0)
                                      * atc.tax_rate,
                                      2)
                                 / 100,
                                 2)
                            * atc.percentage_rate,
                            2)
                       / 100,
                       2)
                  + ROUND (
                         ROUND (
                              ROUND (
                                   ROUND (
                                        NVL (
                                             NVL (xla.unrounded_entered_cr, 0)
                                           - NVL (xla.unrounded_entered_dr, 0),
                                           0)
                                      * atc.tax_rate,
                                      2)
                                 / 100,
                                 2)
                            * (100 - atc.percentage_rate),
                            2)
                       / 100,
                       2))
                  tax_amt                 --Add by Suwatchai 11-APR-12   V 2.0
                         ,
               (SELECT NVL (
                          SUM (
                               NVL (xdl.unrounded_entered_dr, 0)
                             - NVL (xdl.unrounded_entered_cr, 0)),
                          0)
                  FROM xla_distribution_links xdl,
                       xla_ae_headers xeh,
                       xla.xla_transaction_entities xte
                 WHERE     xte.application_id = 200
                       AND xdl.application_id = xeh.application_id
                       AND xte.application_id = xeh.application_id
                       AND xdl.ae_header_id = xeh.ae_header_id
                       AND xte.entity_code = 'AP_PAYMENTS'
                       AND xte.source_id_int_1 = ac.check_id   -------- 254762
                       AND xte.entity_id = xeh.entity_id
                       AND xdl.rounding_class_code IN ('DEF_REC_TAX',
                                                       'LIABILITY')
                       AND xdl.tax_line_ref_id IS NOT NULL
                       AND xdl.applied_to_source_id_num_1 = ai.invoice_id --4597390
                       AND xdl.applied_to_dist_id_num_1 IN (SELECT aid2.invoice_distribution_id
                                                              FROM ap_invoice_distributions_all aid2
                                                             WHERE     aid2.CHARGE_APPLICABLE_TO_DIST_ID =
                                                                          aid.invoice_distribution_id -- 7998263
                                                                   AND aid2.invoice_id =
                                                                          ai.invoice_id --4597390
                                                                   AND aid2.LINE_TYPE_LOOKUP_CODE =
                                                                          'REC_TAX'))
                  tax_amt2           -- Added by Suwatchai  V.2.0    10-APR-12
          --
          FROM ap_invoices_all ai,
               ap_invoice_lines_all ail,
               ap_invoice_distributions_all aid,
               (  SELECT a.tax_rate_code tax,
                         NVL (b.percentage_rate, 0) percentage_rate,
                         NVL (a.percentage_rate, 0) tax_rate
                    FROM zx_rates_b a, zx_rates_b b
                   WHERE     a.active_flag = 'Y'
                         AND NVL (a.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND a.default_rec_rate_code = b.tax_rate_code(+)
                         AND b.rate_type_code(+) = 'RECOVERY'
                         AND b.active_flag(+) = 'Y'
                         AND b.effective_from(+) <= SYSDATE
                         AND NVL (b.effective_to(+), SYSDATE) >= SYSDATE
                GROUP BY a.tax_rate_code,
                         NVL (b.percentage_rate, 0),
                         NVL (a.percentage_rate, 0)) atc,
               zx_rates_b zrb,
               ap_invoice_payments aip,
               ap_checks ac,
               ce_bank_accounts cba,
               ce_bank_acct_uses_all cbau,
               ce_bank_branches_v cbb,
               hr_locations hla,
               (  SELECT invoice_id, tax_rate_id, tax_rate_code
                    FROM ap_invoice_lines_all
                   WHERE line_type_lookup_code = 'TAX'
                GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
               xla_distribution_links xla
         WHERE     ai.invoice_id = ail.invoice_id
               AND ail.invoice_id = aid.invoice_id
               AND ail.line_number = aid.invoice_line_number
               AND ail.tax_classification_code = atc.tax(+)
               AND ail.tax_classification_code = zrb.tax_rate_code
               AND zrb.def_rec_settlement_option_code = 'DEFERRED'
               AND zrb.active_flag = 'Y'
               AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
               AND ail.line_type_lookup_code IN ('ITEM')
               AND ai.invoice_id IN (SELECT invoice_id
                                       FROM ap_invoice_lines_all
                                      WHERE     invoice_id = ai.invoice_id
                                            AND line_type_lookup_code = 'TAX')
               AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y',
                                                                          'P')
               AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
               AND ap_invoices_pkg.get_approval_status (
                      ai.invoice_id,
                      ai.invoice_amount,
                      ai.payment_status_flag,
                      ai.invoice_type_lookup_code) IN ('APPROVED',
                                                       'CANCELLED',
                                                       'AVAILABLE')
               AND ( ( (aip.invoice_payment_id,
                        0,
                        'AP_INVOICE_PAYMENTS_SERVICE',
                        ac.check_id) IN (SELECT tidld.reference_id,    /*modify by spw@ice 'NOT IN' to 'IN'*/
                                                    0,
                                                    tidld.reference_type,
                                                    tidld.check_id
                                               FROM thl_data_loaded tidld
                                              WHERE     tidld.reference_type =
                                                           'AP_INVOICE_PAYMENTS_SERVICE'
                                                    AND tidld.reference_id =
                                                           aip.invoice_payment_id)))
               AND ai.invoice_id = aip.invoice_id
               AND aip.check_id = ac.check_id
               AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
               AND cbau.bank_account_id = cba.bank_account_id
               AND cbb.branch_party_id = cba.bank_branch_id
               AND ai.invoice_id = inv_tax.invoice_id
               AND ail.tax_classification_code = inv_tax.tax_rate_code
               AND zrb.tax_rate_id = inv_tax.tax_rate_id
               AND xla.source_distribution_id_num_1 =
                      aid.invoice_distribution_id
               AND xla.rounding_class_code = 'LIABILITY'
               AND xla.source_distribution_type = 'AP_INV_DIST'
--               AND (xla.event_id,
--                    0,
--                    'XLA_DISTRIBUTION_LINKS_SERVICE',
--                    ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                0,
--                                                tidld.reference_type,
--                                                tidld.check_id
--                                           FROM thl_data_loaded tidld
--                                          WHERE     tidld.reference_type =
--                                                       'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                AND tidld.reference_id =
--                                                       xla.event_id)
               AND ac.check_id = p_check_id
               AND ai.invoice_id = p_invoice_id
               AND zrb.description = hla.location_code
      ORDER BY ail.line_number;

   rec_invoice_line                cursor_invoice_line%ROWTYPE;

   -- Cursor for Load Data to Table : thl_data_loaded
   CURSOR cursor_invoice_dis
   IS
      SELECT DISTINCT ai.invoice_id,
                      aid.invoice_distribution_id,
                      aip.invoice_payment_id,
                      ac.check_id
        FROM ap_invoices_all ai,
             ap_invoice_lines_all ail,
             ap_invoice_distributions_all aid,
             (  SELECT a.tax_rate_code tax,
                       NVL (b.percentage_rate, 0) percentage_rate
                  FROM zx_rates_b a, zx_rates_b b
                 WHERE     a.active_flag = 'Y'
                       AND NVL (a.vat_transaction_type_code, 'NULL') <> 'N/A'
                       AND a.default_rec_rate_code = b.tax_rate_code(+)
                       AND b.rate_type_code(+) = 'RECOVERY'
                       AND b.active_flag(+) = 'Y'
                       AND b.effective_from(+) <= SYSDATE
                       AND NVL (b.effective_to(+), SYSDATE) >= SYSDATE
              GROUP BY a.tax_rate_code, b.percentage_rate) atc,
             zx_rates_b zrb,
             ap_invoice_payments aip,
             ap_checks ac,
             ce_bank_accounts cba,
             ce_bank_acct_uses_all cbau,
             ce_bank_branches_v cbb,
             hr_locations hla,
--             (  SELECT invoice_id, tax_rate_id, tax_rate_code
--                  FROM ap_invoice_lines_all
--                 WHERE line_type_lookup_code = 'TAX'
--              GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
             xla_distribution_links xla
       WHERE     ai.invoice_id = ail.invoice_id
             AND ail.invoice_id = aid.invoice_id
             AND ail.line_number = aid.invoice_line_number
             AND ail.tax_classification_code = atc.tax(+)
             AND ail.tax_classification_code = zrb.tax_rate_code
             AND zrb.def_rec_settlement_option_code = 'DEFERRED'
             AND zrb.active_flag = 'Y'
             AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
             AND ail.line_type_lookup_code IN ('ITEM')
             AND ai.invoice_id IN (SELECT invoice_id
                                     FROM ap_invoice_lines_all
                                    WHERE     invoice_id = ai.invoice_id
                                          AND line_type_lookup_code = 'TAX')
             AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y',
                                                                        'P')
             AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
             AND ap_invoices_pkg.get_approval_status (
                    ai.invoice_id,
                    ai.invoice_amount,
                    ai.payment_status_flag,
                    ai.invoice_type_lookup_code) IN ('APPROVED',
                                                     'CANCELLED',
                                                     'AVAILABLE')
             AND ( (aip.invoice_payment_id,
                    0,
                    'AP_INVOICE_PAYMENTS_SERVICE',
                    ac.check_id) IN (SELECT tidld.reference_id,         /*modify by spw@ice 'NOT IN' to 'IN'*/
                                                0,
                                                tidld.reference_type,
                                                tidld.check_id
                                           FROM thl_data_loaded tidld
                                          WHERE     tidld.reference_type =
                                                       'AP_INVOICE_PAYMENTS_SERVICE'
                                                AND tidld.reference_id =
                                                       aip.invoice_payment_id))
             AND ai.invoice_id = aip.invoice_id
             AND aip.check_id = ac.check_id
             AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
             AND cbau.bank_account_id = cba.bank_account_id
             AND cbb.branch_party_id = cba.bank_branch_id
--             AND ai.invoice_id = inv_tax.invoice_id
--             AND ail.tax_classification_code = inv_tax.tax_rate_code
--             AND zrb.tax_rate_id = inv_tax.tax_rate_id
             AND xla.source_distribution_id_num_1 =
                    aid.invoice_distribution_id
             AND xla.rounding_class_code = 'LIABILITY'
             AND xla.source_distribution_type = 'AP_INV_DIST'
--             AND (xla.event_id,
--                  0,
--                  'XLA_DISTRIBUTION_LINKS_SERVICE',
--                  ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                              0,
--                                              tidld.reference_type,
--                                              tidld.check_id
--                                         FROM thl_data_loaded tidld
--                                        WHERE     tidld.reference_type =
--                                                     'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                              AND tidld.reference_id =
--                                                     xla.event_id)
             AND ac.check_id = p_check_id
             AND zrb.description = hla.location_code;

   rec_invoice_dis                 cursor_invoice_dis%ROWTYPE;
   v_group                         VARCHAR2 (50);
BEGIN
   v_insert := FALSE;
   user_id := fnd_profile.VALUE ('USER_ID');
   org_id := fnd_profile.VALUE ('ORG_ID');
   appl_id := fnd_profile.VALUE ('RESP_APPL_ID');
   
   OPEN cursor_head;
   DBMS_OUTPUT.put_line('CURSOR_HEAD row count : ' || cursor_head%ROWCOUNT); 
   LOOP
      FETCH cursor_head INTO rec_head;

      EXIT WHEN cursor_head%NOTFOUND;

      SELECT thl_taxinvh_s.NEXTVAL INTO v_head_id FROM DUAL;

      v_check_base_inv := 0;
      v_check_rec_tax := 0;
      v_check_nonrec_tax := 0;
      v_paid_percent := 0;
      v_paid_base_inv := 0;
      v_paid_rec_tax := 0;
      v_paid_nonrec_tax := 0;
--      write_log ('Check Number : ' || rec_head.transaction_number);
      
      DBMS_OUTPUT.put_line('Check transaction_id : ' || rec_head.transaction_id);
      DBMS_OUTPUT.put_line('Check tax_code : ' || rec_head.tax_code);
      DBMS_OUTPUT.put_line('Check voucher_number : ' || rec_head.voucher_number);
      DBMS_OUTPUT.put_line('Check location_id : ' || rec_head.location_id);
      DBMS_OUTPUT.put_line('Check bank_acct_number : ' || rec_head.bank_acct_number);
      DBMS_OUTPUT.put_line('Check supplier_name : ' || rec_head.supplier_name);
      
      OPEN cursor_invoice (rec_head.transaction_id,
                           rec_head.tax_code,
                           rec_head.voucher_number,
                           rec_head.location_id,
                           rec_head.bank_acct_number,
                           rec_head.supplier_name);

      LOOP
         FETCH cursor_invoice INTO rec_invoice;

         EXIT WHEN cursor_invoice%NOTFOUND;
         v_group := 'Y';
        DBMS_OUTPUT.put_line ('Check in cursor_invoice : '|| cursor_invoice%rowcount);
         BEGIN
            SELECT NVL (attribute4, 'Y')
              INTO v_group
              FROM ap_invoices_all
             WHERE invoice_id = rec_invoice.invoice_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_group := 'Y';
               
         END;
            
         DBMS_OUTPUT.put_line ('v_group : '|| v_group);
         DBMS_OUTPUT.put_line ('INSTR (rec_invoice.tax_code, 36) : '|| INSTR (rec_invoice.tax_code, '36'));
         IF v_group = 'Y' AND INSTR (rec_invoice.tax_code, '36') = 0  --Case 1
         THEN
            SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

            -- Calculate PAYMENT AMOUNT
            DBMS_OUTPUT.put_line ('10 begin group : Calculate BASE AMOUNT');

            BEGIN
               SELECT NVL (
                         SUM (
                              NVL (xdl.unrounded_entered_dr, 0)
                            - NVL (xdl.unrounded_entered_cr, 0)),
                         0)
                 INTO v_paid_base_inv
                 FROM xla_distribution_links xdl,
                      xla_ae_headers xeh,
                      ap_invoice_payments_all aip,
                      xla.xla_transaction_entities xte
                WHERE     xte.application_id = appl_id
                      AND xdl.application_id = xeh.application_id
                      AND xte.application_id = xeh.application_id
                      AND xdl.ae_header_id = xeh.ae_header_id
                      AND xte.entity_code = 'AP_PAYMENTS'
                      AND xte.source_id_int_1 = aip.check_id
                      AND xte.entity_id = xeh.entity_id
                      AND xdl.rounding_class_code = 'LIABILITY'
                      AND xdl.tax_line_ref_id IS NULL
                      AND aip.check_id = rec_head.transaction_id
                      AND aip.invoice_id = rec_invoice.invoice_id
                      AND xdl.applied_to_source_id_num_1 =
                             rec_invoice.invoice_id
                      AND xdl.applied_to_dist_id_num_1 IN (SELECT MAX (
                                                                     aid.invoice_distribution_id)
                                                             FROM ap_invoice_lines_all ail,
                                                                  ap_invoice_distributions_all aid
                                                            WHERE     ail.invoice_id =
                                                                         aid.invoice_id
                                                                  AND ail.line_number =
                                                                         aid.invoice_line_number
                                                                  AND aid.invoice_distribution_id =
                                                                         xdl.applied_to_dist_id_num_1
                                                                  AND ail.tax_classification_code =
                                                                         rec_head.tax_code
                                                                  AND NVL (
                                                                         ail.project_id,
                                                                         0) =
                                                                         NVL (
                                                                            rec_invoice.project_id,
                                                                            0))
                      AND (xdl.event_id,
                           0,
                           'XLA_DISTRIBUTION_LINKS_SERVICE',
                           aip.check_id) IN (SELECT tidld.reference_id,
                                                        0,
                                                        tidld.reference_type,
                                                        tidld.check_id
                                                   FROM thl_data_loaded tidld
                                                  WHERE     tidld.reference_type =
                                                               'XLA_DISTRIBUTION_LINKS_SERVICE'
                                                        AND tidld.reference_id =
                                                               xdl.event_id)
--                      AND (aip.invoice_payment_id,
--                           0,
--                           'AP_INVOICE_PAYMENTS_SERVICE',
--                           aip.check_id) NOT IN (SELECT tidld.reference_id,
--                                                        0,
--                                                        tidld.reference_type,
--                                                        tidld.check_id
--                                                   FROM thl_data_loaded tidld
--                                                  WHERE     tidld.reference_type =
--                                                               'AP_INVOICE_PAYMENTS_SERVICE'
--                                                        AND tidld.reference_id =
--                                                               aip.invoice_payment_id)
                                                               ;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_paid_base_inv := 0;
                  DBMS_OUTPUT.put_line (SQLERRM);
            END;

            DBMS_OUTPUT.put_line (
               '10 end group : Calculate BASE AMOUNT' || v_paid_base_inv);
            DBMS_OUTPUT.put_line ('20 begin group : Calculate REC TAX AMOUNT');


            DBMS_OUTPUT.put_line ('20 end group : Calculate REC TAX AMOUNT');
            DBMS_OUTPUT.put_line ('30 begin group : Calculate NON REC TAX AMOUNT');

            BEGIN
               SELECT NVL (
                         SUM (
                              NVL (xdl.unrounded_entered_dr, 0)
                            - NVL (xdl.unrounded_entered_cr, 0)),
                         0)
                 INTO v_paid_nonrec_tax
                 FROM xla_distribution_links xdl,
                      xla_ae_headers xeh,
                      ap_invoice_payments_all aip,
                      xla.xla_transaction_entities xte
                WHERE     xte.application_id = appl_id
                      AND xdl.application_id = xeh.application_id
                      AND xte.application_id = xeh.application_id
                      AND xdl.ae_header_id = xeh.ae_header_id
                      AND xte.entity_code = 'AP_PAYMENTS'
                      AND xte.source_id_int_1 = aip.check_id
                      AND xte.entity_id = xeh.entity_id
                      AND xdl.rounding_class_code IN ('DEF_REC_TAX',
                                                      'LIABILITY')
                      AND xdl.tax_line_ref_id IS NOT NULL
                      AND aip.check_id = rec_head.transaction_id
                      AND aip.invoice_id = rec_invoice.invoice_id
                      AND xdl.applied_to_source_id_num_1 =
                             rec_invoice.invoice_id
                      AND xdl.applied_to_dist_id_num_1 IN (SELECT MAX (
                                                                     aid.invoice_distribution_id)
                                                             FROM ap_invoice_lines_all ail,
                                                                  ap_invoice_distributions_all aid
                                                            WHERE     ail.invoice_id =
                                                                         aid.invoice_id
                                                                  AND ail.line_number =
                                                                         aid.invoice_line_number
                                                                  AND aid.invoice_distribution_id =
                                                                         xdl.applied_to_dist_id_num_1
                                                                  AND aid.line_type_lookup_code =
                                                                         'NONREC_TAX'
                                                                  AND ail.tax_rate_code =
                                                                         rec_head.tax_code
                                                                  AND NVL (
                                                                         ail.project_id,
                                                                         0) =
                                                                         NVL (
                                                                            rec_invoice.project_id,
                                                                            0))
                      AND (xdl.event_id,
                           0,
                           'XLA_DISTRIBUTION_LINKS_SERVICE',
                           aip.check_id) IN (SELECT tidld.reference_id,
                                                        0,
                                                        tidld.reference_type,
                                                        tidld.check_id
                                                   FROM thl_data_loaded tidld
                                                  WHERE     tidld.reference_type =
                                                               'XLA_DISTRIBUTION_LINKS_SERVICE'
                                                        AND tidld.reference_id =
                                                               xdl.event_id)
--                      AND (aip.invoice_payment_id,
--                           0,
--                           'AP_INVOICE_PAYMENTS_SERVICE',
--                           aip.check_id) NOT IN (SELECT tidld.reference_id,
--                                                        0,
--                                                        tidld.reference_type,
--                                                        tidld.check_id
--                                                   FROM thl_data_loaded tidld
--                                                  WHERE     tidld.reference_type =
--                                                               'AP_INVOICE_PAYMENTS_SERVICE'
--                                                        AND tidld.reference_id =
--                                                               aip.invoice_payment_id)
                                                               ;
            EXCEPTION
               WHEN OTHERS
               THEN
                  RAISE;
                  DBMS_OUTPUT.put_line (SQLERRM);
                  v_paid_nonrec_tax := 0;
            END;

            DBMS_OUTPUT.put_line ('30 end group : Calculate NON REC TAX AMOUNT');
            -- Find Period Name from GL DATE
            DBMS_OUTPUT.put_line ('45 begin group : Find Period Name from GL DATE');

              SELECT period_name
                INTO v_nperiod
                FROM hr_operating_units hou,
                     gl_sets_of_books gsob,
                     gl_periods_v gpv
               WHERE     (    hou.date_from <= SYSDATE
                          AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                     AND hou.set_of_books_id = gsob.set_of_books_id
                     AND gsob.period_set_name = gpv.period_set_name
                     AND hou.organization_id = org_id
                     AND (TRUNC (rec_head.transaction_date) BETWEEN gpv.start_date
                                                                AND gpv.end_date)
                     AND gpv.adjustment_period_flag = 'N'
            ORDER BY start_date;

--            write_log ('45 end group : Find Period Name from GL DATE');
--            -- Insert Data to table : THL_TAXINVOICES_LINE
--            write_log (
--               '50 begin group : Insert Data to table : THL_TAXINVOICES_LINE');

            BEGIN
            DBMS_OUTPUT.put_line ('Insert thl_taxinvoices_line create by: ' || rec_head.created_by);
               INSERT INTO thl_taxinvoices_line (th_til_id,
                                                 th_tih_id,
                                                 th_til_mode,
                                                 th_til_period_name,
                                                 th_til_tax_inv_id,
                                                 th_til_tax_inv_number,
                                                 th_til_tax_inv_date,
                                                 th_til_supplier_name,
                                                 th_til_project_id,
                                                 th_til_base_amt,
                                                 th_til_tax_amt,
                                                 th_til_non_rec_amt,
                                                 th_til_rec_amt,
                                                 attribute1,
                                                 created_by,
                                                 creation_date,
                                                 last_updated_by,
                                                 last_update_date,
                                                 last_update_login,
                                                 th_til_location_id,
                                                 rcp_tax_inv_date)
                    VALUES (v_line_id,
                            v_head_id,
                            'A',
                            NULL,                                 --v_nperiod,
                            rec_invoice.invoice_id,
                            rec_invoice.invoice_num,
                            rec_head.transaction_date,
                            rec_head.supplier_name,
                            rec_invoice.project_id,
                            v_paid_base_inv,
                            v_paid_rec_tax + v_paid_nonrec_tax,
                            v_paid_nonrec_tax,
                            v_paid_rec_tax,
                            p_conc_request_id,
                            rec_head.created_by,
                            rec_head.creation_date,
                            rec_head.last_updated_by,
                            rec_head.last_update_date,
                            rec_head.last_update_login,
                            rec_head.location_id,
                            rec_head.rcp_tax_inv_date);
            EXCEPTION
               WHEN OTHERS
               THEN
                  DBMS_OUTPUT.put_line ('Error insert THL_TAXINVOICES_LINE ' || SQLERRM);
                  v_sts :=
                     fnd_concurrent.set_completion_status (
                        'ERROR',
                        'Check output file for warnings');
            END;

--            write_log (
--               '50 end group : Insert Data to table : THL_TAXINVOICES_LINE');
            v_check_base_inv := v_check_base_inv + v_paid_base_inv;
            v_check_rec_tax := v_check_rec_tax + v_paid_rec_tax;
            v_check_nonrec_tax := v_check_nonrec_tax + v_paid_nonrec_tax;
         ELSIF v_group = 'Y' AND INSTR (rec_invoice.tax_code, '36') <> 0 -- Case 2
         THEN
            SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

            -- Calculate PAYMENT AMOUNT
--            write_log ('10 begin group 36: Calculate PAYMENT AMOUNT');
--
            IF INSTR (rec_invoice.tax_code, '36') <> 0
            THEN
               BEGIN
                  SELECT NVL (
                            SUM (
                                 NVL (xdl.unrounded_entered_dr, 0)
                               - NVL (xdl.unrounded_entered_cr, 0)),
                            0)
                    INTO v_payment_amt
                    FROM xla_distribution_links xdl,
                         xla_ae_headers xeh,
                         ap_invoice_payments_all aip,
                         xla.xla_transaction_entities xte
                   WHERE     xte.application_id = appl_id
                         AND xdl.application_id = xeh.application_id
                         AND xte.application_id = xeh.application_id
                         AND xdl.ae_header_id = xeh.ae_header_id
                         AND xte.entity_code = 'AP_PAYMENTS'
                         AND xte.source_id_int_1 = aip.check_id
                         AND xte.entity_id = xeh.entity_id
                         AND xdl.rounding_class_code = 'LIABILITY'
                         AND aip.check_id = rec_head.transaction_id
                         AND aip.invoice_id = rec_invoice.invoice_id
                         AND applied_to_source_id_num_1 =
                                rec_invoice.invoice_id
                         AND (xdl.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_SERVICE',
                              aip.check_id) IN (SELECT tidld.reference_id,
                                                           0,
                                                           tidld.reference_type,
                                                           tidld.check_id
                                                      FROM thl_data_loaded tidld
                                                     WHERE     tidld.reference_type =
                                                                  'XLA_DISTRIBUTION_LINKS_SERVICE'
                                                           AND tidld.reference_id =
                                                                  xdl.event_id)
--                         AND (aip.invoice_payment_id,
--                              0,
--                              'AP_INVOICE_PAYMENTS_SERVICE',
--                              aip.check_id) NOT IN (SELECT tidld.reference_id,
--                                                           0,
--                                                           tidld.reference_type,
--                                                           tidld.check_id
--                                                      FROM thl_data_loaded tidld
--                                                     WHERE     tidld.reference_type =
--                                                                  'AP_INVOICE_PAYMENTS_SERVICE'
--                                                           AND tidld.reference_id =
--                                                                  aip.invoice_payment_id);
                        ;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_payment_amt := 0;
               END;
            END IF;

--            write_log ('10 end group 36: Calculate PAYMENT AMOUNT');
--            -- Calculate INVOICE AMOUNT
--            write_log ('20 begin group 36: Calculate INVOICE AMOUNT');

            IF SIGN (v_payment_amt) = -1
            THEN
               -- For Void Payment Cancel Invoice
               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT SUM (v_invoice_amt)
                       INTO v_invoice_amt
                       FROM (SELECT NVL (
                                       SUM (
                                          NVL (xla.unrounded_entered_cr, 0)),
                                       0)
                                       v_invoice_amt
                               FROM ap_invoices_all ai,
                                    ap_invoice_lines_all ail,
                                    ap_invoice_distributions_all aid,
                                    ap_invoice_payments aip,
                                    ap_checks ac,
                                    xla_distribution_links xla
                              WHERE     ai.invoice_id = ail.invoice_id
                                    AND ail.invoice_id = aid.invoice_id
                                    AND ail.line_number =
                                           aid.invoice_line_number
                                    AND ail.line_type_lookup_code IN ('ITEM')
                                    AND ap_invoices_pkg.get_posting_status (
                                           ai.invoice_id) IN ('Y', 'P')
                                    AND ap_checks_pkg.get_posting_status (
                                           ac.check_id) = 'Y'
                                    AND ap_invoices_pkg.get_approval_status (
                                           ai.invoice_id,
                                           ai.invoice_amount,
                                           ai.payment_status_flag,
                                           ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                            'CANCELLED',
                                                                            'AVAILABLE')
                                    AND (aip.invoice_payment_id,
                                         0,
                                         'AP_INVOICE_PAYMENTS_SERVICE',
                                         ac.check_id) IN (SELECT tidld.reference_id,
                                                                     0,
                                                                     tidld.reference_type,
                                                                     tidld.check_id
                                                                FROM thl_data_loaded tidld
                                                               WHERE     tidld.reference_type =
                                                                            'AP_INVOICE_PAYMENTS_SERVICE'
                                                                     AND tidld.reference_id =
                                                                            aip.invoice_payment_id)
                                    AND ai.invoice_id = aip.invoice_id
                                    AND aip.check_id = ac.check_id
                                    AND xla.source_distribution_id_num_1 =
                                           aid.invoice_distribution_id
                                    AND xla.rounding_class_code = 'LIABILITY'
                                    AND xla.source_distribution_type =
                                           'AP_INV_DIST'
--                                    AND (xla.event_id,
--                                         0,
--                                         'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                         ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                                     0,
--                                                                     tidld.reference_type,
--                                                                     tidld.check_id
--                                                                FROM thl_data_loaded tidld
--                                                               WHERE     tidld.reference_type =
--                                                                            'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                                     AND tidld.reference_id =
--                                                                            xla.event_id)
                                    AND ac.check_id = rec_head.transaction_id
                                    AND ai.invoice_id =
                                           rec_invoice.invoice_id
                             UNION ALL
                             SELECT NVL (
                                       SUM (
                                          NVL (xla.unrounded_entered_cr, 0)),
                                       0)
                                       v_invoice_amt
                               FROM ap_invoices_all ai,
                                    ap_invoice_lines_all ail,
                                    ap_invoice_distributions_all aid,
                                    ap_invoice_payments aip,
                                    ap_checks ac,
                                    xla_distribution_links xla
                              WHERE     ai.invoice_id = ail.invoice_id
                                    AND ail.invoice_id = aid.invoice_id
                                    AND ail.line_number =
                                           aid.invoice_line_number
                                    AND ail.line_type_lookup_code IN ('TAX')
                                    AND ap_invoices_pkg.get_posting_status (
                                           ai.invoice_id) IN ('Y', 'P')
                                    AND ap_checks_pkg.get_posting_status (
                                           ac.check_id) = 'Y'
                                    AND ap_invoices_pkg.get_approval_status (
                                           ai.invoice_id,
                                           ai.invoice_amount,
                                           ai.payment_status_flag,
                                           ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                            'CANCELLED',
                                                                            'AVAILABLE')
                                    AND (aip.invoice_payment_id,
                                         0,
                                         'AP_INVOICE_PAYMENTS_SERVICE',
                                         ac.check_id) IN (SELECT tidld.reference_id,
                                                                     0,
                                                                     tidld.reference_type,
                                                                     tidld.check_id
                                                                FROM thl_data_loaded tidld
                                                               WHERE     tidld.reference_type =
                                                                            'AP_INVOICE_PAYMENTS_SERVICE'
                                                                     AND tidld.reference_id =
                                                                            aip.invoice_payment_id)
                                    AND ai.invoice_id = aip.invoice_id
                                    AND aip.check_id = ac.check_id
                                    AND xla.source_distribution_id_num_1 =
                                           aid.invoice_distribution_id
                                    AND xla.rounding_class_code = 'LIABILITY'
                                    AND xla.source_distribution_type =
                                           'AP_INV_DIST'
--                                    AND (xla.event_id,
--                                         0,
--                                         'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                         ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                                     0,
--                                                                     tidld.reference_type,
--                                                                     tidld.check_id
--                                                                FROM thl_data_loaded tidld
--                                                               WHERE     tidld.reference_type =
--                                                                            'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                                     AND tidld.reference_id =
--                                                                            xla.event_id)
                                    AND ac.check_id = rec_head.transaction_id
                                    AND ai.invoice_id =
                                           rec_invoice.invoice_id);
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_invoice_amt := 0;
                  END;
               END IF;
            ELSE
               -- For Normal Case
               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT SUM (v_invoice_amt)
                       INTO v_invoice_amt
                       FROM (SELECT NVL (
                                       SUM (
                                            NVL (xla.unrounded_entered_cr, 0)
                                          - NVL (xla.unrounded_entered_dr, 0)),
                                       0)
                                       v_invoice_amt
                               FROM ap_invoices_all ai,
                                    ap_invoice_lines_all ail,
                                    ap_invoice_distributions_all aid,
                                    ap_invoice_payments aip,
                                    ap_checks ac,
                                    xla_distribution_links xla
                              WHERE     ai.invoice_id = ail.invoice_id
                                    AND ail.invoice_id = aid.invoice_id
                                    AND ail.line_number =
                                           aid.invoice_line_number
                                    AND ail.line_type_lookup_code IN ('ITEM')
                                    AND ap_invoices_pkg.get_posting_status (
                                           ai.invoice_id) IN ('Y', 'P')
                                    AND ap_checks_pkg.get_posting_status (
                                           ac.check_id) = 'Y'
                                    AND ap_invoices_pkg.get_approval_status (
                                           ai.invoice_id,
                                           ai.invoice_amount,
                                           ai.payment_status_flag,
                                           ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                            'CANCELLED',
                                                                            'AVAILABLE')
                                    AND (aip.invoice_payment_id,
                                         0,
                                         'AP_INVOICE_PAYMENTS_SERVICE',
                                         ac.check_id) IN (SELECT tidld.reference_id,
                                                                     0,
                                                                     tidld.reference_type,
                                                                     tidld.check_id
                                                                FROM thl_data_loaded tidld
                                                               WHERE     tidld.reference_type =
                                                                            'AP_INVOICE_PAYMENTS_SERVICE'
                                                                     AND tidld.reference_id =
                                                                            aip.invoice_payment_id)
                                    AND ai.invoice_id = aip.invoice_id
                                    AND aip.check_id = ac.check_id
                                    AND xla.source_distribution_id_num_1 =
                                           aid.invoice_distribution_id
                                    AND xla.rounding_class_code = 'LIABILITY'
                                    AND xla.source_distribution_type =
                                           'AP_INV_DIST'
--                                    AND (xla.event_id,
--                                         0,
--                                         'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                         ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                                     0,
--                                                                     tidld.reference_type,
--                                                                     tidld.check_id
--                                                                FROM thl_data_loaded tidld
--                                                               WHERE     tidld.reference_type =
--                                                                            'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                                     AND tidld.reference_id =
--                                                                            xla.event_id)
                                    AND ac.check_id = rec_head.transaction_id
                                    AND ai.invoice_id =
                                           rec_invoice.invoice_id
                             UNION ALL
                             SELECT NVL (
                                       SUM (
                                            NVL (xla.unrounded_entered_cr, 0)
                                          - NVL (xla.unrounded_entered_dr, 0)),
                                       0)
                                       v_invoice_amt
                               FROM ap_invoices_all ai,
                                    ap_invoice_lines_all ail,
                                    ap_invoice_distributions_all aid,
                                    ap_invoice_payments aip,
                                    ap_checks ac,
                                    xla_distribution_links xla
                              WHERE     ai.invoice_id = ail.invoice_id
                                    AND ail.invoice_id = aid.invoice_id
                                    AND ail.line_number =
                                           aid.invoice_line_number
                                    AND ail.line_type_lookup_code IN ('TAX')
                                    AND ap_invoices_pkg.get_posting_status (
                                           ai.invoice_id) IN ('Y', 'P')
                                    AND ap_checks_pkg.get_posting_status (
                                           ac.check_id) = 'Y'
                                    AND ap_invoices_pkg.get_approval_status (
                                           ai.invoice_id,
                                           ai.invoice_amount,
                                           ai.payment_status_flag,
                                           ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                            'CANCELLED',
                                                                            'AVAILABLE')
                                    AND (aip.invoice_payment_id,
                                         0,
                                         'AP_INVOICE_PAYMENTS_SERVICE',
                                         ac.check_id) IN (SELECT tidld.reference_id,
                                                                     0,
                                                                     tidld.reference_type,
                                                                     tidld.check_id
                                                                FROM thl_data_loaded tidld
                                                               WHERE     tidld.reference_type =
                                                                            'AP_INVOICE_PAYMENTS_SERVICE'
                                                                     AND tidld.reference_id =
                                                                            aip.invoice_payment_id)
                                    AND ai.invoice_id = aip.invoice_id
                                    AND aip.check_id = ac.check_id
                                    AND xla.source_distribution_id_num_1 =
                                           aid.invoice_distribution_id
                                    AND xla.rounding_class_code = 'LIABILITY'
                                    AND xla.source_distribution_type =
                                           'AP_INV_DIST'
--                                    AND (xla.event_id,
--                                         0,
--                                         'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                         ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                                     0,
--                                                                     tidld.reference_type,
--                                                                     tidld.check_id
--                                                                FROM thl_data_loaded tidld
--                                                               WHERE     tidld.reference_type =
--                                                                            'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                                     AND tidld.reference_id =
--                                                                            xla.event_id)
                                    AND ac.check_id = rec_head.transaction_id
                                    AND ai.invoice_id =
                                           rec_invoice.invoice_id);
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_invoice_amt := 0;
                  END;
               END IF;
            END IF;

--            write_log ('20 end group 36: Calculate INVOICE AMOUNT');
--            -- Calculate BASE AMOUNT
--            write_log ('30 begin group 36: Calculate BASE AMOUNT');

            IF SIGN (v_payment_amt) = -1
            THEN
               -- For Void Payment Cancel Invoice
               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0)), 0)
                       INTO v_base_amt
                       FROM ap_invoices_all ai,
                            ap_invoice_lines_all ail,
                            ap_invoice_distributions_all aid,
                            (  SELECT a.tax_rate_code tax,
                                      NVL (b.percentage_rate, 0)
                                         percentage_rate
                                 FROM zx_rates_b a, zx_rates_b b
                                WHERE     a.active_flag = 'Y'
                                      AND NVL (a.vat_transaction_type_code,
                                               'NULL') <> 'N/A'
                                      AND a.default_rec_rate_code =
                                             b.tax_rate_code(+)
                                      AND b.rate_type_code(+) = 'RECOVERY'
                                      AND b.active_flag(+) = 'Y'
                                      AND b.effective_from(+) <= SYSDATE
                                      AND NVL (b.effective_to(+), SYSDATE) >=
                                             SYSDATE
                             GROUP BY a.tax_rate_code, b.percentage_rate) atc,
                            zx_rates_b zrb,
                            ap_invoice_payments aip,
                            ap_checks ac,
                            ce_bank_accounts cba,
                            ce_bank_acct_uses_all cbau,
                            ce_bank_branches_v cbb,
                            hr_locations hla,
--                            (  SELECT invoice_id, tax_rate_id, tax_rate_code
--                                 FROM ap_invoice_lines_all
--                                WHERE line_type_lookup_code = 'TAX'
--                             GROUP BY invoice_id, tax_rate_id, tax_rate_code)
--                            inv_tax,
                            xla_distribution_links xla
                      WHERE     ai.invoice_id = ail.invoice_id
                            AND ail.invoice_id = aid.invoice_id
                            AND ail.line_number = aid.invoice_line_number
                            AND ail.tax_classification_code = atc.tax(+)
                            AND ail.tax_classification_code =
                                   zrb.tax_rate_code
                            AND zrb.def_rec_settlement_option_code =
                                   'DEFERRED'
                            AND zrb.active_flag = 'Y'
                            AND NVL (zrb.vat_transaction_type_code, 'NULL') <>
                                   'N/A'
                            AND ail.line_type_lookup_code IN ('ITEM')
                            AND ai.invoice_id IN (SELECT invoice_id
                                                    FROM ap_invoice_lines_all
                                                   WHERE     invoice_id =
                                                                ai.invoice_id
                                                         AND line_type_lookup_code =
                                                                'TAX')
                            AND ap_invoices_pkg.get_posting_status (
                                   ai.invoice_id) IN ('Y', 'P')
                            AND ap_checks_pkg.get_posting_status (
                                   ac.check_id) = 'Y'
                            AND ap_invoices_pkg.get_approval_status (
                                   ai.invoice_id,
                                   ai.invoice_amount,
                                   ai.payment_status_flag,
                                   ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                    'CANCELLED',
                                                                    'AVAILABLE')
                            AND (aip.invoice_payment_id,
                                 0,
                                 'AP_INVOICE_PAYMENTS_SERVICE',
                                 ac.check_id) IN (SELECT tidld.reference_id,
                                                             0,
                                                             tidld.reference_type,
                                                             tidld.check_id
                                                        FROM thl_data_loaded tidld
                                                       WHERE     tidld.reference_type =
                                                                    'AP_INVOICE_PAYMENTS_SERVICE'
                                                             AND tidld.reference_id =
                                                                    aip.invoice_payment_id)
                            AND ai.invoice_id = aip.invoice_id
                            AND aip.check_id = ac.check_id
                            AND ac.ce_bank_acct_use_id =
                                   cbau.bank_acct_use_id
                            AND cbau.bank_account_id = cba.bank_account_id
                            AND cbb.branch_party_id = cba.bank_branch_id
--                            AND ai.invoice_id = inv_tax.invoice_id
--                            AND ail.tax_classification_code =
--                                   inv_tax.tax_rate_code
--                            AND zrb.tax_rate_id = inv_tax.tax_rate_id
                            AND xla.source_distribution_id_num_1 =
                                   aid.invoice_distribution_id
                            AND xla.rounding_class_code = 'LIABILITY'
                            AND xla.source_distribution_type = 'AP_INV_DIST'
--                            AND (xla.event_id,
--                                 0,
--                                 'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                 ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                             0,
--                                                             tidld.reference_type,
--                                                             tidld.check_id
--                                                        FROM thl_data_loaded tidld
--                                                       WHERE     tidld.reference_type =
--                                                                    'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                             AND tidld.reference_id =
--                                                                    xla.event_id)
                            AND ac.check_id = rec_head.transaction_id
                            AND ai.invoice_id = rec_invoice.invoice_id
                            AND ail.tax_classification_code =
                                   rec_head.tax_code
                            AND cba.bank_account_num =
                                   rec_head.bank_acct_number
                            AND NVL (ail.project_id, 0) =
                                   NVL (rec_invoice.project_id, 0)
                            AND zrb.description = hla.location_code;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_base_amt := 0;
                  END;
               END IF;
            ELSE
               -- For Normal Case
               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT NVL (
                               SUM (
                                    NVL (xla.unrounded_entered_cr, 0)
                                  - NVL (xla.unrounded_entered_dr, 0)),
                               0)
                       INTO v_base_amt
                       FROM ap_invoices_all ai,
                            ap_invoice_lines_all ail,
                            ap_invoice_distributions_all aid,
                            (  SELECT a.tax_rate_code tax,
                                      NVL (b.percentage_rate, 0)
                                         percentage_rate
                                 FROM zx_rates_b a, zx_rates_b b
                                WHERE     a.active_flag = 'Y'
                                      AND NVL (a.vat_transaction_type_code,
                                               'NULL') <> 'N/A'
                                      AND a.default_rec_rate_code =
                                             b.tax_rate_code(+)
                                      AND b.rate_type_code(+) = 'RECOVERY'
                                      AND b.active_flag(+) = 'Y'
                                      AND b.effective_from(+) <= SYSDATE
                                      AND NVL (b.effective_to(+), SYSDATE) >=
                                             SYSDATE
                             GROUP BY a.tax_rate_code, b.percentage_rate) atc,
                            zx_rates_b zrb,
                            ap_invoice_payments aip,
                            ap_checks ac,
                            ce_bank_accounts cba,
                            ce_bank_acct_uses_all cbau,
                            ce_bank_branches_v cbb,
                            hr_locations hla,
--                            (  SELECT invoice_id, tax_rate_id, tax_rate_code
--                                 FROM ap_invoice_lines_all
--                                WHERE line_type_lookup_code = 'TAX'
--                             GROUP BY invoice_id, tax_rate_id, tax_rate_code)
--                            inv_tax,
                            xla_distribution_links xla
                      WHERE     ai.invoice_id = ail.invoice_id
                            AND ail.invoice_id = aid.invoice_id
                            AND ail.line_number = aid.invoice_line_number
                            AND ail.tax_classification_code = atc.tax(+)
                            AND ail.tax_classification_code =
                                   zrb.tax_rate_code
                            AND zrb.def_rec_settlement_option_code =
                                   'DEFERRED'
                            AND zrb.active_flag = 'Y'
                            AND NVL (zrb.vat_transaction_type_code, 'NULL') <>
                                   'N/A'
                            AND ail.line_type_lookup_code IN ('ITEM')
                            AND ai.invoice_id IN (SELECT invoice_id
                                                    FROM ap_invoice_lines_all
                                                   WHERE     invoice_id =
                                                                ai.invoice_id
                                                         AND line_type_lookup_code =
                                                                'TAX')
                            AND ap_invoices_pkg.get_posting_status (
                                   ai.invoice_id) IN ('Y', 'P')
                            AND ap_checks_pkg.get_posting_status (
                                   ac.check_id) = 'Y'
                            AND ap_invoices_pkg.get_approval_status (
                                   ai.invoice_id,
                                   ai.invoice_amount,
                                   ai.payment_status_flag,
                                   ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                    'CANCELLED',
                                                                    'AVAILABLE')
                            AND (aip.invoice_payment_id,
                                 0,
                                 'AP_INVOICE_PAYMENTS_SERVICE',
                                 ac.check_id) IN (SELECT tidld.reference_id,
                                                             0,
                                                             tidld.reference_type,
                                                             tidld.check_id
                                                        FROM thl_data_loaded tidld
                                                       WHERE     tidld.reference_type =
                                                                    'AP_INVOICE_PAYMENTS_SERVICE'
                                                             AND tidld.reference_id =
                                                                    aip.invoice_payment_id)
                            AND ai.invoice_id = aip.invoice_id
                            AND aip.check_id = ac.check_id
                            AND ac.ce_bank_acct_use_id =
                                   cbau.bank_acct_use_id
                            AND cbau.bank_account_id = cba.bank_account_id
                            AND cbb.branch_party_id = cba.bank_branch_id
--                            AND ai.invoice_id = inv_tax.invoice_id
--                            AND ail.tax_classification_code =
--                                   inv_tax.tax_rate_code
--                            AND zrb.tax_rate_id = inv_tax.tax_rate_id
                            AND xla.source_distribution_id_num_1 =
                                   aid.invoice_distribution_id
                            AND xla.rounding_class_code = 'LIABILITY'
                            AND xla.source_distribution_type = 'AP_INV_DIST'
--                            AND (xla.event_id,
--                                 0,
--                                 'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                 ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                             0,
--                                                             tidld.reference_type,
--                                                             tidld.check_id
--                                                        FROM thl_data_loaded tidld
--                                                       WHERE     tidld.reference_type =
--                                                                    'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                             AND tidld.reference_id =
--                                                                    xla.event_id)
                            AND ac.check_id = rec_head.transaction_id
                            AND ai.invoice_id = rec_invoice.invoice_id
                            AND ail.tax_classification_code =
                                   rec_head.tax_code
                            AND cba.bank_account_num =
                                   rec_head.bank_acct_number
                            AND NVL (ail.project_id, 0) =
                                   NVL (rec_invoice.project_id, 0)
                            AND zrb.description = hla.location_code;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_base_amt := 0;
                  END;
               END IF;
            END IF;

--            write_log ('30 end group 36: Calculate BASE AMOUNT');
--            -- Calculate for Partial Payment
--            write_log ('40 begin group 36: Calculate for Partial Payment');

            IF v_invoice_amt <> 0
            THEN
               v_paid_percent := v_payment_amt / v_invoice_amt;
            ELSE
               v_paid_percent := 0;
            END IF;

            v_paid_base_inv := ROUND (v_base_amt * v_paid_percent, 2);
            v_paid_rec_tax :=
               ROUND (
                    ROUND (
                         ROUND (
                              ROUND (v_paid_base_inv * rec_head.tax_rate, 2)
                            / 100,
                            2)
                       * rec_head.tax_recovery_rate,
                       2)
                  / 100,
                  2);
            v_paid_nonrec_tax :=
               ROUND (
                    ROUND (
                         ROUND (
                              ROUND (v_paid_base_inv * rec_head.tax_rate, 2)
                            / 100,
                            2)
                       * (100 - rec_head.tax_recovery_rate),
                       2)
                  / 100,
                  2);

            IF v_paid_base_inv IS NULL
            THEN
               v_paid_base_inv := 0;
            END IF;

            IF v_paid_rec_tax IS NULL
            THEN
               v_paid_rec_tax := 0;
            END IF;

            IF v_paid_nonrec_tax IS NULL
            THEN
               v_paid_nonrec_tax := 0;
            END IF;

--            write_log ('40 end group 36: Calculate for Partial Payment');
--            -- Find Period Name from GL DATE
--            write_log ('45 begin group 36: Find Period Name from GL DATE');

              SELECT period_name
                INTO v_nperiod
                FROM hr_operating_units hou,
                     gl_sets_of_books gsob,
                     gl_periods_v gpv
               WHERE     (    hou.date_from <= SYSDATE
                          AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                     AND hou.set_of_books_id = gsob.set_of_books_id
                     AND gsob.period_set_name = gpv.period_set_name
                     AND hou.organization_id = org_id
                     AND (TRUNC (rec_head.transaction_date) BETWEEN gpv.start_date
                                                                AND gpv.end_date)
                     AND gpv.adjustment_period_flag = 'N'
            ORDER BY start_date;

--            write_log ('45 end group 36: Find Period Name from GL DATE');
--            -- Insert Data to table : THL_TAXINVOICES_LINE
--            write_log (
--               '50 begin group 36: Insert Data to table : THL_TAXINVOICES_LINE');

            BEGIN
               INSERT INTO thl_taxinvoices_line (th_til_id,
                                                 th_tih_id,
                                                 th_til_mode,
                                                 th_til_period_name,
                                                 th_til_tax_inv_id,
                                                 th_til_tax_inv_number,
                                                 th_til_tax_inv_date,
                                                 th_til_supplier_name,
                                                 th_til_project_id,
                                                 th_til_base_amt,
                                                 th_til_tax_amt,
                                                 th_til_non_rec_amt,
                                                 th_til_rec_amt,
                                                 attribute1,
                                                 created_by,
                                                 creation_date,
                                                 last_updated_by,
                                                 last_update_date,
                                                 last_update_login,
                                                 th_til_location_id,
                                                 rcp_tax_inv_date)
                    VALUES (v_line_id,
                            v_head_id,
                            'A',
                            NULL,                                 --v_nperiod,
                            rec_invoice.invoice_id,
                            rec_invoice.invoice_num,
                            rec_head.transaction_date,
                            rec_head.supplier_name,
                            rec_invoice.project_id,
                            v_paid_base_inv,
                            v_paid_rec_tax + v_paid_nonrec_tax,
                            v_paid_nonrec_tax,
                            v_paid_rec_tax,
                            p_conc_request_id,
                            rec_head.created_by,
                            rec_head.creation_date,
                            rec_head.last_updated_by,
                            rec_head.last_update_date,
                            rec_head.last_update_login,
                            rec_head.location_id,
                            rec_head.rcp_tax_inv_date);
            EXCEPTION
               WHEN OTHERS
               THEN
--                  write_log ('Error insert THL_TAXINVOICES_LINE ' || SQLERRM);
                  v_sts :=
                     fnd_concurrent.set_completion_status (
                        'ERROR',
                        'Check output file for warnings');
            END;

--
--            write_log (
--               '50 end group 36: Insert Data to table : THL_TAXINVOICES_LINE');
            v_check_base_inv := v_check_base_inv + v_paid_base_inv;
            v_check_rec_tax := v_check_rec_tax + v_paid_rec_tax;
            v_check_nonrec_tax := v_check_nonrec_tax + v_paid_nonrec_tax;
         ELSIF v_group = 'N' OR INSTR (rec_invoice.tax_code, '36') <> 0 ------  Case 3
         THEN
            OPEN cursor_invoice_line (rec_invoice.invoice_id);

            LOOP
               FETCH cursor_invoice_line INTO rec_invoice_line;

               EXIT WHEN cursor_invoice_line%NOTFOUND;

               SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

               -- Calculate PAYMENT AMOUNT
--               write_log ('10 begin no group : Calculate PAYMENT AMOUNT');

               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT NVL (
                               SUM (
                                    NVL (xdl.unrounded_entered_dr, 0)
                                  - NVL (xdl.unrounded_entered_cr, 0)),
                               0)
                       INTO v_payment_amt
                       FROM xla_distribution_links xdl,
                            xla_ae_headers xeh,
                            ap_invoice_payments_all aip,
                            xla.xla_transaction_entities xte
                      WHERE     xte.application_id = appl_id
                            AND xdl.application_id = xeh.application_id
                            AND xte.application_id = xeh.application_id
                            AND xdl.ae_header_id = xeh.ae_header_id
                            AND xte.entity_code = 'AP_PAYMENTS'
                            AND xte.source_id_int_1 = aip.check_id
                            AND xte.entity_id = xeh.entity_id
                            AND xdl.rounding_class_code = 'LIABILITY'
                            AND aip.check_id = rec_head.transaction_id
                            AND aip.invoice_id = rec_invoice.invoice_id
                            AND applied_to_source_id_num_1 =
                                   rec_invoice.invoice_id
                            AND (xdl.event_id,
                                 0,
                                 'XLA_DISTRIBUTION_LINKS_SERVICE',
                                 aip.check_id) IN (SELECT tidld.reference_id,
                                                              0,
                                                              tidld.reference_type,
                                                              tidld.check_id
                                                         FROM thl_data_loaded tidld
                                                        WHERE     tidld.reference_type =
                                                                     'XLA_DISTRIBUTION_LINKS_SERVICE'
                                                              AND tidld.reference_id =
                                                                     xdl.event_id)
--                            AND (aip.invoice_payment_id,
--                                 0,
--                                 'AP_INVOICE_PAYMENTS_SERVICE',
--                                 aip.check_id) NOT IN (SELECT tidld.reference_id,
--                                                              0,
--                                                              tidld.reference_type,
--                                                              tidld.check_id
--                                                         FROM thl_data_loaded tidld
--                                                        WHERE     tidld.reference_type =
--                                                                     'AP_INVOICE_PAYMENTS_SERVICE'
--                                                              AND tidld.reference_id =
--                                                                     aip.invoice_payment_id)
                                                                     ;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_payment_amt := 0;
                  END;
               END IF;

--               write_log ('10 end no group : Calculate PAYMENT AMOUNT');
--               -- Calculate INVOICE AMOUNT
--               write_log ('20 begin no group : Calculate INVOICE AMOUNT');

               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT SUM (v_invoice_amt)
                       INTO v_invoice_amt
                       FROM (SELECT NVL (
                                       SUM (
                                            NVL (xla.unrounded_entered_cr, 0)
                                          - NVL (xla.unrounded_entered_dr, 0)),
                                       0)
                                       v_invoice_amt
                               FROM ap_invoices_all ai,
                                    ap_invoice_lines_all ail,
                                    ap_invoice_distributions_all aid,
                                    ap_invoice_payments aip,
                                    ap_checks ac,
                                    xla_distribution_links xla
                              WHERE     ai.invoice_id = ail.invoice_id
                                    AND ail.invoice_id = aid.invoice_id
                                    AND ail.line_number =
                                           aid.invoice_line_number
                                    AND ail.line_type_lookup_code IN ('ITEM')
                                    AND ap_invoices_pkg.get_posting_status (
                                           ai.invoice_id) IN ('Y', 'P')
                                    AND ap_checks_pkg.get_posting_status (
                                           ac.check_id) = 'Y'
                                    AND ap_invoices_pkg.get_approval_status (
                                           ai.invoice_id,
                                           ai.invoice_amount,
                                           ai.payment_status_flag,
                                           ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                            'CANCELLED',
                                                                            'AVAILABLE')
                                    AND (aip.invoice_payment_id,
                                         0,
                                         'AP_INVOICE_PAYMENTS_SERVICE',
                                         ac.check_id) IN (SELECT tidld.reference_id,
                                                                     0,
                                                                     tidld.reference_type,
                                                                     tidld.check_id
                                                                FROM thl_data_loaded tidld
                                                               WHERE     tidld.reference_type =
                                                                            'AP_INVOICE_PAYMENTS_SERVICE'
                                                                     AND tidld.reference_id =
                                                                            aip.invoice_payment_id)
                                    AND ai.invoice_id = aip.invoice_id
                                    AND aip.check_id = ac.check_id
                                    AND xla.source_distribution_id_num_1 =
                                           aid.invoice_distribution_id
                                    AND xla.rounding_class_code = 'LIABILITY'
                                    AND xla.source_distribution_type =
                                           'AP_INV_DIST'
--                                    AND (xla.event_id,
--                                         0,
--                                         'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                         ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                                     0,
--                                                                     tidld.reference_type,
--                                                                     tidld.check_id
--                                                                FROM thl_data_loaded tidld
--                                                               WHERE     tidld.reference_type =
--                                                                            'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                                     AND tidld.reference_id =
--                                                                            xla.event_id)
                                    AND ac.check_id = rec_head.transaction_id
                                    AND ai.invoice_id =
                                           rec_invoice.invoice_id
                             UNION ALL
                             SELECT NVL (
                                       SUM (
                                            NVL (xla.unrounded_entered_cr, 0)
                                          - NVL (xla.unrounded_entered_dr, 0)),
                                       0)
                                       v_invoice_amt
                               FROM ap_invoices_all ai,
                                    ap_invoice_lines_all ail,
                                    ap_invoice_distributions_all aid,
                                    ap_invoice_payments aip,
                                    ap_checks ac,
                                    xla_distribution_links xla
                              WHERE     ai.invoice_id = ail.invoice_id
                                    AND ail.invoice_id = aid.invoice_id
                                    AND ail.line_number =
                                           aid.invoice_line_number
                                    AND ail.line_type_lookup_code IN ('TAX')
                                    AND ap_invoices_pkg.get_posting_status (
                                           ai.invoice_id) IN ('Y', 'P')
                                    AND ap_checks_pkg.get_posting_status (
                                           ac.check_id) = 'Y'
                                    AND ap_invoices_pkg.get_approval_status (
                                           ai.invoice_id,
                                           ai.invoice_amount,
                                           ai.payment_status_flag,
                                           ai.invoice_type_lookup_code) IN ('APPROVED',
                                                                            'CANCELLED',
                                                                            'AVAILABLE')
                                    AND (aip.invoice_payment_id,
                                         0,
                                         'AP_INVOICE_PAYMENTS_SERVICE',
                                         ac.check_id) IN (SELECT tidld.reference_id,
                                                                     0,
                                                                     tidld.reference_type,
                                                                     tidld.check_id
                                                                FROM thl_data_loaded tidld
                                                               WHERE     tidld.reference_type =
                                                                            'AP_INVOICE_PAYMENTS_SERVICE'
                                                                     AND tidld.reference_id =
                                                                            aip.invoice_payment_id)
                                    AND ai.invoice_id = aip.invoice_id
                                    AND aip.check_id = ac.check_id
                                    AND xla.source_distribution_id_num_1 =
                                           aid.invoice_distribution_id
                                    AND xla.rounding_class_code = 'LIABILITY'
                                    AND xla.source_distribution_type =
                                           'AP_INV_DIST'
--                                    AND (xla.event_id,
--                                         0,
--                                         'XLA_DISTRIBUTION_LINKS_SERVICE',
--                                         ac.check_id) NOT IN (SELECT tidld.reference_id,
--                                                                     0,
--                                                                     tidld.reference_type,
--                                                                     tidld.check_id
--                                                                FROM thl_data_loaded tidld
--                                                               WHERE     tidld.reference_type =
--                                                                            'XLA_DISTRIBUTION_LINKS_SERVICE'
--                                                                     AND tidld.reference_id =
--                                                                            xla.event_id)
                                    AND ac.check_id = rec_head.transaction_id
                                    AND ai.invoice_id =
                                           rec_invoice.invoice_id);
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_invoice_amt := 0;
                  END;
               END IF;

--               write_log ('20 end no group : Calculate INVOICE AMOUNT');
--               -- Calculate BASE AMOUNT
--               write_log ('30 begin no group : Calculate BASE AMOUNT');
--               v_base_amt := rec_invoice_line.base_amt;
--               write_log ('30 end no group : Calculate BASE AMOUNT');
--               -- Calculate for Partial Payment
--               write_log (
--                  '40 begin no group : Calculate for Partial Payment');

               -------- Add Calculate tax amount  by Suwatchai  10-APR-12    V.2.0


               IF v_invoice_amt <> 0
               THEN
                  v_paid_percent := v_payment_amt / v_invoice_amt;
               ELSE
                  v_paid_percent := 0;
               END IF;



               IF v_payment_amt <> v_invoice_amt            -- Partial payment
               THEN
                  v_paid_base_inv := ROUND (v_base_amt * v_paid_percent, 2);
                  v_paid_rec_tax :=
                     ROUND (
                          ROUND (
                               ROUND (
                                    ROUND (
                                       v_paid_base_inv * rec_head.tax_rate,
                                       2)
                                  / 100,
                                  2)
                             * rec_head.tax_recovery_rate,
                             2)
                        / 100,
                        2);


                  v_paid_nonrec_tax :=
                     ROUND (
                          ROUND (
                               ROUND (
                                    ROUND (
                                       v_paid_base_inv * rec_head.tax_rate,
                                       2)
                                  / 100,
                                  2)
                             * (100 - rec_head.tax_recovery_rate),
                             2)
                        / 100,
                        2);
               ELSE                                           -- Fully payment
                  v_paid_base_inv := v_base_amt;
                  v_paid_rec_tax := rec_invoice_line.tax_amt2; -- v_paid_rec_tax ;        -- Get tax_amount directly from ap_invoice_distributions_all  ..by Suwatchai 10-APR-12  V.2.0
                  v_paid_nonrec_tax := 0;
               END IF;

               IF v_paid_base_inv IS NULL
               THEN
                  v_paid_base_inv := 0;
               END IF;

               IF v_paid_rec_tax IS NULL
               THEN
                  v_paid_rec_tax := 0;
               END IF;

               IF v_paid_nonrec_tax IS NULL
               THEN
                  v_paid_nonrec_tax := 0;
               END IF;

--               write_log ('40 end no group : Calculate for Partial Payment');
--               -- Find Period Name from GL DATE
--               write_log (
--                  '45 begin no group : Find Period Name from GL DATE');

                 SELECT period_name
                   INTO v_nperiod
                   FROM hr_operating_units hou,
                        gl_sets_of_books gsob,
                        gl_periods_v gpv
                  WHERE     (    hou.date_from <= SYSDATE
                             AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                        AND hou.set_of_books_id = gsob.set_of_books_id
                        AND gsob.period_set_name = gpv.period_set_name
                        AND hou.organization_id = org_id
                        AND (TRUNC (rec_head.transaction_date) BETWEEN gpv.start_date
                                                                   AND gpv.end_date)
                        AND gpv.adjustment_period_flag = 'N'
               ORDER BY start_date;

--               write_log ('45 end no group : Find Period Name from GL DATE');
--               -- Insert Data to table : THL_TAXINVOICES_LINE
--               write_log (
--                  '50 begin no group : Insert Data to table : THL_TAXINVOICES_LINE');

               BEGIN
                  INSERT INTO thl_taxinvoices_line (th_til_id,
                                                    th_tih_id,
                                                    th_til_mode,
                                                    th_til_period_name,
                                                    th_til_tax_inv_id,
                                                    th_til_tax_inv_number,
                                                    th_til_tax_inv_date,
                                                    th_til_supplier_name,
                                                    th_til_project_id,
                                                    th_til_base_amt,
                                                    th_til_tax_amt,
                                                    th_til_non_rec_amt,
                                                    th_til_rec_amt,
                                                    attribute1,
                                                    created_by,
                                                    creation_date,
                                                    last_updated_by,
                                                    last_update_date,
                                                    last_update_login,
                                                    th_til_location_id,
                                                    rcp_tax_inv_date)
                       VALUES (v_line_id,
                               v_head_id,
                               'A',
                               NULL,                              --v_nperiod,
                               rec_invoice.invoice_id,
                               rec_invoice.invoice_num,
                               rec_head.transaction_date,
                               rec_invoice_line.supplier_name,
                               rec_invoice.project_id,
                               v_paid_base_inv,
                               v_paid_rec_tax + v_paid_nonrec_tax,
                               v_paid_nonrec_tax,
                               v_paid_rec_tax,
                               p_conc_request_id,
                               rec_head.created_by,
                               rec_head.creation_date,
                               rec_head.last_updated_by,
                               rec_head.last_update_date,
                               rec_head.last_update_login,
                               rec_head.location_id,
                               rec_head.rcp_tax_inv_date);
               EXCEPTION
                  WHEN OTHERS
                  THEN
--                     write_log (
--                        'Error insert THL_TAXINVOICES_LINE ' || SQLERRM);
                     v_sts :=
                        fnd_concurrent.set_completion_status (
                           'ERROR',
                           'Check output file for warnings');
               END;

--               write_log (
--                  '50 end no group : Insert Data to table : THL_TAXINVOICES_LINE');
               v_check_base_inv := v_check_base_inv + v_paid_base_inv;
               v_check_rec_tax := v_check_rec_tax + v_paid_rec_tax;
               v_check_nonrec_tax := v_check_nonrec_tax + v_paid_nonrec_tax;
            END LOOP;

            CLOSE cursor_invoice_line;
         END IF;
      END LOOP;

      CLOSE cursor_invoice;

      v_insert := TRUE;
      -- Insert Data to table : THL_TAXINVOICES_HEADER
--      write_log ('60 begin : Insert Data to table : THL_TAXINVOICES_HEADER');

      BEGIN
         INSERT INTO thl_taxinvoices_header (th_tih_id,
                                             th_tih_number,
                                             th_tih_transaction_id,
                                             th_tih_transaction_number,
                                             th_tih_transaction_date,
                                             th_tih_type_tax,
                                             th_tih_tax_code,
                                             th_tih_location_id,
                                             th_tih_voucher_number,
                                             th_tih_bank_acct_number,
                                             th_tih_bank_acct_name,
                                             th_tih_non_rec_amt,
                                             th_tih_rec_amt,
                                             th_tih_rec_rate,
                                             th_tih_base_amt,
                                             attribute1,
                                             created_by,
                                             creation_date,
                                             last_updated_by,
                                             last_update_date,
                                             last_update_login)
              VALUES (v_head_id,
                      v_head_id,
                      rec_head.transaction_id,
                      rec_head.transaction_number,
                      rec_head.transaction_date,
                      rec_head.type_tax,
                      rec_head.tax_code,
                      rec_head.location_id,
                      rec_head.voucher_number,
                      rec_head.bank_acct_number,
                      rec_head.bank_acct_name,
                      v_check_nonrec_tax,
                      v_check_rec_tax,
                      rec_head.tax_recovery_rate,
                      v_check_base_inv,
                      p_conc_request_id,
                      rec_head.created_by,
                      rec_head.creation_date,
                      rec_head.last_updated_by,
                      rec_head.last_update_date,
                      rec_head.last_update_login);
      EXCEPTION
         WHEN OTHERS
         THEN
--            write_log ('Error insert THL_TAXINVOICES_HEADER ' || SQLERRM);
            v_sts :=
               fnd_concurrent.set_completion_status (
                  'ERROR',
                  'Check output file for warnings');
      END;

--      write_log ('60 end : Insert Data to table : THL_TAXINVOICES_HEADER');
   END LOOP;

   CLOSE cursor_head;

   COMMIT;
EXCEPTION
   WHEN OTHERS
   THEN
      raise_application_error (-20101, 'Saving Error!');
      ROLLBACK;
END;