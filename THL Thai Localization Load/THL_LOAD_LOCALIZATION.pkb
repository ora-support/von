CREATE OR REPLACE PACKAGE BODY APPS."THL_LOAD_LOCALIZATION"
IS
   /**
          V 1.0.0 by Companion : Damrong Tantiwiwatsakul <Boy>
                  Last Updated : 24-DEC-2008 18.00.00
          V 1.1.0 by Companion : Damrong Tantiwiwatsakul <Boy>
                  Last Updated : 22-JAN-2009 15.00.00
                   Detail      : 1. Change Location by Tax Code
                                 2. Insert Invoice ID and Check ID   to Table : THL_DATA_LOADED
          V 1.2.0 by Companion : Damrong Tantiwiwatsakul <Boy>
                  Last Updated : 05-FEB-2009 19.00.00
                   Detail      : Change Payment Base Amount Query at WHT Auto.
          V 1.3.0 by Companion : Damrong Tantiwiwatsakul <Boy>
                  Last Updated : 07-FEB-2009 19.00.00
                   Detail      : Insert cause : zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                                 into Tax Invoices Goods Query

          V 1.4.0 by Companion : Damrong Tantiwiwatsakul <Boy>
                  Last Updated : 16-JUN-2009 19.00.00
                   Detail      : Update case cannot load data void payment to table load

          V 2.0.0  By Suwatchai
                 Last updated : 10-APR-2012
                 Detail  : Add calculate tax amount  (tax_amt2) for tax service
   **/
   /*================================================================

     PROCEDURE NAME:    THL_LOAD_TAXINV_GOODS()

   ==================================================================*/
   PROCEDURE thl_load_taxinv_goods (p_conc_request_id IN NUMBER, p_invoice_id IN NUMBER)
   IS
      user_id                         NUMBER;
      org_id                          NUMBER;

      -- Cursor for Load Data to Table : thl_taxinvoices_header and thl_taxinvoices_line
      CURSOR cursor_head
      IS
           SELECT ai.invoice_id transaction_id,
                  ai.invoice_num transaction_number,
                  ai.invoice_date transaction_date,
                  TRUNC (ail.accounting_date) gl_date,
                  'G' type_tax,
                  ail.tax_classification_code tax_code,
                  NVL (atc.percentage_rate, 0) tax_rate,
                  hla.location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value) voucher_number,
                  NULL bank_acct_number,
                  NULL bank_acct_name,
                  ail.project_id,
                  pv.vendor_id supplier_id,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  pv.vendor_name supplier_name,
                  ai.gl_date rcp_tax_inv_date
             FROM ap_invoices ai,
                  ap_invoice_lines ail,
                  ap_invoice_distributions aid,
                  (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                  ap_suppliers pv,
                  hr_locations hla,
                  (  SELECT invoice_id, tax_rate_id, tax_rate_code
                       FROM ap_invoice_lines
                      WHERE line_type_lookup_code = 'TAX'
                   GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                  xla_distribution_links xla
            WHERE     ai.invoice_id = ail.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.tax_classification_code = atc.tax(+)
                  AND ail.tax_classification_code = zrb.tax_rate_code
                  AND zrb.active_flag = 'Y'
                  AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                  AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                  AND ail.line_type_lookup_code IN ('ITEM')
                  AND ai.invoice_id IN (SELECT invoice_id
                                          FROM ap_invoice_lines
                                         WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                  AND ai.vendor_id = pv.vendor_id
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND (aid.invoice_distribution_id,
                       0,
                       'AP_INVOICE_DISTRIBUTIONS_GOODS',
                       ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                     0,
                                                     tidl.reference_type,
                                                     tidl.invoice_id
                                                FROM thl_data_loaded tidl
                                               WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                  AND ai.invoice_id = inv_tax.invoice_id
                  AND ail.tax_classification_code = inv_tax.tax_rate_code
                  AND zrb.tax_rate_id = inv_tax.tax_rate_id
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'LIABILITY'
                  AND xla.source_distribution_type = 'AP_INV_DIST'
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_GOODS',
                       ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.invoice_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                  AND zrb.description = hla.location_code
                  AND ai.invoice_id = p_invoice_id
         GROUP BY ai.invoice_id,
                  ai.invoice_num,
                  ai.invoice_date,
                  TRUNC (ail.accounting_date),
                  ail.tax_classification_code,
                  hla.location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value),
                  ail.project_id,
                  pv.vendor_id,
                  pv.vendor_name,
                  atc.percentage_rate,
                  ai.gl_date
         ORDER BY DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value),
                  ai.invoice_id,
                  ail.tax_classification_code,
                  ail.project_id;

      rec_head                        cursor_head%ROWTYPE;

      -- Cursor for Load Data to Table : thl_taxinvoices_header and thl_taxinvoices_line
      CURSOR cursor_line (
         cp_invoice_id                 NUMBER,
         cp_invoice_date               DATE,
         cp_tax_classification_code    VARCHAR2)
      IS
           SELECT ai.invoice_id transaction_id,
                  ai.invoice_num transaction_number,
                  ai.invoice_date transaction_date,
                  TRUNC (ail.accounting_date) gl_date,
                  'G' type_tax,
                  ail.tax_classification_code tax_code,
                  NVL (atc.percentage_rate, 0) tax_rate,
                  hla.location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value) voucher_number,
                  NULL bank_acct_number,
                  NULL bank_acct_name,
                  ail.project_id,
                  pv.vendor_id supplier_id,
                  NVL (ail.attribute2, pv.vendor_name) supplier_name,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updatedby,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  DECODE (INSTR (ail.tax_classification_code, '36'),
                          0, NVL (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0), 0),
                          NVL (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0), 0))
                     base_amt,
                  NVL (
                     (SELECT SUM (NVL (NVL (xla_nrec.unrounded_accounted_cr, 0) - NVL (xla_nrec.unrounded_accounted_dr, 0), 0))
                        FROM ap_invoice_lines ail_nrec, ap_invoice_distributions aid_nrec, xla_distribution_links xla_nrec
                       WHERE     aid_nrec.invoice_id = ai.invoice_id
                             AND aid_nrec.charge_applicable_to_dist_id = aid.invoice_distribution_id
                             AND aid_nrec.line_type_lookup_code = 'REC_TAX'
                             AND xla_nrec.source_distribution_id_num_1 = aid_nrec.invoice_distribution_id
                             AND xla_nrec.rounding_class_code = 'LIABILITY'
                             AND xla_nrec.source_distribution_type = 'AP_INV_DIST'
                             AND ail_nrec.invoice_id = aid_nrec.invoice_id
                             AND ail_nrec.line_number = aid_nrec.invoice_line_number),
                     0)
                     AS rec_tax,
                  NVL (
                     (SELECT SUM (NVL (NVL (xla_nrec.unrounded_accounted_cr, 0) - NVL (xla_nrec.unrounded_accounted_dr, 0), 0))
                        FROM ap_invoice_lines ail_nrec, ap_invoice_distributions aid_nrec, xla_distribution_links xla_nrec
                       WHERE     aid_nrec.invoice_id = ai.invoice_id
                             AND aid_nrec.charge_applicable_to_dist_id = aid.invoice_distribution_id
                             AND aid_nrec.line_type_lookup_code = 'NONREC_TAX'
                             AND xla_nrec.source_distribution_id_num_1 = aid_nrec.invoice_distribution_id
                             AND xla_nrec.rounding_class_code = 'LIABILITY'
                             AND xla_nrec.source_distribution_type = 'AP_INV_DIST'
                             AND ail_nrec.invoice_id = aid_nrec.invoice_id
                             AND ail_nrec.line_number = aid_nrec.invoice_line_number),
                     0)
                     AS non_rec_tax,
                  NVL (
                       ( (SELECT SUM (NVL (NVL (xla_nrec.unrounded_accounted_cr, 0) - NVL (xla_nrec.unrounded_accounted_dr, 0), 0))
                            FROM ap_invoice_lines ail_nrec, ap_invoice_distributions aid_nrec, xla_distribution_links xla_nrec
                           WHERE     aid_nrec.invoice_id = ai.invoice_id
                                 AND aid_nrec.charge_applicable_to_dist_id = aid.invoice_distribution_id
                                 AND aid_nrec.line_type_lookup_code = 'REC_TAX'
                                 AND xla_nrec.source_distribution_id_num_1 = aid_nrec.invoice_distribution_id
                                 AND xla_nrec.rounding_class_code = 'LIABILITY'
                                 AND xla_nrec.source_distribution_type = 'AP_INV_DIST'
                                 AND ail_nrec.invoice_id = aid_nrec.invoice_id
                                 AND ail_nrec.line_number = aid_nrec.invoice_line_number))
                     + ( (SELECT SUM (NVL (NVL (xla_nrec.unrounded_accounted_cr, 0) - NVL (xla_nrec.unrounded_accounted_dr, 0), 0))
                            FROM ap_invoice_lines ail_nrec, ap_invoice_distributions aid_nrec, xla_distribution_links xla_nrec
                           WHERE     aid_nrec.invoice_id = ai.invoice_id
                                 AND aid_nrec.charge_applicable_to_dist_id = aid.invoice_distribution_id
                                 AND aid_nrec.line_type_lookup_code = 'NONREC_TAX'
                                 AND xla_nrec.source_distribution_id_num_1 = aid_nrec.invoice_distribution_id
                                 AND xla_nrec.rounding_class_code = 'LIABILITY'
                                 AND xla_nrec.source_distribution_type = 'AP_INV_DIST'
                                 AND ail_nrec.invoice_id = aid_nrec.invoice_id
                                 AND ail_nrec.line_number = aid_nrec.invoice_line_number)),
                     0)
                     AS tax_amt
             FROM ap_invoices ai,
                  ap_invoice_lines ail,
                  ap_invoice_distributions aid,
                  (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate, NVL (a.percentage_rate, 0) tax_rate
                       FROM zx_rates_b a, zx_rates_b b
                      WHERE     a.active_flag = 'Y'
                            AND NVL (a.vat_transaction_type_code, 'NULL') <> 'N/A'
                            AND a.default_rec_rate_code = b.tax_rate_code(+)
                            AND b.rate_type_code(+) = 'RECOVERY'
                            AND b.active_flag(+) = 'Y'
                            AND b.effective_from(+) <= SYSDATE
                            AND NVL (b.effective_to(+), SYSDATE) >= SYSDATE
                   GROUP BY a.tax_rate_code, NVL (b.percentage_rate, 0), NVL (a.percentage_rate, 0)) atc,
                  zx_rates_b zrb,
                  ap_suppliers pv,
                  hr_locations hla,
                  (  SELECT invoice_id, tax_rate_id, tax_rate_code
                       FROM ap_invoice_lines
                      WHERE line_type_lookup_code = 'TAX'
                   GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                  xla_distribution_links xla
            WHERE     ai.invoice_id = ail.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.tax_classification_code = atc.tax(+)
                  AND ail.tax_classification_code = zrb.tax_rate_code
                  AND zrb.active_flag = 'Y'
                  AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                  AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                  AND ail.line_type_lookup_code IN ('ITEM')
                  AND ai.invoice_id IN (SELECT invoice_id
                                          FROM ap_invoice_lines
                                         WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                  AND ai.vendor_id = pv.vendor_id
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND (aid.invoice_distribution_id,
                       0,
                       'AP_INVOICE_DISTRIBUTIONS_GOODS',
                       ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                     0,
                                                     tidl.reference_type,
                                                     tidl.invoice_id
                                                FROM thl_data_loaded tidl
                                               WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                  AND ai.invoice_id = inv_tax.invoice_id
                  AND ail.tax_classification_code = inv_tax.tax_rate_code
                  AND zrb.tax_rate_id = inv_tax.tax_rate_id
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'LIABILITY'
                  AND xla.source_distribution_type = 'AP_INV_DIST'
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_GOODS',
                       ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.invoice_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                  AND ai.invoice_id = cp_invoice_id
                  AND ai.invoice_date = cp_invoice_date
                  AND ail.tax_classification_code = cp_tax_classification_code
                  AND zrb.description = hla.location_code
         ORDER BY ail.line_number;

      rec_line                        cursor_line%ROWTYPE;

      -- Cursor for Load Data to Table : thl_data_loaded
      CURSOR cursor_dis
      IS
         SELECT DISTINCT ai.invoice_id, aid.invoice_distribution_id, xla.event_id
           FROM ap_invoices ai,
                ap_invoice_lines ail,
                ap_invoice_distributions aid,
                (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                ap_suppliers pv,
                hr_locations hla,
                (  SELECT invoice_id, tax_rate_id, tax_rate_code
                     FROM ap_invoice_lines
                    WHERE line_type_lookup_code = 'TAX'
                 GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                xla_distribution_links xla
          WHERE     ai.invoice_id = ail.invoice_id
                AND ail.invoice_id = aid.invoice_id
                AND ail.line_number = aid.invoice_line_number
                AND ail.tax_classification_code = atc.tax(+)
                AND ail.tax_classification_code = zrb.tax_rate_code
                AND zrb.active_flag = 'Y'
                AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                AND ail.line_type_lookup_code IN ('ITEM')
                AND ai.invoice_id IN (SELECT invoice_id
                                        FROM ap_invoice_lines
                                       WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                AND ai.vendor_id = pv.vendor_id
                AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                         ai.invoice_amount,
                                                         ai.payment_status_flag,
                                                         ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                AND (aid.invoice_distribution_id,
                     0,
                     'AP_INVOICE_DISTRIBUTIONS_GOODS',
                     ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                   0,
                                                   tidl.reference_type,
                                                   tidl.invoice_id
                                              FROM thl_data_loaded tidl
                                             WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                AND ai.invoice_id = inv_tax.invoice_id
                AND ail.tax_classification_code = inv_tax.tax_rate_code
                AND zrb.tax_rate_id = inv_tax.tax_rate_id
                AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                AND xla.rounding_class_code = 'LIABILITY'
                AND xla.source_distribution_type = 'AP_INV_DIST'
                AND (xla.event_id,
                     0,
                     'XLA_DISTRIBUTION_LINKS_GOODS',
                     ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.invoice_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                AND ai.invoice_id = p_invoice_id
                AND zrb.description = hla.location_code
         UNION ALL
         SELECT DISTINCT ai.invoice_id, aid.invoice_distribution_id, xla.event_id
           FROM ap_invoices ai,
                ap_invoice_lines ail,
                ap_invoice_distributions aid,
                (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                ap_suppliers pv,
                hr_locations hla,
                (  SELECT invoice_id, tax_rate_id, tax_rate_code
                     FROM ap_invoice_lines
                    WHERE line_type_lookup_code = 'TAX'
                 GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                xla_distribution_links xla
          WHERE     ai.invoice_id = ail.invoice_id
                AND ail.invoice_id = aid.invoice_id
                AND ail.line_number = aid.invoice_line_number
                AND ail.tax_rate_code = atc.tax(+)
                AND ail.tax_rate_code = zrb.tax_rate_code
                AND zrb.active_flag = 'Y'
                AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                AND ail.line_type_lookup_code IN ('TAX')
                AND aid.line_type_lookup_code = 'REC_TAX'
                AND ai.vendor_id = pv.vendor_id
                AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                         ai.invoice_amount,
                                                         ai.payment_status_flag,
                                                         ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                AND (aid.invoice_distribution_id,
                     0,
                     'AP_INVOICE_DISTRIBUTIONS_GOODS',
                     ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                   0,
                                                   tidl.reference_type,
                                                   tidl.invoice_id
                                              FROM thl_data_loaded tidl
                                             WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                AND ai.invoice_id = inv_tax.invoice_id
                AND ail.tax_rate_code = inv_tax.tax_rate_code
                AND zrb.tax_rate_id = inv_tax.tax_rate_id
                AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                AND xla.rounding_class_code = 'LIABILITY'
                AND xla.source_distribution_type = 'AP_INV_DIST'
                AND (xla.event_id,
                     0,
                     'XLA_DISTRIBUTION_LINKS_GOODS',
                     ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.invoice_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                AND ai.invoice_id = p_invoice_id
                AND zrb.description = hla.location_code
         UNION ALL
         SELECT DISTINCT ai.invoice_id, aid.invoice_distribution_id, xla.event_id
           FROM ap_invoices ai,
                ap_invoice_lines ail,
                ap_invoice_distributions aid,
                (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                ap_suppliers pv,
                hr_locations hla,
                (  SELECT invoice_id, tax_rate_id, tax_rate_code
                     FROM ap_invoice_lines
                    WHERE line_type_lookup_code = 'TAX'
                 GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                xla_distribution_links xla
          WHERE     ai.invoice_id = ail.invoice_id
                AND ail.invoice_id = aid.invoice_id
                AND ail.line_number = aid.invoice_line_number
                AND ail.tax_rate_code = atc.tax(+)
                AND ail.tax_rate_code = zrb.tax_rate_code
                AND zrb.active_flag = 'Y'
                AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                AND ail.line_type_lookup_code IN ('TAX')
                AND aid.line_type_lookup_code = 'NONREC_TAX'
                AND ai.vendor_id = pv.vendor_id
                AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                         ai.invoice_amount,
                                                         ai.payment_status_flag,
                                                         ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                AND (aid.invoice_distribution_id,
                     0,
                     'AP_INVOICE_DISTRIBUTIONS_GOODS',
                     ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                   0,
                                                   tidl.reference_type,
                                                   tidl.invoice_id
                                              FROM thl_data_loaded tidl
                                             WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                AND ai.invoice_id = inv_tax.invoice_id
                AND ail.tax_rate_code = inv_tax.tax_rate_code
                AND zrb.tax_rate_id = inv_tax.tax_rate_id
                AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                AND xla.rounding_class_code = 'LIABILITY'
                AND xla.source_distribution_type = 'AP_INV_DIST'
                AND (xla.event_id,
                     0,
                     'XLA_DISTRIBUTION_LINKS_GOODS',
                     ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.invoice_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                AND ai.invoice_id = p_invoice_id
                AND zrb.description = hla.location_code;

      rec_dis                         cursor_dis%ROWTYPE;
      v_head_id                       NUMBER;
      v_line_id                       NUMBER;
      base_amt                        NUMBER;
      rec_tax                         NUMBER;
      non_rec_tax                     NUMBER;
      tax_amt                         NUMBER;
      base_amt_sum                    NUMBER;
      rec_tax_sum                     NUMBER;
      non_rec_tax_sum                 NUMBER;
      tax_amt_sum                     NUMBER;
      xproject_id                     NUMBER;
      xbproject                       BOOLEAN;
      v_insert                        BOOLEAN;
      v_nperiod                       VARCHAR (10);
      v_ai_invoice_id                 NUMBER (15);
      v_ai_invoice_amount             NUMBER;
      v_ai_payment_status_flag        VARCHAR2 (1 BYTE);
      v_ai_invoice_type_lookup_code   VARCHAR2 (25 BYTE);
      v_group                         VARCHAR2 (50);
       v_sts                           BOOLEAN;
   BEGIN
      user_id := fnd_profile.VALUE ('USER_ID');
      org_id := fnd_profile.VALUE ('ORG_ID');
      xbproject := FALSE;
      v_insert := FALSE;
      v_group := 'Y';

      BEGIN
         SELECT NVL (attribute4, 'Y')
           INTO v_group
           FROM ap_invoices_all
          WHERE invoice_id = p_invoice_id;
      EXCEPTION
         WHEN OTHERS
         THEN
            v_group := 'Y';
      END;

      IF v_group = 'Y'
      THEN
         OPEN cursor_head;

         LOOP
            FETCH cursor_head INTO rec_head;

            EXIT WHEN cursor_head%NOTFOUND;
            rec_tax := 0;
            non_rec_tax := 0;
            tax_amt := 0;
            base_amt := 0;
            write_log ('Invoice Number : ' || rec_head.transaction_number);
            -- Calculate BASE AMOUNT
            write_log ('10 begin group : Calculate BASE AMOUNT');

            IF INSTR (rec_head.tax_code, '36') = 0
            THEN
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                    INTO base_amt
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_classification_code = atc.tax(+)
                         AND ail.tax_classification_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('ITEM')
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_classification_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_classification_code = rec_head.tax_code
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     base_amt := 0;
               END;
            ELSE
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                    INTO base_amt
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_classification_code = atc.tax(+)
                         AND ail.tax_classification_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('ITEM')
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_classification_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_classification_code = rec_head.tax_code
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     base_amt := 0;
               END;
            END IF;

            write_log ('10 end group : Calculate BASE AMOUNT');

            -- Calculate RECOVERABLE RATE AMOUNT
            write_log ('20 begin group : Calculate RECOVERABLE RATE AMOUNT');

            IF INSTR (rec_head.tax_code, '36') = 0
            THEN
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                    INTO rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'REC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     rec_tax := 0;
               END;
            ELSE
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                    INTO rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'REC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     rec_tax := 0;
               END;
            END IF;

            write_log ('20 end group : Calculate RECOVERABLE RATE AMOUNT');

            -- Calculate NON RECOVERABLE RATE AMOUNT
            write_log ('30 begin group : Calculate NON RECOVERABLE RATE AMOUNT');

            IF INSTR (rec_head.tax_code, '36') = 0
            THEN
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                    INTO non_rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'NONREC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     non_rec_tax := 0;
               END;
            ELSE
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                    INTO non_rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'NONREC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     non_rec_tax := 0;
               END;
            END IF;

            write_log ('30 end group : Calculate NON RECOVERABLE RATE AMOUNT');

            tax_amt := rec_tax + non_rec_tax;
            -- Find Period Name from GL DATE
            write_log ('40 begin group : Find Period Name from GL DATE');

              SELECT period_name
                INTO v_nperiod
                FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
               WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                     AND hou.set_of_books_id = gsob.set_of_books_id
                     AND gsob.period_set_name = gpv.period_set_name
                     AND hou.organization_id = org_id
                     AND (TRUNC (rec_head.gl_date) BETWEEN gpv.start_date AND gpv.end_date)
                     AND gpv.adjustment_period_flag = 'N'
            ORDER BY start_date;

            write_log ('40 end group : Find Period Name from GL DATE');
            v_insert := TRUE;
            -- Insert Data to table : THL_TAXINVOICES_HEADER
            write_log ('50 begin group : Insert Data to table : THL_TAXINVOICES_HEADER');

            SELECT thl_taxinvh_s.NEXTVAL INTO v_head_id  FROM DUAL;

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
                         non_rec_tax,
                         rec_tax,
                         rec_head.tax_rate,
                         base_amt,
                         p_conc_request_id,
                         rec_head.created_by,
                         rec_head.creation_date,
                         rec_head.last_updated_by,
                         rec_head.last_update_date,
                         rec_head.last_update_login);
            EXCEPTION
               WHEN OTHERS
               THEN
                  write_log ('error' || SQLERRM);
                  v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
            END;


            write_log ('50 end group : Insert Data to table : THL_TAXINVOICES_HEADER');
            -- Insert Data to table : THL_TAXINVOICES_LINE
            write_log ('60 begin group : Insert Data to table : THL_TAXINVOICES_LINE');

            SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

            INSERT INTO thl_taxinvoices_line (th_til_id,
                                              th_tih_id,
                                              th_til_mode,
                                              th_til_period_name,
                                              th_til_tax_inv_id,
                                              th_til_location_id,
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
                                              rcp_tax_inv_date)
                 VALUES (v_line_id,
                         v_head_id,
                         'A',
                         v_nperiod,
                         rec_head.transaction_id,
                         rec_head.location_id,
                         rec_head.transaction_number,
                         rec_head.transaction_date,
                         rec_head.supplier_name,
                         rec_head.project_id,
                         base_amt,
                         tax_amt,
                         non_rec_tax,
                         rec_tax,
                         p_conc_request_id,
                         rec_head.created_by,
                         rec_head.creation_date,
                         rec_head.last_updated_by,
                         rec_head.last_update_date,
                         rec_head.last_update_login,
                         rec_head.rcp_tax_inv_date);

            write_log ('60 end group : Insert Data to table : THL_TAXINVOICES_LINE');
         END LOOP;

         CLOSE cursor_head;

         -- Insert Data to table : THL_DATA_LOADED
         -- For Check Data Loaded
         IF v_insert
         THEN
            write_log ('70 begin group : Insert Data to table : THL_DATA_LOADED');

            OPEN cursor_dis;

            LOOP
               FETCH cursor_dis INTO rec_dis;

               EXIT WHEN cursor_dis%NOTFOUND;

               INSERT INTO thl_data_loaded (reference_id,
                                            reference_line_number,
                                            reference_type,
                                            invoice_id,
                                            created_by,
                                            creation_date,
                                            last_updated_by,
                                            last_update_date,
                                            last_update_login)
                    VALUES (rec_dis.invoice_distribution_id,
                            0,
                            'AP_INVOICE_DISTRIBUTIONS_GOODS',
                            rec_dis.invoice_id,
                            user_id,
                            SYSDATE,
                            user_id,
                            SYSDATE,
                            user_id);

               INSERT INTO thl_data_loaded (reference_id,
                                            reference_line_number,
                                            reference_type,
                                            invoice_id,
                                            created_by,
                                            creation_date,
                                            last_updated_by,
                                            last_update_date,
                                            last_update_login)
                    VALUES (rec_dis.event_id,
                            0,
                            'XLA_DISTRIBUTION_LINKS_GOODS',
                            rec_dis.invoice_id,
                            user_id,
                            SYSDATE,
                            user_id,
                            SYSDATE,
                            user_id);
            END LOOP;

            CLOSE cursor_dis;

            write_log ('70 end group : Insert Data to table : THL_DATA_LOADED');
         END IF;
      ELSIF v_group = 'N'
      THEN
         OPEN cursor_head;

         LOOP
            FETCH cursor_head INTO rec_head;

            EXIT WHEN cursor_head%NOTFOUND;
            rec_tax := 0;
            non_rec_tax := 0;
            tax_amt := 0;
            base_amt := 0;
            write_log ('Invoice Number : ' || rec_head.transaction_number);
            -- Calculate BASE AMOUNT
            write_log ('10 begin no group : Calculate BASE AMOUNT');

            IF INSTR (rec_head.tax_code, '36') = 0
            THEN
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                    INTO base_amt
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_classification_code = atc.tax(+)
                         AND ail.tax_classification_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('ITEM')
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_classification_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_classification_code = rec_head.tax_code
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     base_amt := 0;
               END;
            ELSE
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                    INTO base_amt
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_classification_code = atc.tax(+)
                         AND ail.tax_classification_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('ITEM')
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_classification_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_classification_code = rec_head.tax_code
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     base_amt := 0;
               END;
            END IF;

            write_log ('10 end no group : Calculate BASE AMOUNT');
            -- Calculate RECOVERABLE RATE AMOUNT
            write_log ('20 begin no group : Calculate RECOVERABLE RATE AMOUNT');

            IF INSTR (rec_head.tax_code, '36') = 0
            THEN
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                    INTO rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'REC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     rec_tax := 0;
               END;
            ELSE
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                    INTO rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'REC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     rec_tax := 0;
               END;
            END IF;

            write_log ('20 end no group : Calculate RECOVERABLE RATE AMOUNT');
            -- Calculate NON RECOVERABLE RATE AMOUNT
            write_log ('30 begin no group : Calculate NON RECOVERABLE RATE AMOUNT');

            IF INSTR (rec_head.tax_code, '36') = 0
            THEN
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                    INTO non_rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'NONREC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     non_rec_tax := 0;
               END;
            ELSE
               BEGIN
                  SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                    INTO non_rec_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines ail,
                         ap_invoice_distributions aid,
                         (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                         ap_suppliers pv,
                         hr_locations hla,
                         (  SELECT invoice_id, tax_rate_id, tax_rate_code
                              FROM ap_invoice_lines
                             WHERE line_type_lookup_code = 'TAX'
                          GROUP BY invoice_id, tax_rate_id, tax_rate_code) inv_tax,
                         xla_distribution_links xla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.tax_rate_code = atc.tax(+)
                         AND ail.tax_rate_code = zrb.tax_rate_code
                         AND zrb.active_flag = 'Y'
                         AND zrb.def_rec_settlement_option_code = 'IMMEDIATE'
                         AND NVL (zrb.vat_transaction_type_code, 'NULL') <> 'N/A'
                         AND ail.line_type_lookup_code IN ('TAX')
                         AND aid.line_type_lookup_code = 'NONREC_TAX'
                         AND ai.vendor_id = pv.vendor_id
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                            0,
                                                            tidl.reference_type,
                                                            tidl.invoice_id
                                                       FROM thl_data_loaded tidl
                                                      WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_GOODS' AND tidl.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = inv_tax.invoice_id
                         AND ail.tax_rate_code = inv_tax.tax_rate_code
                         AND zrb.tax_rate_id = inv_tax.tax_rate_id
                         AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                         AND xla.rounding_class_code = 'LIABILITY'
                         AND xla.source_distribution_type = 'AP_INV_DIST'
                         AND (xla.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_GOODS',
                              ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                            0,
                                                            tidld.reference_type,
                                                            tidld.invoice_id
                                                       FROM thl_data_loaded tidld
                                                      WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_GOODS' AND tidld.reference_id = xla.event_id)
                         AND ai.invoice_id = rec_head.transaction_id
                         AND ail.tax_rate_code = rec_head.tax_code
                         AND NVL (ail.project_id, 0) = NVL (rec_head.project_id, 0)
                         AND TRUNC (ail.accounting_date) = rec_head.gl_date
                         AND zrb.description = hla.location_code;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     non_rec_tax := 0;
               END;
            END IF;

            write_log ('30 end no group : Calculate NON RECOVERABLE RATE AMOUNT');
            tax_amt := rec_tax + non_rec_tax;
            -- Find Period Name from GL DATE
            write_log ('40 begin no group : Find Period Name from GL DATE');

              SELECT period_name
                INTO v_nperiod
                FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
               WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                     AND hou.set_of_books_id = gsob.set_of_books_id
                     AND gsob.period_set_name = gpv.period_set_name
                     AND hou.organization_id = org_id
                     AND (TRUNC (rec_head.gl_date) BETWEEN gpv.start_date AND gpv.end_date)
                     AND gpv.adjustment_period_flag = 'N'
            ORDER BY start_date;

            write_log ('40 end no group : Find Period Name from GL DATE');
            v_insert := TRUE;
            -- Insert Data to table : THL_TAXINVOICES_HEADER
            write_log ('50 begin no group : Insert Data to table : THL_TAXINVOICES_HEADER');

            SELECT thl_taxinvh_s.NEXTVAL INTO v_head_id FROM DUAL;
          
         
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
                         non_rec_tax,
                         rec_tax,
                         rec_head.tax_rate,
                         base_amt,
                         p_conc_request_id,
                         rec_head.created_by,
                         rec_head.creation_date,
                         rec_head.last_updated_by,
                         rec_head.last_update_date,
                         rec_head.last_update_login);
            EXCEPTION
               WHEN OTHERS
               THEN
                  write_log ('error:  ' || SQLERRM);
                  v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
            END;

            write_log ('50 end no group : Insert Data to table : THL_TAXINVOICES_HEADER');
            -- Insert Data to table : THL_TAXINVOICES_LINE
            write_log ('60 begin no group : Insert Data to table : THL_TAXINVOICES_LINE');

            OPEN cursor_line (rec_head.transaction_id, rec_head.transaction_date, rec_head.tax_code);

            LOOP
               FETCH cursor_line INTO rec_line;

               EXIT WHEN cursor_line%NOTFOUND;

               SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;
               
             BEGIN

               INSERT INTO thl_taxinvoices_line (th_til_id,
                                                 th_tih_id,
                                                 th_til_mode,
                                                 th_til_period_name,
                                                 th_til_tax_inv_id,
                                                 th_til_location_id,
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
                                                 rcp_tax_inv_date)
                    VALUES (v_line_id,
                            v_head_id,
                            'A',
                            v_nperiod,
                            rec_head.transaction_id,
                            rec_head.location_id,
                            rec_head.transaction_number,
                            rec_head.transaction_date,
                            rec_line.supplier_name,
                            rec_head.project_id,
                            rec_line.base_amt,
                            rec_line.tax_amt,
                            rec_line.non_rec_tax,
                            rec_line.rec_tax,
                            p_conc_request_id,
                            rec_head.created_by,
                            rec_head.creation_date,
                            rec_head.last_updated_by,
                            rec_head.last_update_date,
                            rec_head.last_update_login,
                            rec_head.rcp_tax_inv_date);
                            
            EXCEPTION
               WHEN OTHERS
               THEN
                  write_log ('error:  ' || SQLERRM);
                  v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
            END;
            
            END LOOP;

            CLOSE cursor_line;

            write_log ('60 end no group : Insert Data to table : THL_TAXINVOICES_LINE');
         END LOOP;

         CLOSE cursor_head;

         -- Insert Data to table : THL_DATA_LOADED
         -- For Check Data Loaded
         IF v_insert
         THEN
            write_log ('70 begin no group : Insert Data to table : THL_DATA_LOADED');

            OPEN cursor_dis;

            LOOP
               FETCH cursor_dis INTO rec_dis;

               EXIT WHEN cursor_dis%NOTFOUND;

               INSERT INTO thl_data_loaded (reference_id,
                                            reference_line_number,
                                            invoice_id,
                                            reference_type,
                                            created_by,
                                            creation_date,
                                            last_updated_by,
                                            last_update_date,
                                            last_update_login)
                    VALUES (rec_dis.invoice_distribution_id,
                            0,
                            rec_dis.invoice_id,
                            'AP_INVOICE_DISTRIBUTIONS_GOODS',
                            user_id,
                            SYSDATE,
                            user_id,
                            SYSDATE,
                            user_id);

               INSERT INTO thl_data_loaded (reference_id,
                                            reference_line_number,
                                            reference_type,
                                            invoice_id,
                                            created_by,
                                            creation_date,
                                            last_updated_by,
                                            last_update_date,
                                            last_update_login)
                    VALUES (rec_dis.event_id,
                            0,
                            'XLA_DISTRIBUTION_LINKS_GOODS',
                            rec_dis.invoice_id,
                            user_id,
                            SYSDATE,
                            user_id,
                            SYSDATE,
                            user_id);
            END LOOP;

            CLOSE cursor_dis;

            write_log ('70 end no group : Insert Data to table : THL_DATA_LOADED');
         END IF;
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (-20101, 'Saving Error!');
         ROLLBACK;
   END thl_load_taxinv_goods;

   /*================================================================

     PROCEDURE NAME:    THL_LOAD_taxinv_service()

   ==================================================================*/
   PROCEDURE thl_load_taxinv_service (p_conc_request_id IN NUMBER, p_check_id IN NUMBER)
   IS
      user_id                         NUMBER;
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
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  inv_tax.tax_rate,
                  ai.gl_date rcp_tax_inv_date
             FROM ap_invoices ai,
                  ap_invoice_lines ail,
                  ap_invoice_distributions aid,
                  (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                  (  SELECT invoice_id,
                            tax_rate_id,
                            tax_rate_code,
                            tax_rate
                       FROM ap_invoice_lines
                      WHERE line_type_lookup_code = 'TAX'
                   GROUP BY invoice_id,
                            tax_rate_id,
                            tax_rate_code,
                            tax_rate) inv_tax,
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
                                          FROM ap_invoice_lines
                                         WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND ( (aip.invoice_payment_id,
                         0,
                         'AP_INVOICE_PAYMENTS_SERVICE',
                         ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.check_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id))
                  AND ai.invoice_id = aip.invoice_id
                  AND aip.check_id = ac.check_id
                  AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
                  AND cbau.bank_account_id = cba.bank_account_id
                  AND cbb.branch_party_id = cba.bank_branch_id
                  AND ai.invoice_id = inv_tax.invoice_id
                  AND ail.tax_classification_code = inv_tax.tax_rate_code
                  AND zrb.tax_rate_id = inv_tax.tax_rate_id
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'LIABILITY'
                  AND xla.source_distribution_type = 'AP_INV_DIST'
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_SERVICE',
                       ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.check_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
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
                  inv_tax.tax_rate,
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
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login
             FROM ap_invoices ai,
                  ap_invoice_lines ail,
                  ap_invoice_distributions aid,
                  (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                       FROM ap_invoice_lines
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
                                          FROM ap_invoice_lines
                                         WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND ( ( (aip.invoice_payment_id,
                           0,
                           'AP_INVOICE_PAYMENTS_SERVICE',
                           ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                       0,
                                                       tidld.reference_type,
                                                       tidld.check_id
                                                  FROM thl_data_loaded tidld
                                                 WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)))
                  AND ai.invoice_id = aip.invoice_id
                  AND aip.check_id = ac.check_id
                  AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
                  AND cbau.bank_account_id = cba.bank_account_id
                  AND cbb.branch_party_id = cba.bank_branch_id
                  AND ai.invoice_id = inv_tax.invoice_id
                  AND ail.tax_classification_code = inv_tax.tax_rate_code
                  AND zrb.tax_rate_id = inv_tax.tax_rate_id
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'LIABILITY'
                  AND xla.source_distribution_type = 'AP_INV_DIST'
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_SERVICE',
                       ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.check_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                  AND ac.check_id = p_check_id
                  AND zrb.description = hla.location_code
                  AND NVL (ac.check_id, 0) = NVL (v_transaction_id, 0)
                  AND NVL (ail.tax_classification_code, '0') = NVL (v_tax_code, '0')
                  AND NVL (ac.doc_sequence_value, '0') = NVL (v_voucher_number, '0')
                  AND NVL (hla.location_id, 0) = NVL (v_location_id, 0)
                  AND NVL (cba.bank_account_num, '0') = NVL (v_bank_acct_number, '0')
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
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  DECODE (INSTR (ail.tax_classification_code, '36'),
                          0, NVL (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0), 0),
                          NVL (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0), 0))
                     base_amt,
                  DECODE (
                     INSTR (ail.tax_classification_code, '36'),
                     0,   ROUND (
                               ROUND (
                                  ROUND (ROUND (NVL (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0), 0) * atc.tax_rate, 2) / 100, 2) * atc.percentage_rate,
                                  2)
                             / 100,
                             2)
                        + ROUND (
                               ROUND (
                                    ROUND (ROUND (NVL (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0), 0) * atc.tax_rate, 2) / 100, 2)
                                  * (100 - atc.percentage_rate),
                                  2)
                             / 100,
                             2),
                       ROUND (
                            ROUND (ROUND (ROUND (NVL (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0), 0) * atc.tax_rate, 2) / 100, 2) * atc.percentage_rate,
                                   2)
                          / 100,
                          2)
                     + ROUND (
                            ROUND (
                                 ROUND (ROUND (NVL (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0), 0) * atc.tax_rate, 2) / 100, 2)
                               * (100 - atc.percentage_rate),
                               2)
                          / 100,
                          2))
                     tax_amt                                                                                                                    --Add by Suwatchai 11-APR-12   V 2.0
                            ,
                  (SELECT NVL (SUM (NVL (xdl.unrounded_entered_dr, 0) - NVL (xdl.unrounded_entered_cr, 0)), 0)
                     FROM xla_distribution_links xdl, xla_ae_headers xeh, xla.xla_transaction_entities xte
                    WHERE     xte.application_id = 200
                          AND xdl.application_id = xeh.application_id
                          AND xte.application_id = xeh.application_id
                          AND xdl.ae_header_id = xeh.ae_header_id
                          AND xte.entity_code = 'AP_PAYMENTS'
                          AND xte.source_id_int_1 = ac.check_id                                                                                                      -------- 254762
                          AND xte.entity_id = xeh.entity_id
                          AND xdl.rounding_class_code IN ('DEF_REC_TAX', 'LIABILITY')
                          AND xdl.tax_line_ref_id IS NOT NULL
                          AND xdl.applied_to_source_id_num_1 = ai.invoice_id                                                                                               --4597390
                          AND xdl.applied_to_dist_id_num_1 IN (SELECT aid2.invoice_distribution_id
                                                                 FROM ap_invoice_distributions_all aid2
                                                                WHERE     aid2.CHARGE_APPLICABLE_TO_DIST_ID = aid.invoice_distribution_id                                 -- 7998263
                                                                      AND aid2.invoice_id = ai.invoice_id                                                                  --4597390
                                                                      AND aid2.LINE_TYPE_LOOKUP_CODE = 'REC_TAX'))
                     tax_amt2                                                                                                              -- Added by Suwatchai  V.2.0    10-APR-12
             --
             FROM ap_invoices ai,
                  ap_invoice_lines ail,
                  ap_invoice_distributions aid,
                  (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate, NVL (a.percentage_rate, 0) tax_rate
                       FROM zx_rates_b a, zx_rates_b b
                      WHERE     a.active_flag = 'Y'
                            AND NVL (a.vat_transaction_type_code, 'NULL') <> 'N/A'
                            AND a.default_rec_rate_code = b.tax_rate_code(+)
                            AND b.rate_type_code(+) = 'RECOVERY'
                            AND b.active_flag(+) = 'Y'
                            AND b.effective_from(+) <= SYSDATE
                            AND NVL (b.effective_to(+), SYSDATE) >= SYSDATE
                   GROUP BY a.tax_rate_code, NVL (b.percentage_rate, 0), NVL (a.percentage_rate, 0)) atc,
                  zx_rates_b zrb,
                  ap_invoice_payments aip,
                  ap_checks ac,
                  ce_bank_accounts cba,
                  ce_bank_acct_uses_all cbau,
                  ce_bank_branches_v cbb,
                  hr_locations hla,
                  (  SELECT invoice_id, tax_rate_id, tax_rate_code
                       FROM ap_invoice_lines
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
                                          FROM ap_invoice_lines
                                         WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND ( ( (aip.invoice_payment_id,
                           0,
                           'AP_INVOICE_PAYMENTS_SERVICE',
                           ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                       0,
                                                       tidld.reference_type,
                                                       tidld.check_id
                                                  FROM thl_data_loaded tidld
                                                 WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)))
                  AND ai.invoice_id = aip.invoice_id
                  AND aip.check_id = ac.check_id
                  AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
                  AND cbau.bank_account_id = cba.bank_account_id
                  AND cbb.branch_party_id = cba.bank_branch_id
                  AND ai.invoice_id = inv_tax.invoice_id
                  AND ail.tax_classification_code = inv_tax.tax_rate_code
                  AND zrb.tax_rate_id = inv_tax.tax_rate_id
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'LIABILITY'
                  AND xla.source_distribution_type = 'AP_INV_DIST'
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_SERVICE',
                       ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.check_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
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
           FROM ap_invoices ai,
                ap_invoice_lines ail,
                ap_invoice_distributions aid,
                (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                     FROM ap_invoice_lines
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
                                        FROM ap_invoice_lines
                                       WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                         ai.invoice_amount,
                                                         ai.payment_status_flag,
                                                         ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                AND ( (aip.invoice_payment_id,
                       0,
                       'AP_INVOICE_PAYMENTS_SERVICE',
                       ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.check_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id))
                AND ai.invoice_id = aip.invoice_id
                AND aip.check_id = ac.check_id
                AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
                AND cbau.bank_account_id = cba.bank_account_id
                AND cbb.branch_party_id = cba.bank_branch_id
                AND ai.invoice_id = inv_tax.invoice_id
                AND ail.tax_classification_code = inv_tax.tax_rate_code
                AND zrb.tax_rate_id = inv_tax.tax_rate_id
                AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                AND xla.rounding_class_code = 'LIABILITY'
                AND xla.source_distribution_type = 'AP_INV_DIST'
                AND (xla.event_id,
                     0,
                     'XLA_DISTRIBUTION_LINKS_SERVICE',
                     ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                 0,
                                                 tidld.reference_type,
                                                 tidld.check_id
                                            FROM thl_data_loaded tidld
                                           WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
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
         write_log ('Check Number : ' || rec_head.transaction_number);

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

            IF v_group = 'Y' AND INSTR (rec_invoice.tax_code, '36') = 0                                                                                                     --Case 1
            THEN
               SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

               -- Calculate PAYMENT AMOUNT
               write_log ('10 begin group : Calculate BASE AMOUNT');

               BEGIN
                  SELECT NVL (SUM (NVL (xdl.unrounded_entered_dr, 0) - NVL (xdl.unrounded_entered_cr, 0)), 0)
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
                         AND xdl.applied_to_source_id_num_1 = rec_invoice.invoice_id
                         AND xdl.applied_to_dist_id_num_1 IN (SELECT MAX (aid.invoice_distribution_id)
                                                                FROM ap_invoice_lines_all ail, ap_invoice_distributions_all aid
                                                               WHERE     ail.invoice_id = aid.invoice_id
                                                                     AND ail.line_number = aid.invoice_line_number
                                                                     AND aid.invoice_distribution_id = xdl.applied_to_dist_id_num_1
                                                                     AND ail.tax_classification_code = rec_head.tax_code
                                                                     AND NVL (ail.project_id, 0) = NVL (rec_invoice.project_id, 0))
                         AND (xdl.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_SERVICE',
                              aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                           0,
                                                           tidld.reference_type,
                                                           tidld.check_id
                                                      FROM thl_data_loaded tidld
                                                     WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xdl.event_id)
                         AND (aip.invoice_payment_id,
                              0,
                              'AP_INVOICE_PAYMENTS_SERVICE',
                              aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                           0,
                                                           tidld.reference_type,
                                                           tidld.check_id
                                                      FROM thl_data_loaded tidld
                                                     WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_paid_base_inv := 0;
               END;

               write_log ('10 end group : Calculate BASE AMOUNT' || v_paid_base_inv);
               write_log ('20 begin group : Calculate REC TAX AMOUNT');


               write_log ('20 end group : Calculate REC TAX AMOUNT');
               write_log ('30 begin group : Calculate NON REC TAX AMOUNT');

               BEGIN
                  SELECT NVL (SUM (NVL (xdl.unrounded_entered_dr, 0) - NVL (xdl.unrounded_entered_cr, 0)), 0)
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
                         AND xdl.rounding_class_code IN ('DEF_REC_TAX', 'LIABILITY')
                         AND xdl.tax_line_ref_id IS NOT NULL
                         AND aip.check_id = rec_head.transaction_id
                         AND aip.invoice_id = rec_invoice.invoice_id
                         AND xdl.applied_to_source_id_num_1 = rec_invoice.invoice_id
                         AND xdl.applied_to_dist_id_num_1 IN (SELECT MAX (aid.invoice_distribution_id)
                                                                FROM ap_invoice_lines_all ail, ap_invoice_distributions_all aid
                                                               WHERE     ail.invoice_id = aid.invoice_id
                                                                     AND ail.line_number = aid.invoice_line_number
                                                                     AND aid.invoice_distribution_id = xdl.applied_to_dist_id_num_1
                                                                     AND aid.line_type_lookup_code = 'NONREC_TAX'
                                                                     AND ail.tax_rate_code = rec_head.tax_code
                                                                     AND NVL (ail.project_id, 0) = NVL (rec_invoice.project_id, 0))
                         AND (xdl.event_id,
                              0,
                              'XLA_DISTRIBUTION_LINKS_SERVICE',
                              aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                           0,
                                                           tidld.reference_type,
                                                           tidld.check_id
                                                      FROM thl_data_loaded tidld
                                                     WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xdl.event_id)
                         AND (aip.invoice_payment_id,
                              0,
                              'AP_INVOICE_PAYMENTS_SERVICE',
                              aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                           0,
                                                           tidld.reference_type,
                                                           tidld.check_id
                                                      FROM thl_data_loaded tidld
                                                     WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_paid_nonrec_tax := 0;
               END;

               write_log ('30 end group : Calculate NON REC TAX AMOUNT');
               -- Find Period Name from GL DATE
               write_log ('45 begin group : Find Period Name from GL DATE');

                 SELECT period_name
                   INTO v_nperiod
                   FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
                  WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                        AND hou.set_of_books_id = gsob.set_of_books_id
                        AND gsob.period_set_name = gpv.period_set_name
                        AND hou.organization_id = org_id
                        AND (TRUNC (rec_head.transaction_date) BETWEEN gpv.start_date AND gpv.end_date)
                        AND gpv.adjustment_period_flag = 'N'
               ORDER BY start_date;

               write_log ('45 end group : Find Period Name from GL DATE');
               -- Insert Data to table : THL_TAXINVOICES_LINE
               write_log ('50 begin group : Insert Data to table : THL_TAXINVOICES_LINE');

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
                               NULL,                                                                                                                                    --v_nperiod,
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
                     write_log ('Error insert THL_TAXINVOICES_LINE ' || SQLERRM);
                     v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
               END;

               write_log ('50 end group : Insert Data to table : THL_TAXINVOICES_LINE');
               v_check_base_inv := v_check_base_inv + v_paid_base_inv;
               v_check_rec_tax := v_check_rec_tax + v_paid_rec_tax;
               v_check_nonrec_tax := v_check_nonrec_tax + v_paid_nonrec_tax;
            ELSIF v_group = 'Y' AND INSTR (rec_invoice.tax_code, '36') <> 0                                                                                                -- Case 2
            THEN
               SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

               -- Calculate PAYMENT AMOUNT
               write_log ('10 begin group 36: Calculate PAYMENT AMOUNT');

               IF INSTR (rec_invoice.tax_code, '36') <> 0
               THEN
                  BEGIN
                     SELECT NVL (SUM (NVL (xdl.unrounded_entered_dr, 0) - NVL (xdl.unrounded_entered_cr, 0)), 0)
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
                            AND applied_to_source_id_num_1 = rec_invoice.invoice_id
                            AND (xdl.event_id,
                                 0,
                                 'XLA_DISTRIBUTION_LINKS_SERVICE',
                                 aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                              0,
                                                              tidld.reference_type,
                                                              tidld.check_id
                                                         FROM thl_data_loaded tidld
                                                        WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xdl.event_id)
                            AND (aip.invoice_payment_id,
                                 0,
                                 'AP_INVOICE_PAYMENTS_SERVICE',
                                 aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                              0,
                                                              tidld.reference_type,
                                                              tidld.check_id
                                                         FROM thl_data_loaded tidld
                                                        WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id);
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_payment_amt := 0;
                  END;
               END IF;

               write_log ('10 end group 36: Calculate PAYMENT AMOUNT');
               -- Calculate INVOICE AMOUNT
               write_log ('20 begin group 36: Calculate INVOICE AMOUNT');

               IF SIGN (v_payment_amt) = -1
               THEN
                  -- For Void Payment Cancel Invoice
                  IF INSTR (rec_invoice.tax_code, '36') <> 0
                  THEN
                     BEGIN
                        SELECT SUM (v_invoice_amt)
                          INTO v_invoice_amt
                          FROM (SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0)), 0) v_invoice_amt
                                  FROM ap_invoices ai,
                                       ap_invoice_lines ail,
                                       ap_invoice_distributions aid,
                                       ap_invoice_payments aip,
                                       ap_checks ac,
                                       xla_distribution_links xla
                                 WHERE     ai.invoice_id = ail.invoice_id
                                       AND ail.invoice_id = aid.invoice_id
                                       AND ail.line_number = aid.invoice_line_number
                                       AND ail.line_type_lookup_code IN ('ITEM')
                                       AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                                       AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                                       AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                                ai.invoice_amount,
                                                                                ai.payment_status_flag,
                                                                                ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                                       AND (aip.invoice_payment_id,
                                            0,
                                            'AP_INVOICE_PAYMENTS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                                       AND ai.invoice_id = aip.invoice_id
                                       AND aip.check_id = ac.check_id
                                       AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                                       AND xla.rounding_class_code = 'LIABILITY'
                                       AND xla.source_distribution_type = 'AP_INV_DIST'
                                       AND (xla.event_id,
                                            0,
                                            'XLA_DISTRIBUTION_LINKS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                                       AND ac.check_id = rec_head.transaction_id
                                       AND ai.invoice_id = rec_invoice.invoice_id
                                UNION ALL
                                SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0)), 0) v_invoice_amt
                                  FROM ap_invoices ai,
                                       ap_invoice_lines ail,
                                       ap_invoice_distributions aid,
                                       ap_invoice_payments aip,
                                       ap_checks ac,
                                       xla_distribution_links xla
                                 WHERE     ai.invoice_id = ail.invoice_id
                                       AND ail.invoice_id = aid.invoice_id
                                       AND ail.line_number = aid.invoice_line_number
                                       AND ail.line_type_lookup_code IN ('TAX')
                                       AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                                       AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                                       AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                                ai.invoice_amount,
                                                                                ai.payment_status_flag,
                                                                                ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                                       AND (aip.invoice_payment_id,
                                            0,
                                            'AP_INVOICE_PAYMENTS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                                       AND ai.invoice_id = aip.invoice_id
                                       AND aip.check_id = ac.check_id
                                       AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                                       AND xla.rounding_class_code = 'LIABILITY'
                                       AND xla.source_distribution_type = 'AP_INV_DIST'
                                       AND (xla.event_id,
                                            0,
                                            'XLA_DISTRIBUTION_LINKS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                                       AND ac.check_id = rec_head.transaction_id
                                       AND ai.invoice_id = rec_invoice.invoice_id);
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
                          FROM (SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0) v_invoice_amt
                                  FROM ap_invoices ai,
                                       ap_invoice_lines ail,
                                       ap_invoice_distributions aid,
                                       ap_invoice_payments aip,
                                       ap_checks ac,
                                       xla_distribution_links xla
                                 WHERE     ai.invoice_id = ail.invoice_id
                                       AND ail.invoice_id = aid.invoice_id
                                       AND ail.line_number = aid.invoice_line_number
                                       AND ail.line_type_lookup_code IN ('ITEM')
                                       AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                                       AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                                       AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                                ai.invoice_amount,
                                                                                ai.payment_status_flag,
                                                                                ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                                       AND (aip.invoice_payment_id,
                                            0,
                                            'AP_INVOICE_PAYMENTS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                                       AND ai.invoice_id = aip.invoice_id
                                       AND aip.check_id = ac.check_id
                                       AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                                       AND xla.rounding_class_code = 'LIABILITY'
                                       AND xla.source_distribution_type = 'AP_INV_DIST'
                                       AND (xla.event_id,
                                            0,
                                            'XLA_DISTRIBUTION_LINKS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                                       AND ac.check_id = rec_head.transaction_id
                                       AND ai.invoice_id = rec_invoice.invoice_id
                                UNION ALL
                                SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0) v_invoice_amt
                                  FROM ap_invoices ai,
                                       ap_invoice_lines ail,
                                       ap_invoice_distributions aid,
                                       ap_invoice_payments aip,
                                       ap_checks ac,
                                       xla_distribution_links xla
                                 WHERE     ai.invoice_id = ail.invoice_id
                                       AND ail.invoice_id = aid.invoice_id
                                       AND ail.line_number = aid.invoice_line_number
                                       AND ail.line_type_lookup_code IN ('TAX')
                                       AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                                       AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                                       AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                                ai.invoice_amount,
                                                                                ai.payment_status_flag,
                                                                                ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                                       AND (aip.invoice_payment_id,
                                            0,
                                            'AP_INVOICE_PAYMENTS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                                       AND ai.invoice_id = aip.invoice_id
                                       AND aip.check_id = ac.check_id
                                       AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                                       AND xla.rounding_class_code = 'LIABILITY'
                                       AND xla.source_distribution_type = 'AP_INV_DIST'
                                       AND (xla.event_id,
                                            0,
                                            'XLA_DISTRIBUTION_LINKS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                                       AND ac.check_id = rec_head.transaction_id
                                       AND ai.invoice_id = rec_invoice.invoice_id);
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           v_invoice_amt := 0;
                     END;
                  END IF;
               END IF;

               write_log ('20 end group 36: Calculate INVOICE AMOUNT');
               -- Calculate BASE AMOUNT
               write_log ('30 begin group 36: Calculate BASE AMOUNT');

               IF SIGN (v_payment_amt) = -1
               THEN
                  -- For Void Payment Cancel Invoice
                  IF INSTR (rec_invoice.tax_code, '36') <> 0
                  THEN
                     BEGIN
                        SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0)), 0)
                          INTO v_base_amt
                          FROM ap_invoices ai,
                               ap_invoice_lines ail,
                               ap_invoice_distributions aid,
                               (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                                    FROM ap_invoice_lines
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
                                                       FROM ap_invoice_lines
                                                      WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                               AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                               AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                               AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                        ai.invoice_amount,
                                                                        ai.payment_status_flag,
                                                                        ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                               AND (aip.invoice_payment_id,
                                    0,
                                    'AP_INVOICE_PAYMENTS_SERVICE',
                                    ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                0,
                                                                tidld.reference_type,
                                                                tidld.check_id
                                                           FROM thl_data_loaded tidld
                                                          WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                               AND ai.invoice_id = aip.invoice_id
                               AND aip.check_id = ac.check_id
                               AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
                               AND cbau.bank_account_id = cba.bank_account_id
                               AND cbb.branch_party_id = cba.bank_branch_id
                               AND ai.invoice_id = inv_tax.invoice_id
                               AND ail.tax_classification_code = inv_tax.tax_rate_code
                               AND zrb.tax_rate_id = inv_tax.tax_rate_id
                               AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                               AND xla.rounding_class_code = 'LIABILITY'
                               AND xla.source_distribution_type = 'AP_INV_DIST'
                               AND (xla.event_id,
                                    0,
                                    'XLA_DISTRIBUTION_LINKS_SERVICE',
                                    ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                0,
                                                                tidld.reference_type,
                                                                tidld.check_id
                                                           FROM thl_data_loaded tidld
                                                          WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                               AND ac.check_id = rec_head.transaction_id
                               AND ai.invoice_id = rec_invoice.invoice_id
                               AND ail.tax_classification_code = rec_head.tax_code
                               AND cba.bank_account_num = rec_head.bank_acct_number
                               AND NVL (ail.project_id, 0) = NVL (rec_invoice.project_id, 0)
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
                        SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0)
                          INTO v_base_amt
                          FROM ap_invoices ai,
                               ap_invoice_lines ail,
                               ap_invoice_distributions aid,
                               (  SELECT a.tax_rate_code tax, NVL (b.percentage_rate, 0) percentage_rate
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
                                    FROM ap_invoice_lines
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
                                                       FROM ap_invoice_lines
                                                      WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'TAX')
                               AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                               AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                               AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                        ai.invoice_amount,
                                                                        ai.payment_status_flag,
                                                                        ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                               AND (aip.invoice_payment_id,
                                    0,
                                    'AP_INVOICE_PAYMENTS_SERVICE',
                                    ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                0,
                                                                tidld.reference_type,
                                                                tidld.check_id
                                                           FROM thl_data_loaded tidld
                                                          WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                               AND ai.invoice_id = aip.invoice_id
                               AND aip.check_id = ac.check_id
                               AND ac.ce_bank_acct_use_id = cbau.bank_acct_use_id
                               AND cbau.bank_account_id = cba.bank_account_id
                               AND cbb.branch_party_id = cba.bank_branch_id
                               AND ai.invoice_id = inv_tax.invoice_id
                               AND ail.tax_classification_code = inv_tax.tax_rate_code
                               AND zrb.tax_rate_id = inv_tax.tax_rate_id
                               AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                               AND xla.rounding_class_code = 'LIABILITY'
                               AND xla.source_distribution_type = 'AP_INV_DIST'
                               AND (xla.event_id,
                                    0,
                                    'XLA_DISTRIBUTION_LINKS_SERVICE',
                                    ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                0,
                                                                tidld.reference_type,
                                                                tidld.check_id
                                                           FROM thl_data_loaded tidld
                                                          WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                               AND ac.check_id = rec_head.transaction_id
                               AND ai.invoice_id = rec_invoice.invoice_id
                               AND ail.tax_classification_code = rec_head.tax_code
                               AND cba.bank_account_num = rec_head.bank_acct_number
                               AND NVL (ail.project_id, 0) = NVL (rec_invoice.project_id, 0)
                               AND zrb.description = hla.location_code;
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           v_base_amt := 0;
                     END;
                  END IF;
               END IF;

               write_log ('30 end group 36: Calculate BASE AMOUNT');
               -- Calculate for Partial Payment
               write_log ('40 begin group 36: Calculate for Partial Payment');

               IF v_invoice_amt <> 0
               THEN
                  v_paid_percent := v_payment_amt / v_invoice_amt;
               ELSE
                  v_paid_percent := 0;
               END IF;

               v_paid_base_inv := ROUND (v_base_amt * v_paid_percent, 2);
               v_paid_rec_tax := ROUND (ROUND (ROUND (ROUND (v_paid_base_inv * rec_head.tax_rate, 2) / 100, 2) * rec_head.tax_recovery_rate, 2) / 100, 2);
               v_paid_nonrec_tax := ROUND (ROUND (ROUND (ROUND (v_paid_base_inv * rec_head.tax_rate, 2) / 100, 2) * (100 - rec_head.tax_recovery_rate), 2) / 100, 2);

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

               write_log ('40 end group 36: Calculate for Partial Payment');
               -- Find Period Name from GL DATE
               write_log ('45 begin group 36: Find Period Name from GL DATE');

                 SELECT period_name
                   INTO v_nperiod
                   FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
                  WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                        AND hou.set_of_books_id = gsob.set_of_books_id
                        AND gsob.period_set_name = gpv.period_set_name
                        AND hou.organization_id = org_id
                        AND (TRUNC (rec_head.transaction_date) BETWEEN gpv.start_date AND gpv.end_date)
                        AND gpv.adjustment_period_flag = 'N'
               ORDER BY start_date;

               write_log ('45 end group 36: Find Period Name from GL DATE');
               -- Insert Data to table : THL_TAXINVOICES_LINE
               write_log ('50 begin group 36: Insert Data to table : THL_TAXINVOICES_LINE');

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
                               NULL,                                                                                                                                    --v_nperiod,
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
                     write_log ('Error insert THL_TAXINVOICES_LINE ' || SQLERRM);
                     v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
               END;


               write_log ('50 end group 36: Insert Data to table : THL_TAXINVOICES_LINE');
               v_check_base_inv := v_check_base_inv + v_paid_base_inv;
               v_check_rec_tax := v_check_rec_tax + v_paid_rec_tax;
               v_check_nonrec_tax := v_check_nonrec_tax + v_paid_nonrec_tax;
            ELSIF v_group = 'N' OR INSTR (rec_invoice.tax_code, '36') <> 0                                                                                            ------  Case 3
            THEN
               OPEN cursor_invoice_line (rec_invoice.invoice_id);

               LOOP
                  FETCH cursor_invoice_line INTO rec_invoice_line;

                  EXIT WHEN cursor_invoice_line%NOTFOUND;

                  SELECT thl_taxinvl_s.NEXTVAL INTO v_line_id FROM DUAL;

                  -- Calculate PAYMENT AMOUNT
                  write_log ('10 begin no group : Calculate PAYMENT AMOUNT');

                  IF INSTR (rec_invoice.tax_code, '36') <> 0
                  THEN
                     BEGIN
                        SELECT NVL (SUM (NVL (xdl.unrounded_entered_dr, 0) - NVL (xdl.unrounded_entered_cr, 0)), 0)
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
                               AND applied_to_source_id_num_1 = rec_invoice.invoice_id
                               AND (xdl.event_id,
                                    0,
                                    'XLA_DISTRIBUTION_LINKS_SERVICE',
                                    aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                                 0,
                                                                 tidld.reference_type,
                                                                 tidld.check_id
                                                            FROM thl_data_loaded tidld
                                                           WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xdl.event_id)
                               AND (aip.invoice_payment_id,
                                    0,
                                    'AP_INVOICE_PAYMENTS_SERVICE',
                                    aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                                 0,
                                                                 tidld.reference_type,
                                                                 tidld.check_id
                                                            FROM thl_data_loaded tidld
                                                           WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id);
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           v_payment_amt := 0;
                     END;
                  END IF;

                  write_log ('10 end no group : Calculate PAYMENT AMOUNT');
                  -- Calculate INVOICE AMOUNT
                  write_log ('20 begin no group : Calculate INVOICE AMOUNT');

                  IF INSTR (rec_invoice.tax_code, '36') <> 0
                  THEN
                     BEGIN
                        SELECT SUM (v_invoice_amt)
                          INTO v_invoice_amt
                          FROM (SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0) v_invoice_amt
                                  FROM ap_invoices ai,
                                       ap_invoice_lines ail,
                                       ap_invoice_distributions aid,
                                       ap_invoice_payments aip,
                                       ap_checks ac,
                                       xla_distribution_links xla
                                 WHERE     ai.invoice_id = ail.invoice_id
                                       AND ail.invoice_id = aid.invoice_id
                                       AND ail.line_number = aid.invoice_line_number
                                       AND ail.line_type_lookup_code IN ('ITEM')
                                       AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                                       AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                                       AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                                ai.invoice_amount,
                                                                                ai.payment_status_flag,
                                                                                ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                                       AND (aip.invoice_payment_id,
                                            0,
                                            'AP_INVOICE_PAYMENTS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                                       AND ai.invoice_id = aip.invoice_id
                                       AND aip.check_id = ac.check_id
                                       AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                                       AND xla.rounding_class_code = 'LIABILITY'
                                       AND xla.source_distribution_type = 'AP_INV_DIST'
                                       AND (xla.event_id,
                                            0,
                                            'XLA_DISTRIBUTION_LINKS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                                       AND ac.check_id = rec_head.transaction_id
                                       AND ai.invoice_id = rec_invoice.invoice_id
                                UNION ALL
                                SELECT NVL (SUM (NVL (xla.unrounded_entered_cr, 0) - NVL (xla.unrounded_entered_dr, 0)), 0) v_invoice_amt
                                  FROM ap_invoices ai,
                                       ap_invoice_lines ail,
                                       ap_invoice_distributions aid,
                                       ap_invoice_payments aip,
                                       ap_checks ac,
                                       xla_distribution_links xla
                                 WHERE     ai.invoice_id = ail.invoice_id
                                       AND ail.invoice_id = aid.invoice_id
                                       AND ail.line_number = aid.invoice_line_number
                                       AND ail.line_type_lookup_code IN ('TAX')
                                       AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                                       AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                                       AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                                ai.invoice_amount,
                                                                                ai.payment_status_flag,
                                                                                ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                                       AND (aip.invoice_payment_id,
                                            0,
                                            'AP_INVOICE_PAYMENTS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id)
                                       AND ai.invoice_id = aip.invoice_id
                                       AND aip.check_id = ac.check_id
                                       AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                                       AND xla.rounding_class_code = 'LIABILITY'
                                       AND xla.source_distribution_type = 'AP_INV_DIST'
                                       AND (xla.event_id,
                                            0,
                                            'XLA_DISTRIBUTION_LINKS_SERVICE',
                                            ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                                        0,
                                                                        tidld.reference_type,
                                                                        tidld.check_id
                                                                   FROM thl_data_loaded tidld
                                                                  WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xla.event_id)
                                       AND ac.check_id = rec_head.transaction_id
                                       AND ai.invoice_id = rec_invoice.invoice_id);
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           v_invoice_amt := 0;
                     END;
                  END IF;

                  write_log ('20 end no group : Calculate INVOICE AMOUNT');
                  -- Calculate BASE AMOUNT
                  write_log ('30 begin no group : Calculate BASE AMOUNT');
                  v_base_amt := rec_invoice_line.base_amt;
                  write_log ('30 end no group : Calculate BASE AMOUNT');
                  -- Calculate for Partial Payment
                  write_log ('40 begin no group : Calculate for Partial Payment');

                  -------- Add Calculate tax amount  by Suwatchai  10-APR-12    V.2.0


                  IF v_invoice_amt <> 0
                  THEN
                     v_paid_percent := v_payment_amt / v_invoice_amt;
                  ELSE
                     v_paid_percent := 0;
                  END IF;



                  IF v_payment_amt <> v_invoice_amt                                                                                                               -- Partial payment
                  THEN
                     v_paid_base_inv := ROUND (v_base_amt * v_paid_percent, 2);
                     v_paid_rec_tax := ROUND (ROUND (ROUND (ROUND (v_paid_base_inv * rec_head.tax_rate, 2) / 100, 2) * rec_head.tax_recovery_rate, 2) / 100, 2);


                     v_paid_nonrec_tax := ROUND (ROUND (ROUND (ROUND (v_paid_base_inv * rec_head.tax_rate, 2) / 100, 2) * (100 - rec_head.tax_recovery_rate), 2) / 100, 2);
                  ELSE                                                                                                                                              -- Fully payment
                     v_paid_base_inv := v_base_amt;
                     v_paid_rec_tax := rec_invoice_line.tax_amt2; -- v_paid_rec_tax ;        -- Get tax_amount directly from ap_invoice_distributions  ..by Suwatchai 10-APR-12  V.2.0
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

                  write_log ('40 end no group : Calculate for Partial Payment');
                  -- Find Period Name from GL DATE
                  write_log ('45 begin no group : Find Period Name from GL DATE');

                    SELECT period_name
                      INTO v_nperiod
                      FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
                     WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                           AND hou.set_of_books_id = gsob.set_of_books_id
                           AND gsob.period_set_name = gpv.period_set_name
                           AND hou.organization_id = org_id
                           AND (TRUNC (rec_head.transaction_date) BETWEEN gpv.start_date AND gpv.end_date)
                           AND gpv.adjustment_period_flag = 'N'
                  ORDER BY start_date;

                  write_log ('45 end no group : Find Period Name from GL DATE');
                  -- Insert Data to table : THL_TAXINVOICES_LINE
                  write_log ('50 begin no group : Insert Data to table : THL_TAXINVOICES_LINE');

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
                                  NULL,                                                                                                                                 --v_nperiod,
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
                        write_log ('Error insert THL_TAXINVOICES_LINE ' || SQLERRM);
                        v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
                  END;

                  write_log ('50 end no group : Insert Data to table : THL_TAXINVOICES_LINE');
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
         write_log ('60 begin : Insert Data to table : THL_TAXINVOICES_HEADER');

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
               write_log ('Error insert THL_TAXINVOICES_HEADER ' || SQLERRM);
               v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
         END;

         write_log ('60 end : Insert Data to table : THL_TAXINVOICES_HEADER');
      END LOOP;

      CLOSE cursor_head;

      -- Insert Data to table : THL_DATA_LOADED
      -- For Check Data Loaded
      IF v_insert
      THEN
         write_log ('70 begin : Insert Data to table : THL_DATA_LOADED');

         OPEN cursor_head;

         LOOP
            FETCH cursor_head INTO rec_head;

            EXIT WHEN cursor_head%NOTFOUND;

            OPEN cursor_invoice (rec_head.transaction_id,
                                 rec_head.tax_code,
                                 rec_head.voucher_number,
                                 rec_head.location_id,
                                 rec_head.bank_acct_number,
                                 rec_head.supplier_name);

            LOOP
               FETCH cursor_invoice INTO rec_invoice;

               EXIT WHEN cursor_invoice%NOTFOUND;

               BEGIN
                  INSERT INTO thl_data_loaded (reference_id,
                                               reference_line_number,
                                               reference_type,
                                               invoice_id,
                                               check_id,
                                               created_by,
                                               creation_date,
                                               last_updated_by,
                                               last_update_date,
                                               last_update_login)
                     SELECT DISTINCT xdl.event_id,
                                     0,
                                     'XLA_DISTRIBUTION_LINKS_SERVICE',
                                     aip.invoice_id,
                                     aip.check_id,
                                     user_id,
                                     SYSDATE,
                                     user_id,
                                     SYSDATE,
                                     user_id
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
                            AND applied_to_source_id_num_1 = rec_invoice.invoice_id
                            AND (xdl.event_id,
                                 0,
                                 'XLA_DISTRIBUTION_LINKS_SERVICE',
                                 aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                              0,
                                                              tidld.reference_type,
                                                              tidld.check_id
                                                         FROM thl_data_loaded tidld
                                                        WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_SERVICE' AND tidld.reference_id = xdl.event_id)
                            AND (aip.invoice_payment_id,
                                 0,
                                 'AP_INVOICE_PAYMENTS_SERVICE',
                                 aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                              0,
                                                              tidld.reference_type,
                                                              tidld.check_id
                                                         FROM thl_data_loaded tidld
                                                        WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_SERVICE' AND tidld.reference_id = aip.invoice_payment_id);
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     write_log ('Error insert THL_DATA_LOADED ' || SQLERRM);
                     v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
               END;
            END LOOP;

            CLOSE cursor_invoice;
         END LOOP;

         CLOSE cursor_head;

         OPEN cursor_invoice_dis;

         LOOP
            FETCH cursor_invoice_dis INTO rec_invoice_dis;

            EXIT WHEN cursor_invoice_dis%NOTFOUND;

            BEGIN
               INSERT INTO thl_data_loaded (reference_id,
                                            reference_line_number,
                                            reference_type,
                                            invoice_id,
                                            check_id,
                                            created_by,
                                            creation_date,
                                            last_updated_by,
                                            last_update_date,
                                            last_update_login)
                    VALUES (rec_invoice_dis.invoice_distribution_id,
                            0,
                            'AP_INVOICE_DISTRIBUTIONS_SERVICE',
                            rec_invoice_dis.invoice_id,
                            rec_invoice_dis.check_id,
                            user_id,
                            SYSDATE,
                            user_id,
                            SYSDATE,
                            user_id);
            EXCEPTION
               WHEN OTHERS
               THEN
                  write_log ('Error insert THL_DATA_LOADED ' || SQLERRM);
                  v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
            END;

            BEGIN
               INSERT INTO thl_data_loaded (reference_id,
                                            reference_line_number,
                                            reference_type,
                                            invoice_id,
                                            check_id,
                                            created_by,
                                            creation_date,
                                            last_updated_by,
                                            last_update_date,
                                            last_update_login)
                    VALUES (rec_invoice_dis.invoice_payment_id,
                            0,
                            'AP_INVOICE_PAYMENTS_SERVICE',
                            rec_invoice_dis.invoice_id,
                            rec_invoice_dis.check_id,
                            user_id,
                            SYSDATE,
                            user_id,
                            SYSDATE,
                            user_id);
            EXCEPTION
               WHEN OTHERS
               THEN
                  write_log ('Error insert thl_data_loaded ' || SQLERRM);
                  v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
            END;
         END LOOP;

         CLOSE cursor_invoice_dis;

         write_log ('70 end : Insert Data to table : THL_DATA_LOADED');
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         raise_application_error (-20101, 'Saving Error!');
         ROLLBACK;
   END thl_load_taxinv_service;

   /*================================================================

     PROCEDURE NAME:    THL_LOAD_WHT_M()

   ==================================================================*/
   PROCEDURE thl_load_wht_m (p_conc_request_id IN NUMBER, p_invoice_id IN NUMBER)
   IS
      user_id                         NUMBER;
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
      v_paid_percent                  NUMBER;
      v_paid_base_inv                 NUMBER;
      v_paid_rec_tax                  NUMBER;
      v_paid_nonrec_tax               NUMBER;
      v_check_base_inv                NUMBER;
      v_check_rec_tax                 NUMBER;
      v_check_nonrec_tax              NUMBER;
      v_period                        VARCHAR2 (10);
      v_seq_name                      VARCHAR2 (50);
      v_query                         VARCHAR2 (800);
      cursor1                         INTEGER;
      v_wht_num                       NUMBER;
      rows_processed                  INTEGER;
      v_check_base                    NUMBER;
      v_line_base                     NUMBER;
      v_inv_base                      NUMBER;
      v_inv_tax                       NUMBER;
      v_line_tax                      NUMBER;
      v_check_tax                     NUMBER;
      v_pay_act                       NUMBER;
      v_base_amt_line                 NUMBER;
      v_tax_amt_line                  NUMBER;
      v_nperiod                       VARCHAR2 (10);
      v_ai_invoice_id                 NUMBER (15);
      v_ai_invoice_amount             NUMBER;
      v_ai_payment_status_flag        VARCHAR2 (1 BYTE);
      v_ai_invoice_type_lookup_code   VARCHAR2 (25 BYTE);
      v_invoice_date                  DATE;

      -- Cursor for Load Data to Table : thl_wht_header
      CURSOR cursor_manual_head
      IS
           SELECT ai.invoice_id,
                  ai.invoice_num doc_number,
                  TRUNC (ail.gl_date) doc_date,
                  'MANUAL' doc_type,
                  'Invoice' doc_relate,
                  hla.location_id location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value) voucher,
                  TRUNC (ail.gl_date) gl_date,
                  TO_CHAR (ail.gl_date, 'MON-YYYY') period_name,
                  atc.attribute12 pnd,
                  atc.NAME tax_code,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  TRUNC (ai.invoice_date) invoice_date
             FROM ap_invoices ai,
                  ap_invoice_lines_v ail,
                  ap_invoice_distributions aid,
                  ap_tax_codes atc,
                  ap_suppliers pv,
                  ap_supplier_sites pvs,
                  hr_locations hla,
                  xla_distribution_links xla,
                  (SELECT hps.party_id,
                          hps.party_site_id,
                          hzl.address1 AS address_line1,
                          hzl.address2 AS address_line2,
                          hzl.address3 AS address_line3,
                          hzl.city AS city,
                          hzl.state AS state,
                          hzl.province AS province,
                          hzl.county AS county,
                          hzl.postal_code AS zip,
                          phone.raw_phone_number AS phone,
                          'US' AS LANGUAGE,
                          fax.raw_phone_number AS fax,
                          hzl.address4 AS address_line4,
                          '' AS area_code,
                          hzl.country AS country
                     FROM hz_party_sites hps,
                          hz_locations hzl,
                          fnd_territories_vl fvl,
                          hz_contact_points email,
                          hz_contact_points phone,
                          hz_contact_points fax,
                          hz_party_site_uses pay,
                          hz_party_site_uses pur,
                          hz_party_site_uses rfq
                    WHERE     hps.status = 'A'
                          AND hzl.country = fvl.territory_code
                          AND email.owner_table_id(+) = hps.party_site_id
                          AND email.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND email.status(+) = 'A'
                          AND email.contact_point_type(+) = 'EMAIL'
                          AND email.primary_flag(+) = 'Y'
                          AND phone.owner_table_id(+) = hps.party_site_id
                          AND phone.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND phone.status(+) = 'A'
                          AND phone.contact_point_type(+) = 'PHONE'
                          AND phone.phone_line_type(+) = 'GEN'
                          AND phone.primary_flag(+) = 'Y'
                          AND fax.owner_table_id(+) = hps.party_site_id
                          AND fax.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND fax.status(+) = 'A'
                          AND fax.contact_point_type(+) = 'PHONE'
                          AND fax.phone_line_type(+) = 'FAX'
                          AND hps.location_id = hzl.location_id
                          AND pay.party_site_id(+) = hps.party_site_id
                          AND pur.party_site_id(+) = hps.party_site_id
                          AND rfq.party_site_id(+) = hps.party_site_id
                          AND pay.status(+) = 'A'
                          AND pur.status(+) = 'A'
                          AND rfq.status(+) = 'A'
                          AND NVL (pay.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pur.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (rfq.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pay.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (pur.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (rfq.begin_date(+), SYSDATE) <= SYSDATE
                          AND pay.site_use_type(+) = 'PAY'
                          AND pur.site_use_type(+) = 'PURCHASING'
                          AND rfq.site_use_type(+) = 'RFQ'
                          AND NOT EXISTS
                                     (SELECT 1
                                        FROM pos_address_requests par, pos_supplier_mappings psm
                                       WHERE     psm.party_id = hps.party_id
                                             AND psm.mapping_id = par.mapping_id
                                             AND party_site_id = hps.party_site_id
                                             AND request_status = 'PENDING'
                                             AND request_type IN ('UPDATE', 'DELETE'))) pva,
                  (SELECT pv.vendor_id,
                          DECODE (UPPER (pv.vendor_type_lookup_code),
                                  'EMPLOYEE', papf.national_identifier,
                                  DECODE (pv.organization_type_lookup_code,  'INDIVIDUAL', pv.individual_1099,  'FOREIGN INDIVIDUAL', pv.individual_1099,  hp.jgzz_fiscal_code))
                             hz_taxpayer_id
                     FROM po_vendors pv,
                          ap_awt_groups aag,
                          rcv_routing_headers rcpt,
                          fnd_currencies_tl fct,
                          fnd_currencies_tl pay,
                          fnd_lookup_values pay_group,
                          ap_terms_tl terms,
                          po_vendors PARENT,
                          per_employees_current_x emp,
                          hz_parties hp,
                          ap_income_tax_types aptt,
                          per_all_people_f papf
                    WHERE     pv.party_id = hp.party_id
                          AND pv.parent_vendor_id = PARENT.vendor_id(+)
                          AND pv.awt_group_id = aag.GROUP_ID(+)
                          AND pv.receiving_routing_id = rcpt.routing_header_id(+)
                          AND fct.LANGUAGE(+) = USERENV ('lang')
                          AND pay.LANGUAGE(+) = USERENV ('lang')
                          AND pv.invoice_currency_code = fct.currency_code(+)
                          AND pv.payment_currency_code = pay.currency_code(+)
                          AND pv.pay_group_lookup_code = pay_group.lookup_code(+)
                          AND pay_group.lookup_type(+) = 'PAY GROUP'
                          AND pay_group.LANGUAGE(+) = USERENV ('lang')
                          AND pv.terms_id = terms.term_id(+)
                          AND terms.LANGUAGE(+) = USERENV ('LANG')
                          AND terms.enabled_flag(+) = 'Y'
                          AND pv.employee_id = emp.employee_id(+)
                          AND pv.employee_id = papf.person_id(+)
                          AND pv.type_1099 = aptt.income_tax_type(+)) pv_tax
            WHERE     ai.invoice_id = ail.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.awt_group_name = atc.NAME
                  AND ail.line_type_lookup_code = 'AWT'
                  AND ail.line_source = 'MANUAL LINE ENTRY'
                  AND (aid.invoice_distribution_id,
                       0,
                       'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                       ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                     0,
                                                     tidl.reference_type,
                                                     tidl.invoice_id
                                                FROM thl_data_loaded tidl
                                               WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                  AND ai.vendor_id = pv.vendor_id
                  AND ai.vendor_site_id = pvs.vendor_site_id
                  AND pv.party_id = pva.party_id(+)
                  AND pvs.party_site_id = pva.party_site_id
                  AND pv.vendor_id = pv_tax.vendor_id(+)
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'AWT'
                  AND xla.applied_to_source_id_num_1 = ai.invoice_id
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_AWT_M',
                       ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.invoice_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                  AND ai.invoice_id = p_invoice_id
                  AND atc.attribute15 = hla.location_code
         GROUP BY ai.invoice_id,
                  ai.invoice_num,
                  TRUNC (ai.invoice_date),                                                                                                                                   -- V2.0
                  TO_CHAR (ail.gl_date, 'MON-YYYY'),
                  'MANUAL',
                  'Invoice',
                  hla.location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value),
                  TRUNC (ail.gl_date),
                  atc.attribute12,
                  atc.NAME,
                  ail.awt_group_id
         ORDER BY DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value);

      rec_manual_head                 cursor_manual_head%ROWTYPE;

      -- Cursor for Load Data to Table : thl_wht_line
      CURSOR cursor_manual_group (
         v_invoice_id    NUMBER,
         v_wht_code      VARCHAR2,
         v_doc_date      DATE)
      IS
           SELECT ai.invoice_id,
                  ai.invoice_num doc_number,
                  TRUNC (ail.gl_date) doc_date,
                  'MANUAL' doc_type,
                  'Invoice' doc_relate,
                  hla.location_id location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value) voucher,
                  TRUNC (ail.gl_date) gl_date,
                  TO_CHAR (ail.gl_date, 'MON-YYYY') period_name,
                  atc.attribute12 pnd,
                  atc.NAME wht_code,
                  NVL (atc.attribute14, pv.global_attribute17) revenue_type,
                  NVL (atc.attribute13, pv.global_attribute18) revenue_name,
                  NVL (ail.attribute2, pv.vendor_name) supplier_name,
                  pva.address_line1 supplier_address1,
                  pva.address_line2 supplier_address2,
                  pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip supplier_address3,
                  zptp.rep_registration_number supplier_tax_id,
                  pvs.global_attribute20 supplier_branch_no,
                  '' agent_name,
                  '' agent_address,
                  '' agent_tax_id,
                  '' agent_branch_no,
                  hla.attribute15 resp_name,
                  hla.address_line_1 resp_address1,
                  hla.address_line_2 resp_address2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code resp_address3,
                  hla.attribute1 resp_tax_id,
                  SUBSTR (hla.attribute14, 1, 4) resp_branch_no,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login
             FROM ap_invoice_lines_v ail,
                  ap_invoice_distributions aid,
                  ap_invoices ai,
                  ap_tax_codes atc,
                  ap_suppliers pv,
                  hr_locations hla,
                  ap_supplier_sites pvs,
                  xla_distribution_links xla,
                  zx_party_tax_profile zptp,
                  (SELECT hps.party_id,
                          hps.party_site_id,
                          hzl.address1 AS address_line1,
                          hzl.address2 AS address_line2,
                          hzl.address3 AS address_line3,
                          hzl.city AS city,
                          hzl.state AS state,
                          hzl.province AS province,
                          hzl.county AS county,
                          hzl.postal_code AS zip,
                          phone.raw_phone_number AS phone,
                          'US' AS LANGUAGE,
                          fax.raw_phone_number AS fax,
                          hzl.address4 AS address_line4,
                          '' AS area_code,
                          hzl.country AS country
                     FROM hz_party_sites hps,
                          hz_locations hzl,
                          fnd_territories_vl fvl,
                          hz_contact_points email,
                          hz_contact_points phone,
                          hz_contact_points fax,
                          hz_party_site_uses pay,
                          hz_party_site_uses pur,
                          hz_party_site_uses rfq
                    WHERE     hps.status = 'A'
                          AND hzl.country = fvl.territory_code
                          AND email.owner_table_id(+) = hps.party_site_id
                          AND email.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND email.status(+) = 'A'
                          AND email.contact_point_type(+) = 'EMAIL'
                          AND email.primary_flag(+) = 'Y'
                          AND phone.owner_table_id(+) = hps.party_site_id
                          AND phone.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND phone.status(+) = 'A'
                          AND phone.contact_point_type(+) = 'PHONE'
                          AND phone.phone_line_type(+) = 'GEN'
                          AND phone.primary_flag(+) = 'Y'
                          AND fax.owner_table_id(+) = hps.party_site_id
                          AND fax.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND fax.status(+) = 'A'
                          AND fax.contact_point_type(+) = 'PHONE'
                          AND fax.phone_line_type(+) = 'FAX'
                          AND hps.location_id = hzl.location_id
                          AND pay.party_site_id(+) = hps.party_site_id
                          AND pur.party_site_id(+) = hps.party_site_id
                          AND rfq.party_site_id(+) = hps.party_site_id
                          AND pay.status(+) = 'A'
                          AND pur.status(+) = 'A'
                          AND rfq.status(+) = 'A'
                          AND NVL (pay.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pur.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (rfq.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pay.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (pur.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (rfq.begin_date(+), SYSDATE) <= SYSDATE
                          AND pay.site_use_type(+) = 'PAY'
                          AND pur.site_use_type(+) = 'PURCHASING'
                          AND rfq.site_use_type(+) = 'RFQ'
                          AND NOT EXISTS
                                     (SELECT 1
                                        FROM pos_address_requests par, pos_supplier_mappings psm
                                       WHERE     psm.party_id = hps.party_id
                                             AND psm.mapping_id = par.mapping_id
                                             AND party_site_id = hps.party_site_id
                                             AND request_status = 'PENDING'
                                             AND request_type IN ('UPDATE', 'DELETE'))) pva,
                  (SELECT pv.vendor_id,
                          DECODE (UPPER (pv.vendor_type_lookup_code),
                                  'EMPLOYEE', papf.national_identifier,
                                  DECODE (pv.organization_type_lookup_code,  'INDIVIDUAL', pv.individual_1099,  'FOREIGN INDIVIDUAL', pv.individual_1099,  hp.jgzz_fiscal_code))
                             hz_taxpayer_id
                     FROM po_vendors pv,
                          ap_awt_groups aag,
                          rcv_routing_headers rcpt,
                          fnd_currencies_tl fct,
                          fnd_currencies_tl pay,
                          fnd_lookup_values pay_group,
                          ap_terms_tl terms,
                          po_vendors PARENT,
                          per_employees_current_x emp,
                          hz_parties hp,
                          ap_income_tax_types aptt,
                          per_all_people_f papf
                    WHERE     pv.party_id = hp.party_id
                          AND pv.parent_vendor_id = PARENT.vendor_id(+)
                          AND pv.awt_group_id = aag.GROUP_ID(+)
                          AND pv.receiving_routing_id = rcpt.routing_header_id(+)
                          AND fct.LANGUAGE(+) = USERENV ('lang')
                          AND pay.LANGUAGE(+) = USERENV ('lang')
                          AND pv.invoice_currency_code = fct.currency_code(+)
                          AND pv.payment_currency_code = pay.currency_code(+)
                          AND pv.pay_group_lookup_code = pay_group.lookup_code(+)
                          AND pay_group.lookup_type(+) = 'PAY GROUP'
                          AND pay_group.LANGUAGE(+) = USERENV ('lang')
                          AND pv.terms_id = terms.term_id(+)
                          AND terms.LANGUAGE(+) = USERENV ('LANG')
                          AND terms.enabled_flag(+) = 'Y'
                          AND pv.employee_id = emp.employee_id(+)
                          AND pv.employee_id = papf.person_id(+)
                          AND pv.type_1099 = aptt.income_tax_type(+)) pv_tax
            WHERE     ail.invoice_id = ai.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.line_type_lookup_code = 'AWT'
                  AND ail.line_source = 'MANUAL LINE ENTRY'
                  AND ail.awt_group_name = atc.NAME
                  AND ai.vendor_id = pv.vendor_id
                  AND pv.party_id = pva.party_id(+)
                  AND pvs.party_site_id = pva.party_site_id
                  AND pv.vendor_id = pv_tax.vendor_id(+)
                  AND ai.vendor_site_id = pvs.vendor_site_id
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND (aid.invoice_distribution_id,
                       0,
                       'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                       ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                     0,
                                                     tidl.reference_type,
                                                     tidl.invoice_id
                                                FROM thl_data_loaded tidl
                                               WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'AWT'
                  AND xla.applied_to_source_id_num_1 = ai.invoice_id
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_AWT_M',
                       ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.invoice_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                  AND ai.invoice_id = p_invoice_id
                  AND atc.attribute15 = hla.location_code
                  AND pvs.party_site_id = zptp.party_id
                  AND zptp.party_type_code = 'THIRD_PARTY_SITE'
                  AND ai.invoice_id = v_invoice_id
                  AND atc.NAME = v_wht_code
                  AND TRUNC (ail.gl_date) = TRUNC (v_doc_date)
         GROUP BY ai.invoice_id,
                  ai.invoice_num,
                  TRUNC (ail.gl_date),
                  'MANUAL',
                  'Invoice',
                  hla.location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value),
                  TO_CHAR (ail.gl_date, 'MON-YYYY'),
                  atc.attribute12,
                  atc.NAME,
                  NVL (atc.attribute14, pv.global_attribute17),
                  NVL (atc.attribute13, pv.global_attribute18),
                  NVL (ail.attribute2, pv.vendor_name),
                  pva.address_line1,
                  pva.address_line2,
                  pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip,
                  pv_tax.hz_taxpayer_id,
                  pvs.global_attribute20,
                  hla.attribute15,
                  hla.address_line_1,
                  hla.address_line_2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code,
                  hla.attribute1,
                  hla.attribute14,
                  zptp.rep_registration_number
         ORDER BY DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value);

      rec_manual_group                cursor_manual_group%ROWTYPE;

      CURSOR cursor_manual_nogroup (
         v_invoice_id    NUMBER,
         v_wht_code      VARCHAR2,
         v_doc_date      DATE)
      IS
           SELECT ai.invoice_id,
                  ai.invoice_num doc_number,
                  TRUNC (ail.gl_date) doc_date,
                  'MANUAL' doc_type,
                  'Invoice' doc_relate,
                  hla.location_id location_id,
                  DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value) voucher,
                  TRUNC (ail.gl_date) gl_date,
                  TO_CHAR (ail.gl_date, 'MON-YYYY') period_name,
                  atc.attribute12 pnd,
                  atc.NAME wht_code,
                  NVL (atc.attribute14, pv.global_attribute17) revenue_type,
                  NVL (atc.attribute13, pv.global_attribute18) revenue_name,
                  NVL (ail.attribute2, pv.vendor_name) supplier_name,
                  pva.address_line1 supplier_address1,
                  pva.address_line2 supplier_address2,
                  pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip supplier_address3,
                  zptp.rep_registration_number supplier_tax_id,
                  pvs.global_attribute20 supplier_branch_no,
                  '' agent_name,
                  '' agent_address,
                  '' agent_tax_id,
                  '' agent_branch_no,
                  hla.attribute15 resp_name,
                  hla.address_line_1 resp_address1,
                  hla.address_line_2 resp_address2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code resp_address3,
                  hla.attribute1 resp_tax_id,
                  SUBSTR (hla.attribute14, 1, 4) resp_branch_no,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  ROUND (ROUND (aid.amount, 2) * ROUND (NVL (ai.exchange_rate, 1), 2), 2) wht_amount
             FROM ap_invoice_lines_v ail,
                  ap_invoice_distributions aid,
                  ap_invoices ai,
                  ap_tax_codes atc,
                  ap_suppliers pv,
                  hr_locations hla,
                  ap_supplier_sites pvs,
                  xla_distribution_links xla,
                  zx_party_tax_profile zptp,
                  (SELECT hps.party_id,
                          hps.party_site_id,
                          hzl.address1 AS address_line1,
                          hzl.address2 AS address_line2,
                          hzl.address3 AS address_line3,
                          hzl.city AS city,
                          hzl.state AS state,
                          hzl.province AS province,
                          hzl.county AS county,
                          hzl.postal_code AS zip,
                          phone.raw_phone_number AS phone,
                          'US' AS LANGUAGE,
                          fax.raw_phone_number AS fax,
                          hzl.address4 AS address_line4,
                          '' AS area_code,
                          hzl.country AS country
                     FROM hz_party_sites hps,
                          hz_locations hzl,
                          fnd_territories_vl fvl,
                          hz_contact_points email,
                          hz_contact_points phone,
                          hz_contact_points fax,
                          hz_party_site_uses pay,
                          hz_party_site_uses pur,
                          hz_party_site_uses rfq
                    WHERE     hps.status = 'A'
                          AND hzl.country = fvl.territory_code
                          AND email.owner_table_id(+) = hps.party_site_id
                          AND email.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND email.status(+) = 'A'
                          AND email.contact_point_type(+) = 'EMAIL'
                          AND email.primary_flag(+) = 'Y'
                          AND phone.owner_table_id(+) = hps.party_site_id
                          AND phone.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND phone.status(+) = 'A'
                          AND phone.contact_point_type(+) = 'PHONE'
                          AND phone.phone_line_type(+) = 'GEN'
                          AND phone.primary_flag(+) = 'Y'
                          AND fax.owner_table_id(+) = hps.party_site_id
                          AND fax.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND fax.status(+) = 'A'
                          AND fax.contact_point_type(+) = 'PHONE'
                          AND fax.phone_line_type(+) = 'FAX'
                          AND hps.location_id = hzl.location_id
                          AND pay.party_site_id(+) = hps.party_site_id
                          AND pur.party_site_id(+) = hps.party_site_id
                          AND rfq.party_site_id(+) = hps.party_site_id
                          AND pay.status(+) = 'A'
                          AND pur.status(+) = 'A'
                          AND rfq.status(+) = 'A'
                          AND NVL (pay.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pur.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (rfq.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pay.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (pur.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (rfq.begin_date(+), SYSDATE) <= SYSDATE
                          AND pay.site_use_type(+) = 'PAY'
                          AND pur.site_use_type(+) = 'PURCHASING'
                          AND rfq.site_use_type(+) = 'RFQ'
                          AND NOT EXISTS
                                     (SELECT 1
                                        FROM pos_address_requests par, pos_supplier_mappings psm
                                       WHERE     psm.party_id = hps.party_id
                                             AND psm.mapping_id = par.mapping_id
                                             AND party_site_id = hps.party_site_id
                                             AND request_status = 'PENDING'
                                             AND request_type IN ('UPDATE', 'DELETE'))) pva,
                  (SELECT pv.vendor_id,
                          DECODE (UPPER (pv.vendor_type_lookup_code),
                                  'EMPLOYEE', papf.national_identifier,
                                  DECODE (pv.organization_type_lookup_code,  'INDIVIDUAL', pv.individual_1099,  'FOREIGN INDIVIDUAL', pv.individual_1099,  hp.jgzz_fiscal_code))
                             hz_taxpayer_id
                     FROM po_vendors pv,
                          ap_awt_groups aag,
                          rcv_routing_headers rcpt,
                          fnd_currencies_tl fct,
                          fnd_currencies_tl pay,
                          fnd_lookup_values pay_group,
                          ap_terms_tl terms,
                          po_vendors PARENT,
                          per_employees_current_x emp,
                          hz_parties hp,
                          ap_income_tax_types aptt,
                          per_all_people_f papf
                    WHERE     pv.party_id = hp.party_id
                          AND pv.parent_vendor_id = PARENT.vendor_id(+)
                          AND pv.awt_group_id = aag.GROUP_ID(+)
                          AND pv.receiving_routing_id = rcpt.routing_header_id(+)
                          AND fct.LANGUAGE(+) = USERENV ('lang')
                          AND pay.LANGUAGE(+) = USERENV ('lang')
                          AND pv.invoice_currency_code = fct.currency_code(+)
                          AND pv.payment_currency_code = pay.currency_code(+)
                          AND pv.pay_group_lookup_code = pay_group.lookup_code(+)
                          AND pay_group.lookup_type(+) = 'PAY GROUP'
                          AND pay_group.LANGUAGE(+) = USERENV ('lang')
                          AND pv.terms_id = terms.term_id(+)
                          AND terms.LANGUAGE(+) = USERENV ('LANG')
                          AND terms.enabled_flag(+) = 'Y'
                          AND pv.employee_id = emp.employee_id(+)
                          AND pv.employee_id = papf.person_id(+)
                          AND pv.type_1099 = aptt.income_tax_type(+)) pv_tax
            WHERE     ail.invoice_id = ai.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.line_type_lookup_code = 'AWT'
                  AND ail.line_source = 'MANUAL LINE ENTRY'
                  AND ail.awt_group_name = atc.NAME
                  AND ai.vendor_id = pv.vendor_id
                  AND pv.party_id = pva.party_id(+)
                  AND pvs.party_site_id = pva.party_site_id
                  AND pv.vendor_id = pv_tax.vendor_id(+)
                  AND ai.vendor_site_id = pvs.vendor_site_id
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND (aid.invoice_distribution_id,
                       0,
                       'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                       ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                     0,
                                                     tidl.reference_type,
                                                     tidl.invoice_id
                                                FROM thl_data_loaded tidl
                                               WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                  AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                  AND xla.rounding_class_code = 'AWT'
                  AND xla.applied_to_source_id_num_1 = ai.invoice_id
                  AND (xla.event_id,
                       0,
                       'XLA_DISTRIBUTION_LINKS_AWT_M',
                       ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.invoice_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                  AND ai.invoice_id = p_invoice_id
                  AND atc.attribute15 = hla.location_code
                  AND pvs.party_site_id = zptp.party_id
                  AND zptp.party_type_code = 'THIRD_PARTY_SITE'
                  AND ai.invoice_id = v_invoice_id
                  AND atc.NAME = v_wht_code
                  AND TRUNC (ail.gl_date) = TRUNC (v_doc_date)
         ORDER BY DECODE (ai.doc_sequence_id, NULL, ai.voucher_num, ai.doc_sequence_value);

      rec_manual_nogroup              cursor_manual_nogroup%ROWTYPE;

      -- Cursor for Load Data to Table : thl_data_loaded
      CURSOR cursor_manual_dis
      IS
         SELECT DISTINCT ai.invoice_id, aid.invoice_distribution_id, xla.event_id
           FROM ap_invoices ai,
                ap_invoice_lines_v ail,
                ap_invoice_distributions aid,
                ap_tax_codes atc,
                ap_suppliers pv,
                ap_supplier_sites pvs,
                hr_locations hla,
                xla_distribution_links xla
          WHERE     ai.invoice_id = ail.invoice_id
                AND ail.invoice_id = aid.invoice_id
                AND ail.line_number = aid.invoice_line_number
                AND ail.awt_group_name = atc.NAME
                AND ail.line_type_lookup_code = 'AWT'
                AND ail.line_source = 'MANUAL LINE ENTRY'
                AND (aid.invoice_distribution_id,
                     0,
                     'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                     ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                   0,
                                                   tidl.reference_type,
                                                   tidl.invoice_id
                                              FROM thl_data_loaded tidl
                                             WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                AND ai.vendor_id = pv.vendor_id
                AND ai.vendor_site_id = pvs.vendor_site_id
                AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                AND xla.rounding_class_code = 'AWT'
                AND xla.applied_to_source_id_num_1 = ai.invoice_id
                AND (xla.event_id,
                     0,
                     'XLA_DISTRIBUTION_LINKS_AWT_M',
                     ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.invoice_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                AND ai.invoice_id = p_invoice_id
                AND atc.attribute15 = hla.location_code
         UNION ALL
         SELECT DISTINCT ai.invoice_id, aid.invoice_distribution_id, xla.event_id
           FROM ap_invoices ai,
                ap_invoice_lines_v ail,
                ap_invoice_distributions aid,
                ap_tax_codes atc,
                ap_suppliers pv,
                ap_supplier_sites pvs,
                hr_locations hla,
                xla_distribution_links xla
          WHERE     ai.invoice_id = ail.invoice_id
                AND ail.invoice_id = aid.invoice_id
                AND ail.line_number = aid.invoice_line_number
                AND ail.awt_group_name = atc.NAME
                AND ail.line_type_lookup_code = 'ITEM'
                AND ail.line_source = 'MANUAL LINE ENTRY'
                AND (aid.invoice_distribution_id,
                     0,
                     'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                     ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                   0,
                                                   tidl.reference_type,
                                                   tidl.invoice_id
                                              FROM thl_data_loaded tidl
                                             WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                AND ai.vendor_id = pv.vendor_id
                AND ai.vendor_site_id = pvs.vendor_site_id
                AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                AND xla.rounding_class_code = 'LIABILITY'
                AND xla.source_distribution_type = 'AP_INV_DIST'
                AND xla.applied_to_source_id_num_1 = ai.invoice_id
                AND (xla.event_id,
                     0,
                     'XLA_DISTRIBUTION_LINKS_AWT_M',
                     ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.invoice_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                AND ai.invoice_id = p_invoice_id
                AND atc.attribute15 = hla.location_code;

      rec_manual_dis                  cursor_manual_dis%ROWTYPE;
      error_whtno                     EXCEPTION;
   BEGIN
      v_insert := FALSE;
      user_id := fnd_profile.VALUE ('USER_ID');
      org_id := fnd_profile.VALUE ('ORG_ID');

      OPEN cursor_manual_head;

      LOOP
         FETCH cursor_manual_head INTO rec_manual_head;

         EXIT WHEN cursor_manual_head%NOTFOUND;

         SELECT thl_whth_s.NEXTVAL INTO v_head_id FROM DUAL;

         write_log ('Invoice Number : ' || rec_manual_head.doc_number);
         -- Calculate BASE AMOUNT
         write_log ('10 begin group : Calculate BASE AMOUNT');

         BEGIN
            SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
              INTO v_base_amt
              FROM ap_invoice_lines_v ail,
                   ap_invoice_distributions aid,
                   ap_invoices ai,
                   ap_tax_codes atc,
                   ap_suppliers pv,
                   hr_locations hla,
                   ap_supplier_sites pvs,
                   xla_distribution_links xla
             WHERE     ail.invoice_id = ai.invoice_id
                   AND ail.invoice_id = aid.invoice_id
                   AND ail.line_number = aid.invoice_line_number
                   AND ail.line_type_lookup_code = 'ITEM'
                   AND ai.invoice_id IN (SELECT invoice_id
                                           FROM ap_invoice_lines
                                          WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'AWT')
                   AND ail.awt_group_name = atc.NAME
                   AND ai.vendor_id = pv.vendor_id
                   AND ai.vendor_site_id = pvs.vendor_site_id
                   AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                   AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                            ai.invoice_amount,
                                                            ai.payment_status_flag,
                                                            ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                   AND (aid.invoice_distribution_id,
                        0,
                        'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                        ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                      0,
                                                      tidl.reference_type,
                                                      tidl.invoice_id
                                                 FROM thl_data_loaded tidl
                                                WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                   AND ail.awt_group_name = rec_manual_head.tax_code
                   AND TRUNC (ail.gl_date) = TRUNC (rec_manual_head.doc_date)
                   AND ai.invoice_id = rec_manual_head.invoice_id
                   AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                   AND xla.rounding_class_code = 'LIABILITY'
                   AND xla.source_distribution_type = 'AP_INV_DIST'
                   AND xla.applied_to_source_id_num_1 = ai.invoice_id
                   AND (xla.event_id,
                        0,
                        'XLA_DISTRIBUTION_LINKS_AWT_M',
                        ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                      0,
                                                      tidld.reference_type,
                                                      tidld.invoice_id
                                                 FROM thl_data_loaded tidld
                                                WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                   AND atc.attribute15 = hla.location_code;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_base_amt := 0;
         END;

         write_log ('10 end group : Calculate BASE AMOUNT');
         -- Calculate TAX AMOUNT
         write_log ('20 begin group : Calculate TAX AMOUNT');

         BEGIN
            SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
              INTO v_tax_amt
              FROM ap_invoice_lines_v ail,
                   ap_invoice_distributions aid,
                   ap_invoices ai,
                   ap_tax_codes atc,
                   ap_suppliers pv,
                   hr_locations hla,
                   ap_supplier_sites pvs,
                   xla_distribution_links xla
             WHERE     ail.invoice_id = ai.invoice_id
                   AND ail.invoice_id = aid.invoice_id
                   AND ail.line_number = aid.invoice_line_number
                   AND ail.line_type_lookup_code = 'AWT'
                   AND ail.line_source = 'MANUAL LINE ENTRY'
                   AND ail.awt_group_name = atc.NAME
                   AND ai.vendor_id = pv.vendor_id
                   AND ai.vendor_site_id = pvs.vendor_site_id
                   AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                   AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                            ai.invoice_amount,
                                                            ai.payment_status_flag,
                                                            ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                   AND (aid.invoice_distribution_id,
                        0,
                        'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                        ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                      0,
                                                      tidl.reference_type,
                                                      tidl.invoice_id
                                                 FROM thl_data_loaded tidl
                                                WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                   AND atc.attribute12 = rec_manual_head.pnd
                   AND ail.awt_group_name = rec_manual_head.tax_code
                   AND TRUNC (ail.gl_date) = TRUNC (rec_manual_head.doc_date)
                   AND ai.invoice_id = rec_manual_head.invoice_id
                   AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                   AND xla.rounding_class_code = 'AWT'
                   AND xla.applied_to_source_id_num_1 = ai.invoice_id
                   AND (xla.event_id,
                        0,
                        'XLA_DISTRIBUTION_LINKS_AWT_M',
                        ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                      0,
                                                      tidld.reference_type,
                                                      tidld.invoice_id
                                                 FROM thl_data_loaded tidld
                                                WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                   AND atc.attribute15 = hla.location_code;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_tax_amt := 0;
         END;

         write_log ('20 end group : Calculate TAX AMOUNT');
         -- Find Period Name from GL DATE
         write_log ('30 begin group : Find Period Name from GL DATE');

           SELECT period_name
             INTO v_nperiod
             FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
            WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                  AND hou.set_of_books_id = gsob.set_of_books_id
                  AND gsob.period_set_name = gpv.period_set_name
                  AND hou.organization_id = org_id
                  AND (TRUNC (rec_manual_head.gl_date) BETWEEN gpv.start_date AND gpv.end_date)
                  AND gpv.adjustment_period_flag = 'N'
         ORDER BY start_date;

         write_log ('30 end group : Find Period Name from GL DATE');
         -- Create WHT Number
         write_log ('40 begin group : Create WHT Number');
         v_period := SUBSTR (v_nperiod, 1, 3) || SUBSTR (v_nperiod, 5, 2);
         v_query :=                                                                                                                                 -- 'select th_wht_'      -- V1.0
                   'select thl_wht_a_'                                                                                                                                       -- V2.0
                                      || v_period || '_' || rec_manual_head.pnd || '_s.nextval from dual';

         BEGIN
            cursor1 := DBMS_SQL.open_cursor;
            DBMS_SQL.parse (cursor1, v_query, 2);
            DBMS_SQL.define_column (cursor1, 1, v_wht_num);
            rows_processed := DBMS_SQL.EXECUTE (cursor1);

            IF DBMS_SQL.fetch_rows (cursor1) > 0
            THEN
               DBMS_SQL.COLUMN_VALUE (cursor1, 1, v_wht_num);
            ELSE
               EXIT;
            END IF;

            DBMS_SQL.close_cursor (cursor1);
         EXCEPTION
            WHEN OTHERS
            THEN
               write_log ('40 : Can not create WHT Number , ' || v_nperiod || ' , ' || rec_manual_head.pnd);
               ROLLBACK;
               RAISE error_whtno;

               IF DBMS_SQL.is_open (cursor1)
               THEN
                  DBMS_SQL.close_cursor (cursor1);
               END IF;

               EXIT;
         END;

         write_log ('40 end group : Create WHT Number');
         v_insert := TRUE;
         -- Insert Data to table : THL_WHT_HEADER
         write_log ('50 begin group : Insert Data to table : THL_WHT_HEADER');
         v_tax_amt := v_tax_amt * (-1);

         INSERT INTO thl_wht_header (th_whth_id,
                                     th_whth_doc_id,
                                     th_whth_doc_number,
                                     th_whth_doc_date,
                                     th_whth_type,
                                     th_whth_related_document,
                                     th_whth_location_id,
                                     th_whth_base_amt,
                                     th_whth_tax_amt,
                                     th_whth_voucher_number,
                                     th_whth_period_name,
                                     th_whth_wht_number,                                                                                                                     -- V2.0
                                     th_phor_ngor_dor,
                                     attribute1,
                                     created_by,
                                     creation_date,
                                     last_updated_by,
                                     last_update_date,
                                     last_update_login,
                                     th_act_transaction_date                                                                                                                 -- V2.0
                                                            )
              VALUES (v_head_id,
                      rec_manual_head.invoice_id,
                      rec_manual_head.doc_number,
                      rec_manual_head.doc_date,
                      rec_manual_head.doc_type,
                      rec_manual_head.doc_relate,
                      rec_manual_head.location_id,
                      v_base_amt,
                      v_tax_amt,
                      rec_manual_head.voucher,
                      v_nperiod,
                      v_wht_num,                                                                                                                                             -- V2.0
                      rec_manual_head.pnd,
                      p_conc_request_id,
                      rec_manual_head.created_by,
                      rec_manual_head.creation_date,
                      rec_manual_head.last_updated_by,
                      rec_manual_head.last_update_date,
                      rec_manual_head.last_update_login,
                      rec_manual_head.invoice_date                                                                                                                           -- V2.0
                                                  );

         write_log ('50 end group : Insert Data to table : THL_WHT_HEADER');

         OPEN cursor_manual_group (rec_manual_head.invoice_id, rec_manual_head.tax_code, rec_manual_head.doc_date);

         LOOP
            FETCH cursor_manual_group INTO rec_manual_group;

            EXIT WHEN cursor_manual_group%NOTFOUND;

            SELECT thl_whtl_s.NEXTVAL INTO v_line_id FROM DUAL;

            -- Calculate BASE AMOUNT LINE
            write_log ('60 begin group : Calculate BASE AMOUNT LINE');

            BEGIN
               SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                 INTO v_base_amt_line
                 FROM ap_invoice_lines_v ail,
                      ap_invoice_distributions aid,
                      ap_invoices ai,
                      ap_tax_codes atc,
                      ap_suppliers pv,
                      hr_locations hla,
                      ap_supplier_sites pvs,
                      xla_distribution_links xla
                WHERE     ail.invoice_id = ai.invoice_id
                      AND ail.invoice_id = aid.invoice_id
                      AND ail.line_number = aid.invoice_line_number
                      AND ail.line_type_lookup_code = 'ITEM'
                      AND ai.invoice_id IN (SELECT invoice_id
                                              FROM ap_invoice_lines
                                             WHERE invoice_id = ai.invoice_id AND line_type_lookup_code = 'AWT')
                      AND ail.awt_group_name = atc.NAME
                      AND ai.vendor_id = pv.vendor_id
                      AND ai.vendor_site_id = pvs.vendor_site_id
                      AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                      AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                               ai.invoice_amount,
                                                               ai.payment_status_flag,
                                                               ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                      AND (aid.invoice_distribution_id,
                           0,
                           'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                           ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                         0,
                                                         tidl.reference_type,
                                                         tidl.invoice_id
                                                    FROM thl_data_loaded tidl
                                                   WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                      AND ail.awt_group_name = rec_manual_group.wht_code
                      AND TRUNC (ail.gl_date) = TRUNC (rec_manual_group.doc_date)
                      AND ai.invoice_id = rec_manual_group.invoice_id
                      AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                      AND xla.rounding_class_code = 'LIABILITY'
                      AND xla.source_distribution_type = 'AP_INV_DIST'
                      AND xla.applied_to_source_id_num_1 = ai.invoice_id
                      AND (xla.event_id,
                           0,
                           'XLA_DISTRIBUTION_LINKS_AWT_M',
                           ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                         0,
                                                         tidld.reference_type,
                                                         tidld.invoice_id
                                                    FROM thl_data_loaded tidld
                                                   WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                      AND atc.attribute15 = hla.location_code;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_base_amt_line := 0;
            END;

            write_log ('60 end group : Calculate BASE AMOUNT LINE');
            -- Calculate TAX AMOUNT LINE
            write_log ('70 begin group : Calculate TAX AMOUNT LINE');

            BEGIN
               SELECT NVL (SUM (NVL (xla.unrounded_accounted_cr, 0) - NVL (xla.unrounded_accounted_dr, 0)), 0)
                 INTO v_tax_amt_line
                 FROM ap_invoice_lines_v ail,
                      ap_invoice_distributions aid,
                      ap_invoices ai,
                      ap_tax_codes atc,
                      ap_suppliers pv,
                      hr_locations hla,
                      ap_supplier_sites pvs,
                      xla_distribution_links xla
                WHERE     ail.invoice_id = ai.invoice_id
                      AND ail.invoice_id = aid.invoice_id
                      AND ail.line_number = aid.invoice_line_number
                      AND ail.line_type_lookup_code = 'AWT'
                      AND ail.line_source = 'MANUAL LINE ENTRY'
                      AND ail.awt_group_name = atc.NAME
                      AND ai.vendor_id = pv.vendor_id
                      AND ai.vendor_site_id = pvs.vendor_site_id
                      AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                      AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                               ai.invoice_amount,
                                                               ai.payment_status_flag,
                                                               ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                      AND (aid.invoice_distribution_id,
                           0,
                           'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                           ai.invoice_id) NOT IN (SELECT tidl.reference_id,
                                                         0,
                                                         tidl.reference_type,
                                                         tidl.invoice_id
                                                    FROM thl_data_loaded tidl
                                                   WHERE tidl.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_M' AND tidl.reference_id = aid.invoice_distribution_id)
                      AND ail.awt_group_name = rec_manual_group.wht_code
                      AND TRUNC (ail.gl_date) = TRUNC (rec_manual_group.doc_date)
                      AND ai.invoice_id = rec_manual_group.invoice_id
                      AND xla.source_distribution_id_num_1 = aid.invoice_distribution_id
                      AND xla.rounding_class_code = 'AWT'
                      AND xla.applied_to_source_id_num_1 = ai.invoice_id
                      AND (xla.event_id,
                           0,
                           'XLA_DISTRIBUTION_LINKS_AWT_M',
                           ai.invoice_id) NOT IN (SELECT tidld.reference_id,
                                                         0,
                                                         tidld.reference_type,
                                                         tidld.invoice_id
                                                    FROM thl_data_loaded tidld
                                                   WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_M' AND tidld.reference_id = xla.event_id)
                      AND atc.attribute15 = hla.location_code;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  v_tax_amt_line := 0;
            END;

            write_log ('70 end group : Calculate TAX AMOUNT LINE');
            -- Insert Data to table : THL_WHT_LINE
            write_log ('80 begin group : Insert Data to table : THL_WHT_LINE');
            v_tax_amt_line := v_tax_amt_line * (-1);

            INSERT INTO thl_wht_line (th_whtl_id,
                                      th_whth_id,
                                      th_whtl_wt_code,
                                      th_whtl_revenue_type,
                                      th_whtl_revenue_name,
                                      th_whtl_base_amt,
                                      th_whtl_tax_amt,
                                      th_whtl_supplier_name,
                                      th_whtl_supplier_address1,
                                      th_whtl_supplier_address2,
                                      th_whtl_supplier_address3,
                                      th_whtl_supplier_tax_id,
                                      th_whtl_supplier_branch_no,
                                      th_whtl_agent_name,
                                      th_whtl_agent_address1,
                                      th_whtl_agent_address2,
                                      th_whtl_agent_address3,
                                      th_whtl_agent_tax_id,
                                      th_whtl_agent_branch_no,
                                      th_whtl_resp_name,
                                      th_whtl_resp_address1,
                                      th_whtl_resp_address2,
                                      th_whtl_resp_address3,
                                      th_whtl_resp_tax_id,
                                      th_whtl_resp_branch_no,
                                      attribute1,
                                      created_by,
                                      creation_date,
                                      last_updated_by,
                                      last_update_date,
                                      last_update_login)
                 VALUES (v_line_id,
                         v_head_id,
                         rec_manual_group.wht_code,
                         rec_manual_group.revenue_type,
                         rec_manual_group.revenue_name,
                         v_base_amt_line,
                         v_tax_amt_line,
                         rec_manual_group.supplier_name,
                         rec_manual_group.supplier_address1,
                         rec_manual_group.supplier_address2,
                         rec_manual_group.supplier_address3,
                         rec_manual_group.supplier_tax_id,
                         rec_manual_group.supplier_branch_no,
                         rec_manual_group.agent_name,
                         rec_manual_group.agent_address,
                         NULL,
                         NULL,
                         rec_manual_group.agent_tax_id,
                         rec_manual_group.agent_branch_no,
                         rec_manual_group.resp_name,
                         rec_manual_group.resp_address1,
                         rec_manual_group.resp_address2,
                         rec_manual_group.resp_address3,
                         rec_manual_group.resp_tax_id,
                         rec_manual_group.resp_branch_no,
                         p_conc_request_id,
                         rec_manual_group.created_by,
                         rec_manual_group.creation_date,
                         rec_manual_group.last_updated_by,
                         rec_manual_group.last_update_date,
                         rec_manual_group.last_update_login);

            write_log ('80 end group : Insert Data to table : THL_WHT_LINE');
         END LOOP;

         CLOSE cursor_manual_group;
      END LOOP;

      CLOSE cursor_manual_head;

      -- Insert Data to table : THL_DATA_LOADED
      -- For Check Data Loaded
      IF v_insert
      THEN
         write_log ('90 begin group : Insert Data to table : THL_DATA_LOADED');

         OPEN cursor_manual_dis;

         LOOP
            FETCH cursor_manual_dis INTO rec_manual_dis;

            EXIT WHEN cursor_manual_dis%NOTFOUND;

            INSERT INTO thl_data_loaded (reference_id,
                                         reference_line_number,
                                         reference_type,
                                         invoice_id,
                                         created_by,
                                         creation_date,
                                         last_updated_by,
                                         last_update_date,
                                         last_update_login)
                 VALUES (rec_manual_dis.event_id,
                         0,
                         'XLA_DISTRIBUTION_LINKS_AWT_M',
                         rec_manual_dis.invoice_id,
                         user_id,
                         SYSDATE,
                         user_id,
                         SYSDATE,
                         user_id);

            INSERT INTO thl_data_loaded (reference_id,
                                         reference_line_number,
                                         reference_type,
                                         invoice_id,
                                         created_by,
                                         creation_date,
                                         last_updated_by,
                                         last_update_date,
                                         last_update_login)
                 VALUES (rec_manual_dis.invoice_distribution_id,
                         0,
                         'AP_INVOICE_DISTRIBUTIONS_AWT_M',
                         rec_manual_dis.invoice_id,
                         user_id,
                         SYSDATE,
                         user_id,
                         SYSDATE,
                         user_id);
         END LOOP;

         CLOSE cursor_manual_dis;

         write_log ('90 end group : Insert Data to table : THL_DATA_LOADED');
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
   END thl_load_wht_m;

   /*================================================================

     PROCEDURE NAME:    thk_load_wht_a()

   ==================================================================*/
   PROCEDURE thl_load_wht_a (p_conc_request_id IN NUMBER, p_check_id IN NUMBER)
   IS
      user_id                         NUMBER;
      org_id                          NUMBER;
      appl_id                         NUMBER;
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
      v_paid_percent                  NUMBER;
      v_paid_base_inv                 NUMBER;
      v_paid_rec_tax                  NUMBER;
      v_paid_nonrec_tax               NUMBER;
      v_check_base_inv                NUMBER;
      v_check_rec_tax                 NUMBER;
      v_check_nonrec_tax              NUMBER;
      v_period                        VARCHAR2 (10);
      v_seq_name                      VARCHAR2 (50);
      v_query                         VARCHAR2 (800);
      cursor1                         INTEGER;
      v_wht_num                       NUMBER;
      rows_processed                  INTEGER;
      v_check_base                    NUMBER;
      v_line_base                     NUMBER;
      v_inv_base                      NUMBER;
      v_inv_tax                       NUMBER;
      v_line_tax                      NUMBER;
      v_check_tax                     NUMBER;
      v_pay_act                       NUMBER;
      v_base_amt_line                 NUMBER;
      v_tax_amt_line                  NUMBER;
      v_nperiod                       VARCHAR2 (10);
      v_ai_invoice_id                 NUMBER (15);
      v_ai_invoice_amount             NUMBER;
      v_ai_payment_status_flag        VARCHAR2 (1 BYTE);
      v_ai_invoice_type_lookup_code   VARCHAR2 (25 BYTE);

      -- Cursor for Load Data to Table : thl_wht_header
      CURSOR cursor_head
      IS
           SELECT ac.check_id,
                  ac.check_number doc_number,
                  ac.check_date doc_date,
                  'PAY' doc_type,
                  'Bank Account' doc_relate,
                  hla.location_id location_id,
                  ac.doc_sequence_value voucher_number,
                  TO_CHAR (ac.check_date, 'MON-YYYY') period_name,
                  atc.attribute12 pnd,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login,
                  TRUNC (aip.accounting_date) accounting_date                                                                                                                -- V2.0
             FROM ap_checks ac,
                  ap_invoice_payments aip,
                  ap_invoices ai,
                  ap_invoice_lines_v ail,
                  ap_invoice_distributions aid,
                  ap_tax_codes atc,
                  hr_locations hla,
                  ap_suppliers pv,
                  ap_supplier_sites pvs,
                  (SELECT hps.party_id,
                          hps.party_site_id,
                          hzl.address1 AS address_line1,
                          hzl.address2 AS address_line2,
                          hzl.address3 AS address_line3,
                          hzl.city AS city,
                          hzl.state AS state,
                          hzl.province AS province,
                          hzl.county AS county,
                          hzl.postal_code AS zip,
                          phone.raw_phone_number AS phone,
                          'US' AS LANGUAGE,
                          fax.raw_phone_number AS fax,
                          hzl.address4 AS address_line4,
                          '' AS area_code,
                          hzl.country AS country
                     FROM hz_party_sites hps,
                          hz_locations hzl,
                          fnd_territories_vl fvl,
                          hz_contact_points email,
                          hz_contact_points phone,
                          hz_contact_points fax,
                          hz_party_site_uses pay,
                          hz_party_site_uses pur,
                          hz_party_site_uses rfq
                    WHERE     hps.status = 'A'
                          AND hzl.country = fvl.territory_code
                          AND email.owner_table_id(+) = hps.party_site_id
                          AND email.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND email.status(+) = 'A'
                          AND email.contact_point_type(+) = 'EMAIL'
                          AND email.primary_flag(+) = 'Y'
                          AND phone.owner_table_id(+) = hps.party_site_id
                          AND phone.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND phone.status(+) = 'A'
                          AND phone.contact_point_type(+) = 'PHONE'
                          AND phone.phone_line_type(+) = 'GEN'
                          AND phone.primary_flag(+) = 'Y'
                          AND fax.owner_table_id(+) = hps.party_site_id
                          AND fax.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND fax.status(+) = 'A'
                          AND fax.contact_point_type(+) = 'PHONE'
                          AND fax.phone_line_type(+) = 'FAX'
                          AND hps.location_id = hzl.location_id
                          AND pay.party_site_id(+) = hps.party_site_id
                          AND pur.party_site_id(+) = hps.party_site_id
                          AND rfq.party_site_id(+) = hps.party_site_id
                          AND pay.status(+) = 'A'
                          AND pur.status(+) = 'A'
                          AND rfq.status(+) = 'A'
                          AND NVL (pay.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pur.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (rfq.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pay.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (pur.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (rfq.begin_date(+), SYSDATE) <= SYSDATE
                          AND pay.site_use_type(+) = 'PAY'
                          AND pur.site_use_type(+) = 'PURCHASING'
                          AND rfq.site_use_type(+) = 'RFQ'
                          AND NOT EXISTS
                                     (SELECT 1
                                        FROM pos_address_requests par, pos_supplier_mappings psm
                                       WHERE     psm.party_id = hps.party_id
                                             AND psm.mapping_id = par.mapping_id
                                             AND party_site_id = hps.party_site_id
                                             AND request_status = 'PENDING'
                                             AND request_type IN ('UPDATE', 'DELETE'))) pva,
                  (SELECT pv.vendor_id,
                          DECODE (UPPER (pv.vendor_type_lookup_code),
                                  'EMPLOYEE', papf.national_identifier,
                                  DECODE (pv.organization_type_lookup_code,  'INDIVIDUAL', pv.individual_1099,  'FOREIGN INDIVIDUAL', pv.individual_1099,  hp.jgzz_fiscal_code))
                             hz_taxpayer_id
                     FROM po_vendors pv,
                          ap_awt_groups aag,
                          rcv_routing_headers rcpt,
                          fnd_currencies_tl fct,
                          fnd_currencies_tl pay,
                          fnd_lookup_values pay_group,
                          ap_terms_tl terms,
                          po_vendors PARENT,
                          per_employees_current_x emp,
                          hz_parties hp,
                          ap_income_tax_types aptt,
                          per_all_people_f papf
                    WHERE     pv.party_id = hp.party_id
                          AND pv.parent_vendor_id = PARENT.vendor_id(+)
                          AND pv.awt_group_id = aag.GROUP_ID(+)
                          AND pv.receiving_routing_id = rcpt.routing_header_id(+)
                          AND fct.LANGUAGE(+) = USERENV ('lang')
                          AND pay.LANGUAGE(+) = USERENV ('lang')
                          AND pv.invoice_currency_code = fct.currency_code(+)
                          AND pv.payment_currency_code = pay.currency_code(+)
                          AND pv.pay_group_lookup_code = pay_group.lookup_code(+)
                          AND pay_group.lookup_type(+) = 'PAY GROUP'
                          AND pay_group.LANGUAGE(+) = USERENV ('lang')
                          AND pv.terms_id = terms.term_id(+)
                          AND terms.LANGUAGE(+) = USERENV ('LANG')
                          AND terms.enabled_flag(+) = 'Y'
                          AND pv.employee_id = emp.employee_id(+)
                          AND pv.employee_id = papf.person_id(+)
                          AND pv.type_1099 = aptt.income_tax_type(+)) pv_tax
            WHERE     ac.check_id = aip.check_id
                  AND aip.invoice_id = ai.invoice_id
                  AND ai.invoice_id = ail.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.line_type_lookup_code = 'AWT'
                  AND ail.line_source = 'AUTO WITHHOLDING'
                  AND ail.awt_group_name = atc.NAME
                  AND ac.vendor_id = pv.vendor_id
                  AND ac.vendor_site_id = pvs.vendor_site_id
                  AND pv.party_id = pva.party_id(+)
                  AND pvs.party_site_id = pva.party_site_id
                  AND pv.vendor_id = pv_tax.vendor_id(+)
                  AND (aip.invoice_payment_id,
                       0,
                       'AP_INVOICE_PAYMENTS_AWT_A',
                       ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.check_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id)
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND ac.check_id = p_check_id
                  AND atc.attribute15 = hla.location_code
         GROUP BY ac.doc_sequence_value,
                  ac.check_id,
                  ac.check_number,
                  ac.check_date,
                  'PAY',
                  'Bank Account',
                  hla.location_id,
                  TO_CHAR (ac.check_date, 'MON-YYYY'),
                  atc.attribute12,
                  TRUNC (aip.accounting_date)                                                                                                                                -- V2.0
         ORDER BY ac.doc_sequence_value;

      rec_head                        cursor_head%ROWTYPE;

      -- Cursor for Load Data to Table : thl_wht_line
      CURSOR cursor_line (
         v_check_id          NUMBER,
         v_voucher_number    NUMBER,
         v_pnd               VARCHAR2,
         v_location_id       NUMBER)
      IS
           SELECT ac.check_id,
                  ac.check_number doc_number,
                  ac.check_date doc_date,
                  'PAY' doc_type,
                  'Bank Account' doc_relate,
                  hla.location_id location_id,
                  ac.doc_sequence_value voucher_number,
                  TO_CHAR (ac.check_date, 'MON-YYYY') period_name,
                  atc.attribute12 pnd,
                  atc.NAME wht_code,
                  NVL (atc.attribute14, pv.global_attribute17) revenue_type,
                  NVL (atc.attribute13, pv.global_attribute18) revenue_name,
                  NVL (pv.vendor_name, ac.vendor_name) supplier_name,
                  DECODE (pva.address_line1, NULL, ac.address_line1, pva.address_line1) supplier_address1,
                  DECODE (pva.address_line1, NULL, ac.address_line2, pva.address_line2) supplier_address2,
                  DECODE (pva.address_line1,
                          NULL, ac.address_line3,
                          pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip)
                     supplier_address3,
                  zptp.rep_registration_number supplier_tax_id,
                  pvs.global_attribute20 supplier_branch_no,
                  '' agent_name,
                  '' agent_address,
                  '' agent_tax_id,
                  '' agent_branch_no,
                  hla.attribute15 resp_name,
                  hla.address_line_1 resp_address1,
                  hla.address_line_2 resp_address2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code resp_address3,
                  hla.attribute1 resp_tax_id,
                  SUBSTR (hla.attribute14, 1, 4) resp_branch_no,
                  awt_rate.tax_rate,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login
             FROM ap_checks ac,
                  ap_invoice_payments aip,
                  ap_invoices ai,
                  ap_invoice_lines_v ail,
                  ap_invoice_distributions aid,
                  ap_tax_codes atc,
                  hr_locations hla,
                  ap_suppliers pv,
                  ap_supplier_sites pvs,
                  ap_awt_tax_rates awt_rate,
                  zx_party_tax_profile zptp,
                  (SELECT hps.party_id,
                          hps.party_site_id,
                          hzl.address1 AS address_line1,
                          hzl.address2 AS address_line2,
                          hzl.address3 AS address_line3,
                          hzl.city AS city,
                          hzl.state AS state,
                          hzl.province AS province,
                          hzl.county AS county,
                          hzl.postal_code AS zip,
                          phone.raw_phone_number AS phone,
                          'US' AS LANGUAGE,
                          fax.raw_phone_number AS fax,
                          hzl.address4 AS address_line4,
                          '' AS area_code,
                          hzl.country AS country
                     FROM hz_party_sites hps,
                          hz_locations hzl,
                          fnd_territories_vl fvl,
                          hz_contact_points email,
                          hz_contact_points phone,
                          hz_contact_points fax,
                          hz_party_site_uses pay,
                          hz_party_site_uses pur,
                          hz_party_site_uses rfq
                    WHERE     hps.status = 'A'
                          AND hzl.country = fvl.territory_code
                          AND email.owner_table_id(+) = hps.party_site_id
                          AND email.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND email.status(+) = 'A'
                          AND email.contact_point_type(+) = 'EMAIL'
                          AND email.primary_flag(+) = 'Y'
                          AND phone.owner_table_id(+) = hps.party_site_id
                          AND phone.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND phone.status(+) = 'A'
                          AND phone.contact_point_type(+) = 'PHONE'
                          AND phone.phone_line_type(+) = 'GEN'
                          AND phone.primary_flag(+) = 'Y'
                          AND fax.owner_table_id(+) = hps.party_site_id
                          AND fax.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND fax.status(+) = 'A'
                          AND fax.contact_point_type(+) = 'PHONE'
                          AND fax.phone_line_type(+) = 'FAX'
                          AND hps.location_id = hzl.location_id
                          AND pay.party_site_id(+) = hps.party_site_id
                          AND pur.party_site_id(+) = hps.party_site_id
                          AND rfq.party_site_id(+) = hps.party_site_id
                          AND pay.status(+) = 'A'
                          AND pur.status(+) = 'A'
                          AND rfq.status(+) = 'A'
                          AND NVL (pay.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pur.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (rfq.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pay.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (pur.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (rfq.begin_date(+), SYSDATE) <= SYSDATE
                          AND pay.site_use_type(+) = 'PAY'
                          AND pur.site_use_type(+) = 'PURCHASING'
                          AND rfq.site_use_type(+) = 'RFQ'
                          AND NOT EXISTS
                                     (SELECT 1
                                        FROM pos_address_requests par, pos_supplier_mappings psm
                                       WHERE     psm.party_id = hps.party_id
                                             AND psm.mapping_id = par.mapping_id
                                             AND party_site_id = hps.party_site_id
                                             AND request_status = 'PENDING'
                                             AND request_type IN ('UPDATE', 'DELETE'))) pva,
                  (SELECT pv.vendor_id,
                          DECODE (UPPER (pv.vendor_type_lookup_code),
                                  'EMPLOYEE', papf.national_identifier,
                                  DECODE (pv.organization_type_lookup_code,  'INDIVIDUAL', pv.individual_1099,  'FOREIGN INDIVIDUAL', pv.individual_1099,  hp.jgzz_fiscal_code))
                             hz_taxpayer_id
                     FROM po_vendors pv,
                          ap_awt_groups aag,
                          rcv_routing_headers rcpt,
                          fnd_currencies_tl fct,
                          fnd_currencies_tl pay,
                          fnd_lookup_values pay_group,
                          ap_terms_tl terms,
                          po_vendors PARENT,
                          per_employees_current_x emp,
                          hz_parties hp,
                          ap_income_tax_types aptt,
                          per_all_people_f papf
                    WHERE     pv.party_id = hp.party_id
                          AND pv.parent_vendor_id = PARENT.vendor_id(+)
                          AND pv.awt_group_id = aag.GROUP_ID(+)
                          AND pv.receiving_routing_id = rcpt.routing_header_id(+)
                          AND fct.LANGUAGE(+) = USERENV ('lang')
                          AND pay.LANGUAGE(+) = USERENV ('lang')
                          AND pv.invoice_currency_code = fct.currency_code(+)
                          AND pv.payment_currency_code = pay.currency_code(+)
                          AND pv.pay_group_lookup_code = pay_group.lookup_code(+)
                          AND pay_group.lookup_type(+) = 'PAY GROUP'
                          AND pay_group.LANGUAGE(+) = USERENV ('lang')
                          AND pv.terms_id = terms.term_id(+)
                          AND terms.LANGUAGE(+) = USERENV ('LANG')
                          AND terms.enabled_flag(+) = 'Y'
                          AND pv.employee_id = emp.employee_id(+)
                          AND pv.employee_id = papf.person_id(+)
                          AND pv.type_1099 = aptt.income_tax_type(+)) pv_tax
            WHERE     ac.check_id = aip.check_id
                  AND aip.invoice_id = ai.invoice_id
                  AND ai.invoice_id = ail.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.line_type_lookup_code = 'AWT'
                  AND ail.line_source = 'AUTO WITHHOLDING'
                  AND ail.awt_group_name = atc.NAME
                  AND ac.vendor_id = pv.vendor_id
                  AND pv.party_id = pva.party_id(+)
                  AND pvs.party_site_id = pva.party_site_id
                  AND pv.vendor_id = pv_tax.vendor_id(+)
                  AND awt_rate.tax_name = atc.NAME
                  AND SYSDATE >= NVL (awt_rate.start_date, '01-JAN-1900')
                  AND SYSDATE <= NVL (awt_rate.end_date, '01-JAN-2900')
                  AND awt_rate.rate_type = 'STANDARD'
                  AND ac.vendor_site_id = pvs.vendor_site_id
                  AND ( (aip.invoice_payment_id,
                         0,
                         'AP_INVOICE_PAYMENTS_AWT_A',
                         ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.check_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id))
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND ac.check_id = v_check_id
                  AND atc.attribute15 = hla.location_code
                  AND pvs.party_site_id = zptp.party_id
                  AND zptp.party_type_code = 'THIRD_PARTY_SITE'
                  AND NVL (ac.check_id, 0) = NVL (v_check_id, 0)
                  AND NVL (ac.doc_sequence_value, '0') = NVL (v_voucher_number, '0')
                  AND NVL (atc.attribute12, '0') = NVL (v_pnd, '0')
                  AND NVL (hla.location_id, 0) = NVL (v_location_id, 0)
         GROUP BY ac.check_id,
                  ac.check_number,
                  hla.location_id,
                  ac.check_date,
                  atc.attribute12,
                  atc.NAME,
                  NVL (atc.attribute14, pv.global_attribute17),
                  NVL (atc.attribute13, pv.global_attribute18),
                  NVL (pv.vendor_name, ac.vendor_name),
                  DECODE (pva.address_line1, NULL, ac.address_line1, pva.address_line1),
                  DECODE (pva.address_line1, NULL, ac.address_line2, pva.address_line2),
                  DECODE (pva.address_line1,
                          NULL, ac.address_line3,
                          pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip),
                  NVL (pv_tax.hz_taxpayer_id, pv.vat_registration_num),
                  pvs.global_attribute20,
                  hla.attribute15,
                  hla.address_line_1,
                  hla.address_line_2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code,
                  hla.attribute1,
                  hla.attribute14,
                  ac.doc_sequence_value,
                  awt_rate.tax_rate,
                  zptp.rep_registration_number
         ORDER BY ac.doc_sequence_value;

      rec_line                        cursor_line%ROWTYPE;

      -- Cursor for Load Data from Invoice to Table : thl_wht_line
      CURSOR cursor_invoice (
         v_check_id          NUMBER,
         v_voucher_number    NUMBER,
         v_pnd               VARCHAR2,
         v_location_id       NUMBER,
         v_wht_code          VARCHAR2)
      IS
           SELECT ac.check_id,
                  ai.invoice_num,
                  ai.invoice_id,
                  aip.invoice_payment_id,
                  ac.check_number doc_number,
                  ac.check_date doc_date,
                  'PAY' doc_type,
                  'Bank Account' doc_relate,
                  hla.location_id location_id,
                  ac.doc_sequence_value voucher_number,
                  TO_CHAR (ac.check_date, 'MON-YYYY') period_name,
                  atc.attribute12 pnd,
                  atc.NAME wht_code,
                  NVL (atc.attribute14, pv.global_attribute17) revenue_type,
                  NVL (atc.attribute13, pv.global_attribute18) revenue_name,
                  NVL (pv.vendor_name, ac.vendor_name) supplier_name,
                  DECODE (pva.address_line1, NULL, ac.address_line1, pva.address_line1) supplier_address1,
                  DECODE (pva.address_line1, NULL, ac.address_line2, pva.address_line2) supplier_address2,
                  DECODE (pva.address_line1,
                          NULL, ac.address_line3,
                          pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip)
                     supplier_address3,
                  zptp.rep_registration_number supplier_tax_id,
                  pvs.global_attribute20 supplier_branch_no,
                  '' agent_name,
                  '' agent_address,
                  '' agent_tax_id,
                  '' agent_branch_no,
                  hla.attribute15 resp_name,
                  hla.address_line_1 resp_address1,
                  hla.address_line_2 resp_address2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code resp_address3,
                  hla.attribute1 resp_tax_id,
                  SUBSTR (hla.attribute14, 1, 4) resp_branch_no,
                  user_id created_by,
                  SYSDATE creation_date,
                  user_id last_updated_by,
                  SYSDATE last_update_date,
                  user_id last_update_login
             FROM ap_checks ac,
                  ap_invoice_payments aip,
                  ap_invoices ai,
                  ap_invoice_lines_v ail,
                  ap_invoice_distributions aid,
                  ap_tax_codes atc,
                  hr_locations hla,
                  ap_suppliers pv,
                  ap_supplier_sites pvs,
                  zx_party_tax_profile zptp,
                  (SELECT hps.party_id,
                          hps.party_site_id,
                          hzl.address1 AS address_line1,
                          hzl.address2 AS address_line2,
                          hzl.address3 AS address_line3,
                          hzl.city AS city,
                          hzl.state AS state,
                          hzl.province AS province,
                          hzl.county AS county,
                          hzl.postal_code AS zip,
                          phone.raw_phone_number AS phone,
                          'US' AS LANGUAGE,
                          fax.raw_phone_number AS fax,
                          hzl.address4 AS address_line4,
                          '' AS area_code,
                          hzl.country AS country
                     FROM hz_party_sites hps,
                          hz_locations hzl,
                          fnd_territories_vl fvl,
                          hz_contact_points email,
                          hz_contact_points phone,
                          hz_contact_points fax,
                          hz_party_site_uses pay,
                          hz_party_site_uses pur,
                          hz_party_site_uses rfq
                    WHERE     hps.status = 'A'
                          AND hzl.country = fvl.territory_code
                          AND email.owner_table_id(+) = hps.party_site_id
                          AND email.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND email.status(+) = 'A'
                          AND email.contact_point_type(+) = 'EMAIL'
                          AND email.primary_flag(+) = 'Y'
                          AND phone.owner_table_id(+) = hps.party_site_id
                          AND phone.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND phone.status(+) = 'A'
                          AND phone.contact_point_type(+) = 'PHONE'
                          AND phone.phone_line_type(+) = 'GEN'
                          AND phone.primary_flag(+) = 'Y'
                          AND fax.owner_table_id(+) = hps.party_site_id
                          AND fax.owner_table_name(+) = 'HZ_PARTY_SITES'
                          AND fax.status(+) = 'A'
                          AND fax.contact_point_type(+) = 'PHONE'
                          AND fax.phone_line_type(+) = 'FAX'
                          AND hps.location_id = hzl.location_id
                          AND pay.party_site_id(+) = hps.party_site_id
                          AND pur.party_site_id(+) = hps.party_site_id
                          AND rfq.party_site_id(+) = hps.party_site_id
                          AND pay.status(+) = 'A'
                          AND pur.status(+) = 'A'
                          AND rfq.status(+) = 'A'
                          AND NVL (pay.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pur.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (rfq.end_date(+), SYSDATE) >= SYSDATE
                          AND NVL (pay.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (pur.begin_date(+), SYSDATE) <= SYSDATE
                          AND NVL (rfq.begin_date(+), SYSDATE) <= SYSDATE
                          AND pay.site_use_type(+) = 'PAY'
                          AND pur.site_use_type(+) = 'PURCHASING'
                          AND rfq.site_use_type(+) = 'RFQ'
                          AND NOT EXISTS
                                     (SELECT 1
                                        FROM pos_address_requests par, pos_supplier_mappings psm
                                       WHERE     psm.party_id = hps.party_id
                                             AND psm.mapping_id = par.mapping_id
                                             AND party_site_id = hps.party_site_id
                                             AND request_status = 'PENDING'
                                             AND request_type IN ('UPDATE', 'DELETE'))) pva,
                  (SELECT pv.vendor_id,
                          DECODE (UPPER (pv.vendor_type_lookup_code),
                                  'EMPLOYEE', papf.national_identifier,
                                  DECODE (pv.organization_type_lookup_code,  'INDIVIDUAL', pv.individual_1099,  'FOREIGN INDIVIDUAL', pv.individual_1099,  hp.jgzz_fiscal_code))
                             hz_taxpayer_id
                     FROM po_vendors pv,
                          ap_awt_groups aag,
                          rcv_routing_headers rcpt,
                          fnd_currencies_tl fct,
                          fnd_currencies_tl pay,
                          fnd_lookup_values pay_group,
                          ap_terms_tl terms,
                          po_vendors PARENT,
                          per_employees_current_x emp,
                          hz_parties hp,
                          ap_income_tax_types aptt,
                          per_all_people_f papf
                    WHERE     pv.party_id = hp.party_id
                          AND pv.parent_vendor_id = PARENT.vendor_id(+)
                          AND pv.awt_group_id = aag.GROUP_ID(+)
                          AND pv.receiving_routing_id = rcpt.routing_header_id(+)
                          AND fct.LANGUAGE(+) = USERENV ('lang')
                          AND pay.LANGUAGE(+) = USERENV ('lang')
                          AND pv.invoice_currency_code = fct.currency_code(+)
                          AND pv.payment_currency_code = pay.currency_code(+)
                          AND pv.pay_group_lookup_code = pay_group.lookup_code(+)
                          AND pay_group.lookup_type(+) = 'PAY GROUP'
                          AND pay_group.LANGUAGE(+) = USERENV ('lang')
                          AND pv.terms_id = terms.term_id(+)
                          AND terms.LANGUAGE(+) = USERENV ('LANG')
                          AND terms.enabled_flag(+) = 'Y'
                          AND pv.employee_id = emp.employee_id(+)
                          AND pv.employee_id = papf.person_id(+)
                          AND pv.type_1099 = aptt.income_tax_type(+)) pv_tax
            WHERE     ac.check_id = aip.check_id
                  AND aip.invoice_id = ai.invoice_id
                  AND ai.invoice_id = ail.invoice_id
                  AND ail.invoice_id = aid.invoice_id
                  AND ail.line_number = aid.invoice_line_number
                  AND ail.line_type_lookup_code = 'AWT'
                  AND ail.line_source = 'AUTO WITHHOLDING'
                  AND ail.awt_group_name = atc.NAME
                  AND ac.vendor_id = pv.vendor_id
                  AND pv.party_id = pva.party_id(+)
                  AND pvs.party_site_id = pva.party_site_id
                  AND pv.vendor_id = pv_tax.vendor_id(+)
                  AND ac.vendor_site_id = pvs.vendor_site_id
                  AND ( (aip.invoice_payment_id,
                         0,
                         'AP_INVOICE_PAYMENTS_AWT_A',
                         ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                     0,
                                                     tidld.reference_type,
                                                     tidld.check_id
                                                FROM thl_data_loaded tidld
                                               WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id))
                  AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                  AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                  AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                           ai.invoice_amount,
                                                           ai.payment_status_flag,
                                                           ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                  AND ac.check_id = v_check_id
                  AND atc.attribute15 = hla.location_code
                  AND pvs.party_site_id = zptp.party_id
                  AND zptp.party_type_code = 'THIRD_PARTY_SITE'
                  AND NVL (ac.check_id, 0) = NVL (v_check_id, 0)
                  AND NVL (ac.doc_sequence_value, '0') = NVL (v_voucher_number, '0')
                  AND NVL (atc.attribute12, '0') = NVL (v_pnd, '0')
                  AND NVL (hla.location_id, 0) = NVL (v_location_id, 0)
                  AND NVL (atc.NAME, '0') = NVL (v_wht_code, '0')
         GROUP BY ac.check_id,
                  ac.check_number,
                  hla.location_id,
                  ac.check_date,
                  atc.attribute12,
                  atc.NAME,
                  NVL (atc.attribute14, pv.global_attribute17),
                  NVL (atc.attribute13, pv.global_attribute18),
                  NVL (pv.vendor_name, ac.vendor_name),
                  DECODE (pva.address_line1, NULL, ac.address_line1, pva.address_line1),
                  DECODE (pva.address_line1, NULL, ac.address_line2, pva.address_line2),
                  DECODE (pva.address_line1,
                          NULL, ac.address_line3,
                          pva.address_line3 || ' ' || pva.city || ' ' || pva.county || ' ' || pva.state || ' ' || pva.province || ' ' || pva.zip),
                  NVL (pv_tax.hz_taxpayer_id, pv.vat_registration_num),
                  pvs.global_attribute20,
                  hla.attribute15,
                  hla.address_line_1,
                  hla.address_line_2,
                  hla.address_line_3 || ' ' || hla.region_2 || ' ' || hla.postal_code,
                  hla.attribute1,
                  hla.attribute14,
                  ac.doc_sequence_value,
                  ai.invoice_num,
                  ai.invoice_id,
                  aip.invoice_payment_id,
                  zptp.rep_registration_number
         ORDER BY ac.doc_sequence_value;

      rec_invoice                     cursor_invoice%ROWTYPE;

      -- Cursor for Load Data to Table : thl_data_loaded
      CURSOR cursor_invoice_dis
      IS
         SELECT DISTINCT ai.invoice_id,
                         ac.check_id,
                         aid.invoice_distribution_id,
                         aip.invoice_payment_id
           FROM ap_checks ac,
                ap_invoice_payments aip,
                ap_invoices ai,
                ap_invoice_lines_v ail,
                ap_invoice_distributions aid,
                ap_tax_codes atc,
                hr_locations hla,
                ap_suppliers pv,
                ap_supplier_sites pvs
          WHERE     ac.check_id = aip.check_id
                AND aip.invoice_id = ai.invoice_id
                AND ai.invoice_id = ail.invoice_id
                AND ail.invoice_id = aid.invoice_id
                AND ail.line_number = aid.invoice_line_number
                AND ail.line_type_lookup_code = 'AWT'
                AND ail.line_source = 'AUTO WITHHOLDING'
                AND ail.awt_group_name = atc.NAME
                AND ac.vendor_id = pv.vendor_id
                AND ac.vendor_site_id = pvs.vendor_site_id
                AND ( (aip.invoice_payment_id,
                       0,
                       'AP_INVOICE_PAYMENTS_AWT_A',
                       ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                   0,
                                                   tidld.reference_type,
                                                   tidld.check_id
                                              FROM thl_data_loaded tidld
                                             WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id))
                AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                         ai.invoice_amount,
                                                         ai.payment_status_flag,
                                                         ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                AND ac.check_id = p_check_id
                AND atc.attribute15 = hla.location_code;

      rec_invoice_dis                 cursor_invoice_dis%ROWTYPE;
      error_whtno                     EXCEPTION;
      v_inv_base_buf                  NUMBER;
   BEGIN
      v_insert := FALSE;
      user_id := fnd_profile.VALUE ('USER_ID');
      org_id := fnd_profile.VALUE ('ORG_ID');
      appl_id := fnd_profile.VALUE ('RESP_APPL_ID');

      OPEN cursor_head;

      LOOP
         FETCH cursor_head INTO rec_head;

         EXIT WHEN cursor_head%NOTFOUND;

         SELECT thl_whth_s.NEXTVAL INTO v_head_id FROM DUAL;

         v_check_base := 0;
         v_check_tax := 0;
         write_log ('Check Number : ' || rec_head.doc_number);

         OPEN cursor_line (rec_head.check_id,
                           rec_head.voucher_number,
                           rec_head.pnd,
                           rec_head.location_id);

         LOOP
            FETCH cursor_line INTO rec_line;

            EXIT WHEN cursor_line%NOTFOUND;
            v_line_base := 0;
            v_line_tax := 0;

            SELECT thl_whtl_s.NEXTVAL INTO v_line_id FROM DUAL;

            v_inv_base := 0;
            v_inv_tax := 0;

            OPEN cursor_invoice (rec_line.check_id,
                                 rec_line.voucher_number,
                                 rec_line.pnd,
                                 rec_line.location_id,
                                 rec_line.wht_code);

            LOOP
               FETCH cursor_invoice INTO rec_invoice;

               EXIT WHEN cursor_invoice%NOTFOUND;
               write_log ('10 begin : Calculate WHT BASE AMOUNT');

               BEGIN
                  BEGIN
                     SELECT NVL (SUM (ROUND (ROUND (aid.awt_gross_amount, 2) * ROUND (NVL (ai.exchange_rate, 1), 2), 2)), 0)
                       INTO v_inv_base_buf
                       FROM ap_invoices ai,
                            ap_invoice_lines_v ail,
                            ap_invoice_distributions aid,
                            ap_invoice_payments aip,
                            ap_checks ac,
                            ap_tax_codes atc,
                            hr_locations hla
                      WHERE     ai.invoice_id = ail.invoice_id
                            AND ail.invoice_id = aid.invoice_id
                            AND ail.line_number = aid.invoice_line_number
                            AND ail.line_type_lookup_code IN ('AWT')
                            AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                            AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                            AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                     ai.invoice_amount,
                                                                     ai.payment_status_flag,
                                                                     ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                            AND (aip.invoice_payment_id,
                                 0,
                                 'AP_INVOICE_PAYMENTS_AWT_A',
                                 ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                             0,
                                                             tidld.reference_type,
                                                             tidld.check_id
                                                        FROM thl_data_loaded tidld
                                                       WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id)
                            AND (aid.invoice_distribution_id,
                                 0,
                                 'AP_INVOICE_DISTRIBUTIONS_AWT_A',
                                 ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                             0,
                                                             tidld.reference_type,
                                                             tidld.check_id
                                                        FROM thl_data_loaded tidld
                                                       WHERE tidld.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_A' AND tidld.reference_id = aid.invoice_distribution_id)
                            AND ai.invoice_id = aip.invoice_id
                            AND aip.check_id = ac.check_id
                            AND ail.awt_group_name = atc.NAME
                            AND atc.attribute15 = hla.location_code
                            AND aip.invoice_payment_id = aid.awt_invoice_payment_id
                            AND ac.check_id = rec_line.check_id
                            AND ai.invoice_id = rec_invoice.invoice_id
                            AND NVL (atc.NAME, 0) = NVL (rec_line.wht_code, 0)
                            AND NVL (hla.location_id, 0) = NVL (rec_line.location_id, 0)
                            AND NVL (atc.attribute12, '0') = NVL (rec_line.pnd, '0')
                            AND NVL (ac.doc_sequence_value, '0') = NVL (rec_line.voucher_number, '0');
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        v_inv_base_buf := 0;
                  END;

                  IF v_inv_base_buf = 0
                  THEN
                     v_inv_base := 0;
                  ELSE
                     SELECT NVL (MAX (ROUND (ROUND (aid.awt_gross_amount, 2) * ROUND (NVL (ai.exchange_rate, 1), 2), 2)), 0)
                       INTO v_inv_base
                       FROM ap_invoices ai,
                            ap_invoice_lines_v ail,
                            ap_invoice_distributions aid,
                            ap_invoice_payments aip,
                            ap_checks ac,
                            ap_tax_codes atc,
                            hr_locations hla
                      WHERE     ai.invoice_id = ail.invoice_id
                            AND ail.invoice_id = aid.invoice_id
                            AND ail.line_number = aid.invoice_line_number
                            AND ail.line_type_lookup_code IN ('AWT')
                            AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                            AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                            AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                     ai.invoice_amount,
                                                                     ai.payment_status_flag,
                                                                     ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                            AND (aip.invoice_payment_id,
                                 0,
                                 'AP_INVOICE_PAYMENTS_AWT_A',
                                 ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                             0,
                                                             tidld.reference_type,
                                                             tidld.check_id
                                                        FROM thl_data_loaded tidld
                                                       WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id)
                            AND (aid.invoice_distribution_id,
                                 0,
                                 'AP_INVOICE_DISTRIBUTIONS_AWT_A',
                                 ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                             0,
                                                             tidld.reference_type,
                                                             tidld.check_id
                                                        FROM thl_data_loaded tidld
                                                       WHERE tidld.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_A' AND tidld.reference_id = aid.invoice_distribution_id)
                            AND ai.invoice_id = aip.invoice_id
                            AND aip.check_id = ac.check_id
                            AND ail.awt_group_name = atc.NAME
                            AND atc.attribute15 = hla.location_code
                            AND aip.invoice_payment_id = aid.awt_invoice_payment_id
                            AND ac.check_id = rec_line.check_id
                            AND ai.invoice_id = rec_invoice.invoice_id
                            AND NVL (atc.NAME, 0) = NVL (rec_line.wht_code, 0)
                            AND NVL (hla.location_id, 0) = NVL (rec_line.location_id, 0)
                            AND NVL (atc.attribute12, '0') = NVL (rec_line.pnd, '0')
                            AND NVL (ac.doc_sequence_value, '0') = NVL (rec_line.voucher_number, '0');
                  END IF;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_inv_base := 0;
               END;

               write_log ('10 end : Calculate WHT BASE AMOUNT');
               write_log ('20 begin : Calculate WHT AMOUNT');

               BEGIN
                  SELECT NVL (SUM (ROUND (ROUND (aid.amount, 2) * ROUND (NVL (ai.exchange_rate, 1), 2), 2)) * (-1), 0)
                    INTO v_inv_tax
                    FROM ap_invoices ai,
                         ap_invoice_lines_v ail,
                         ap_invoice_distributions aid,
                         ap_invoice_payments aip,
                         ap_checks ac,
                         ap_tax_codes atc,
                         hr_locations hla
                   WHERE     ai.invoice_id = ail.invoice_id
                         AND ail.invoice_id = aid.invoice_id
                         AND ail.line_number = aid.invoice_line_number
                         AND ail.line_type_lookup_code IN ('AWT')
                         AND ap_invoices_pkg.get_posting_status (ai.invoice_id) IN ('Y', 'P')
                         AND ap_checks_pkg.get_posting_status (ac.check_id) = 'Y'
                         AND ap_invoices_pkg.get_approval_status (ai.invoice_id,
                                                                  ai.invoice_amount,
                                                                  ai.payment_status_flag,
                                                                  ai.invoice_type_lookup_code) IN ('APPROVED', 'CANCELLED', 'AVAILABLE')
                         AND (aip.invoice_payment_id,
                              0,
                              'AP_INVOICE_PAYMENTS_AWT_A',
                              ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                          0,
                                                          tidld.reference_type,
                                                          tidld.check_id
                                                     FROM thl_data_loaded tidld
                                                    WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id)
                         AND (aid.invoice_distribution_id,
                              0,
                              'AP_INVOICE_DISTRIBUTIONS_AWT_A',
                              ac.check_id) NOT IN (SELECT tidld.reference_id,
                                                          0,
                                                          tidld.reference_type,
                                                          tidld.check_id
                                                     FROM thl_data_loaded tidld
                                                    WHERE tidld.reference_type = 'AP_INVOICE_DISTRIBUTIONS_AWT_A' AND tidld.reference_id = aid.invoice_distribution_id)
                         AND ai.invoice_id = aip.invoice_id
                         AND aip.check_id = ac.check_id
                         AND ail.awt_group_name = atc.NAME
                         AND atc.attribute15 = hla.location_code
                         AND aip.invoice_payment_id = aid.awt_invoice_payment_id
                         AND ac.check_id = rec_line.check_id
                         AND ai.invoice_id = rec_invoice.invoice_id
                         AND NVL (atc.NAME, 0) = NVL (rec_line.wht_code, 0)
                         AND NVL (hla.location_id, 0) = NVL (rec_line.location_id, 0)
                         AND NVL (atc.attribute12, '0') = NVL (rec_line.pnd, '0')
                         AND NVL (ac.doc_sequence_value, '0') = NVL (rec_line.voucher_number, '0');
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_inv_tax := 0;
               END;

               write_log ('20 end : Calculate WHT AMOUNT');
               v_line_base := v_line_base + v_inv_base;
               v_line_tax := v_line_tax + v_inv_tax;
            END LOOP;

            CLOSE cursor_invoice;

            -- Insert Data to table : THL_WHT_LINE
            write_log ('30 begin : Insert Data to table : THL_WHT_LINE');

            INSERT INTO thl_wht_line (th_whtl_id,
                                      th_whth_id,
                                      th_whtl_wt_code,
                                      th_whtl_revenue_type,
                                      th_whtl_revenue_name,
                                      th_whtl_base_amt,
                                      th_whtl_tax_amt,
                                      th_whtl_supplier_name,
                                      th_whtl_supplier_address1,
                                      th_whtl_supplier_address2,
                                      th_whtl_supplier_address3,
                                      th_whtl_supplier_tax_id,
                                      th_whtl_supplier_branch_no,
                                      th_whtl_agent_name,
                                      th_whtl_agent_address1,
                                      th_whtl_agent_address2,
                                      th_whtl_agent_address3,
                                      th_whtl_agent_tax_id,
                                      th_whtl_agent_branch_no,
                                      th_whtl_resp_name,
                                      th_whtl_resp_address1,
                                      th_whtl_resp_address2,
                                      th_whtl_resp_address3,
                                      th_whtl_resp_tax_id,
                                      th_whtl_resp_branch_no,
                                      attribute1,
                                      created_by,
                                      creation_date,
                                      last_updated_by,
                                      last_update_date,
                                      last_update_login)
                 VALUES (v_line_id,
                         v_head_id,
                         rec_line.wht_code,
                         rec_line.revenue_type,
                         rec_line.revenue_name,
                         v_line_base,
                         v_line_tax * (-1),
                         rec_line.supplier_name,
                         rec_line.supplier_address1,
                         rec_line.supplier_address2,
                         rec_line.supplier_address3,
                         rec_line.supplier_tax_id,
                         rec_line.supplier_branch_no,
                         rec_line.agent_name,
                         rec_line.agent_address,
                         NULL,
                         NULL,
                         rec_line.agent_tax_id,
                         rec_line.agent_branch_no,
                         rec_line.resp_name,
                         rec_line.resp_address1,
                         rec_line.resp_address2,
                         rec_line.resp_address3,
                         rec_line.resp_tax_id,
                         rec_line.resp_branch_no,
                         p_conc_request_id,
                         rec_line.created_by,
                         rec_line.creation_date,
                         rec_line.last_updated_by,
                         rec_line.last_update_date,
                         rec_line.last_update_login);

            write_log ('30 end : Insert Data to table : THL_WHT_LINE');
            v_check_base := v_check_base + v_line_base;
            v_check_tax := v_check_tax + v_line_tax;
         END LOOP;

         CLOSE cursor_line;

         -- Find Period Name from CHECK DATE
         write_log ('40 begin : Find Period Name from CHECK DATE');

           SELECT period_name
             INTO v_nperiod
             FROM hr_operating_units hou, gl_sets_of_books gsob, gl_periods_v gpv
            WHERE     (hou.date_from <= SYSDATE AND NVL (hou.date_to, SYSDATE) >= SYSDATE)
                  AND hou.set_of_books_id = gsob.set_of_books_id
                  AND gsob.period_set_name = gpv.period_set_name
                  AND hou.organization_id = org_id
                  AND (TRUNC (rec_head.doc_date) BETWEEN gpv.start_date AND gpv.end_date)
                  AND gpv.adjustment_period_flag = 'N'
         ORDER BY start_date;

         write_log ('40 end : Find Period Name from CHECK DATE');
         -- Create WHT Number
         write_log ('50 begin : Create WHT Number');
         v_period := SUBSTR (rec_head.period_name, 1, 3) || SUBSTR (rec_head.period_name, 7, 2);
         v_query :=                                                                                                                                 -- 'select th_wht_'      -- V1.0
                   'select thl_wht_a_'                                                                                                                                       -- V2.0
                                      || v_period || '_' || rec_head.pnd || '_s.nextval from dual';

         BEGIN
            cursor1 := DBMS_SQL.open_cursor;
            DBMS_SQL.parse (cursor1, v_query, 2);
            DBMS_SQL.define_column (cursor1, 1, v_wht_num);
            rows_processed := DBMS_SQL.EXECUTE (cursor1);

            IF DBMS_SQL.fetch_rows (cursor1) > 0
            THEN
               DBMS_SQL.COLUMN_VALUE (cursor1, 1, v_wht_num);
            ELSE
               EXIT;
            END IF;

            DBMS_SQL.close_cursor (cursor1);
         EXCEPTION
            WHEN OTHERS
            THEN
               write_log ('50 : Can not create WHT Number , ' || rec_head.period_name || ' , ' || rec_head.pnd);
               ROLLBACK;
               RAISE error_whtno;

               IF DBMS_SQL.is_open (cursor1)
               THEN
                  DBMS_SQL.close_cursor (cursor1);
               END IF;

               EXIT;
         END;

         write_log ('50 end : Create WHT Number');
         v_insert := TRUE;
         -- Insert Data to table : THL_WHT_HEADER
         write_log ('60 begin : Insert Data to table : THL_WHT_HEADER');

         INSERT INTO thl_wht_header (th_whth_id,
                                     th_whth_doc_id,
                                     th_whth_doc_number,
                                     th_whth_doc_date,
                                     th_whth_type,
                                     th_whth_related_document,
                                     th_whth_location_id,
                                     th_whth_base_amt,
                                     th_whth_tax_amt,
                                     th_whth_voucher_number,
                                     th_whth_period_name,
                                     th_whth_wht_number,
                                     th_phor_ngor_dor,
                                     attribute1,
                                     created_by,
                                     creation_date,
                                     last_updated_by,
                                     last_update_date,
                                     last_update_login,
                                     th_act_transaction_date                                                                                                                 -- V2.0
                                                            )
              VALUES (v_head_id,
                      rec_head.check_id,
                      rec_head.doc_number,
                      rec_head.doc_date,
                      rec_head.doc_type,
                      rec_head.doc_relate,
                      rec_head.location_id,
                      v_check_base,
                      v_check_tax * (-1),
                      rec_head.voucher_number,
                      v_nperiod,
                      v_wht_num,
                      rec_head.pnd,
                      p_conc_request_id,
                      rec_head.created_by,
                      rec_head.creation_date,
                      rec_head.last_updated_by,
                      rec_head.last_update_date,
                      rec_head.last_update_login,
                      rec_head.accounting_date                                                                                                                               -- V2.0
                                              );

         write_log ('60 end : Insert Data to table : THL_WHT_HEADER');
      END LOOP;

      CLOSE cursor_head;

      -- Insert Data to table : THL_DATA_LOADED
      -- For Check Data Loaded
      IF v_insert
      THEN
         write_log ('70 begin : Insert Data to table : THL_DATA_LOADED');

         OPEN cursor_head;

         LOOP
            FETCH cursor_head INTO rec_head;

            EXIT WHEN cursor_head%NOTFOUND;

            OPEN cursor_line (rec_head.check_id,
                              rec_head.voucher_number,
                              rec_head.pnd,
                              rec_head.location_id);

            LOOP
               FETCH cursor_line INTO rec_line;

               EXIT WHEN cursor_line%NOTFOUND;

               OPEN cursor_invoice (rec_line.check_id,
                                    rec_line.voucher_number,
                                    rec_line.pnd,
                                    rec_line.location_id,
                                    rec_line.wht_code);

               LOOP
                  FETCH cursor_invoice INTO rec_invoice;

                  EXIT WHEN cursor_invoice%NOTFOUND;

                  BEGIN
                     INSERT INTO thl_data_loaded (reference_id,
                                                  reference_line_number,
                                                  reference_type,
                                                  invoice_id,
                                                  check_id,
                                                  created_by,
                                                  creation_date,
                                                  last_updated_by,
                                                  last_update_date,
                                                  last_update_login)
                        SELECT DISTINCT xdl.event_id,
                                        0,
                                        'XLA_DISTRIBUTION_LINKS_AWT_A',
                                        aip.invoice_id,
                                        aip.check_id,
                                        user_id,
                                        SYSDATE,
                                        user_id,
                                        SYSDATE,
                                        user_id
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
                               AND aip.check_id = rec_head.check_id
                               AND aip.invoice_id = rec_invoice.invoice_id
                               AND applied_to_source_id_num_1 = rec_invoice.invoice_id
                               AND (xdl.event_id,
                                    0,
                                    'XLA_DISTRIBUTION_LINKS_AWT_A',
                                    aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                                 0,
                                                                 tidld.reference_type,
                                                                 tidld.check_id
                                                            FROM thl_data_loaded tidld
                                                           WHERE tidld.reference_type = 'XLA_DISTRIBUTION_LINKS_AWT_A' AND tidld.reference_id = xdl.event_id)
                               AND (aip.invoice_payment_id,
                                    0,
                                    'AP_INVOICE_PAYMENTS_AWT_A',
                                    aip.check_id) NOT IN (SELECT tidld.reference_id,
                                                                 0,
                                                                 tidld.reference_type,
                                                                 tidld.check_id
                                                            FROM thl_data_loaded tidld
                                                           WHERE tidld.reference_type = 'AP_INVOICE_PAYMENTS_AWT_A' AND tidld.reference_id = aip.invoice_payment_id);
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        NULL;
                  END;
               END LOOP;

               CLOSE cursor_invoice;
            END LOOP;

            CLOSE cursor_line;
         END LOOP;

         CLOSE cursor_head;

         OPEN cursor_invoice_dis;

         LOOP
            FETCH cursor_invoice_dis INTO rec_invoice_dis;

            EXIT WHEN cursor_invoice_dis%NOTFOUND;

            INSERT INTO thl_data_loaded (reference_id,
                                         reference_line_number,
                                         reference_type,
                                         invoice_id,
                                         check_id,
                                         created_by,
                                         creation_date,
                                         last_updated_by,
                                         last_update_date,
                                         last_update_login)
                 VALUES (rec_invoice_dis.invoice_distribution_id,
                         0,
                         'AP_INVOICE_DISTRIBUTIONS_AWT_A',
                         rec_invoice_dis.invoice_id,
                         rec_invoice_dis.check_id,
                         user_id,
                         SYSDATE,
                         user_id,
                         SYSDATE,
                         user_id);

            INSERT INTO thl_data_loaded (reference_id,
                                         reference_line_number,
                                         reference_type,
                                         invoice_id,
                                         check_id,
                                         created_by,
                                         creation_date,
                                         last_updated_by,
                                         last_update_date,
                                         last_update_login)
                 VALUES (rec_invoice_dis.invoice_payment_id,
                         0,
                         'AP_INVOICE_PAYMENTS_AWT_A',
                         rec_invoice_dis.invoice_id,
                         rec_invoice_dis.check_id,
                         user_id,
                         SYSDATE,
                         user_id,
                         SYSDATE,
                         user_id);
         END LOOP;

         CLOSE cursor_invoice_dis;

         write_log ('70 end : Insert Data to table : THL_DATA_LOADED');
      END IF;

      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
   END thl_load_wht_a;

   /*================================================================

     PROCEDURE NAME:    write_log()

   ==================================================================*/
   PROCEDURE write_log (param_msg VARCHAR2)
   IS
   BEGIN
      fnd_file.put_line (fnd_file.LOG, param_msg);
   END write_log;
END thl_load_localization; 
/