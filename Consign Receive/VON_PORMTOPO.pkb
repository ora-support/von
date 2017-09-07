CREATE OR REPLACE PACKAGE BODY APPS.von_pormtopo
AS
   inv_api_version                  NUMBER DEFAULT 1.0;
   g_ret_sts_success       CONSTANT VARCHAR2 (1) := 'S';
   g_ret_sts_error         CONSTANT VARCHAR2 (1) := 'E';
   g_ret_sts_unexp_error   CONSTANT VARCHAR2 (1) := 'U';
   g_miss_num              CONSTANT NUMBER := 9.99e125;
   g_miss_char             CONSTANT VARCHAR2 (1) := CHR (0);
   g_miss_date             CONSTANT DATE := TO_DATE ('1', 'j');
   /*Exception definitions */
   g_exc_error                      EXCEPTION;
   g_exc_unexpected_error           EXCEPTION;
   g_debug                          NUMBER := NVL (fnd_profile.VALUE ('INV_DEBUG_TRACE'), 0);
   g_cns_status_result              VARCHAR2 (200);

   PROCEDURE write_log (buff VARCHAR2, logtype VARCHAR2)
   AS
   BEGIN
      IF UPPER (logtype) = 'LOG'
      THEN
         fnd_file.put_line (fnd_file.LOG, buff);
      -- DBMS_OUTPUT.put_line (buff);
      ELSE
         fnd_file.put_line (fnd_file.output, buff);
      END IF;
   END write_log;

   FUNCTION upd_memo (p_header_id NUMBER)
      RETURN VARCHAR2
   IS
      buyer_id      NUMBER;

      CURSOR r1
      IS
           SELECT DISTINCT memo_header_id, receipt_id
             FROM von_purchase_memo_dt
            WHERE memo_header_id = p_header_id
         ORDER BY receipt_id;

      CURSOR c1_new (c_mm_hid NUMBER)
      IS
         SELECT DISTINCT contract_id
           FROM von_purchase_memo_dt
          WHERE memo_header_id = c_mm_hid;

      --   BEGIN
      --      OPEN cur_user;
      --
      --      FETCH cur_user INTO   buyer_id;
      --
      --      write_log ('User id : ' || userid || ' Buyer id : ' || buyer_id, 'log');
      --
      --      CLOSE cur_user;

      flg           VARCHAR2 (1);
      n             NUMBER;
      n_all         NUMBER := 0;
      n_cur         NUMBER := 0;
      n_cur_real    NUMBER := 0;
      vwhere        VARCHAR2 (1000);
      n_decistion   NUMBER;
   BEGIN
      UPDATE von_purchase_memo_hd
         SET document_status = '3040'
       WHERE memo_header_id = p_header_id;

      COMMIT;

      FOR x IN r1
      LOOP
         UPDATE von_receipt
            SET document_status = '1020'
          WHERE receipt_id = x.receipt_id;

         COMMIT;
      END LOOP;

      FOR y IN c1_new (p_header_id)
      LOOP
         --            UPDATE von_contract
         --               SET document_status = null-- for return to first Step
         --             WHERE contract_id = y.contract_id;
         --           STANDARD. COMMIT;

         SELECT contract_weight, balance_weight
           INTO n_all, n_cur
           FROM von_contract_balance_view
          WHERE contract_id = y.contract_id;

         BEGIN
            SELECT NVL (
                      (  SELECT (con.contract_weight - SUM (memo_net_weight))
                           FROM von_purchase_memo_dt dt, von_purchase_memo_hd hd
                          WHERE     dt.memo_header_id = hd.memo_header_id
                                AND contract_id IS NOT NULL
                                AND con.contract_id = contract_id
                                AND con.contract_number = dt.contract_number
                                AND document_status NOT IN (3040)
                       GROUP BY contract_id, contract_number),
                      con.contract_weight)
                      balance_weight
              INTO n_cur_real
              FROM von_contract con
             WHERE con.document_status NOT IN ('2010', '2050') AND con.contract_id = y.contract_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               n_cur_real := 0;
         END;

         IF n_cur_real > 0 AND n_all = n_cur_real                                                                                                                              -- EMPTY => Open Contract
         THEN
            UPDATE von_contract
               SET document_status = '2020'
             WHERE contract_id = y.contract_id;

            COMMIT;
         ELSIF n_cur_real > 0                                                                                                                                                                    -- Some
         THEN
            UPDATE von_contract
               SET document_status = '2030'
             WHERE contract_id = y.contract_id;

            COMMIT;
         ELSIF n_cur_real = 0
         THEN
            UPDATE von_contract
               SET document_status = '2020'
             WHERE contract_id = y.contract_id;

            COMMIT;
         END IF;
      END LOOP;


      RETURN ('C');
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 'E';
   END upd_memo;

   FUNCTION count_total_memo (p_org_id NUMBER, p_memo_num VARCHAR2)
      RETURN NUMBER
   IS
      v_total   NUMBER;
   BEGIN
      SELECT COUNT (*)
        INTO v_total
        FROM (SELECT /*+ CPU_COSTING  */
                     DISTINCT hd.memo_number
                FROM von_purchase_memo_hd hd, von_purchase_memo_dt dt, hr_organization_units_v hr
               WHERE     hd.memo_header_id = dt.memo_header_id
                     AND hr.organization_id = hd.ou_org_id
                     AND hd.ou_org_id = p_org_id
                     AND (NVL (dt.po_status, 'N') <> 'Y' OR NVL (dt.rcv_status, 'N') <> 'Y' OR NVL (dt.ap_inv_status, 'N') <> 'Y')
                     AND hd.memo_number LIKE (p_memo_num || '%')
                     AND hd.document_status IN (3020, 3030));

      RETURN (v_total);
   END;

   --    FUNCTION chk_consign_status (p_header_id  NUMBER )
   --        RETURN varchar2
   --    IS
   --        v  NUMBER;
   --    BEGIN
   --
   --        (SELECT
   --                DECODE (NVL (x.consign_flag, 'N'), 'Y', NVL (dt.consign_status, 'E'), 'N')
   --                                                  FROM von_receipt x
   --                                                 WHERE x.receipt_id = dt.receipt_id)) NOT IN ('Y', 'N')
   --        ) consign_status
   --
   --
   --       select * from von_receipt irc, von_purchase_memo_dt idt
   --       where irc.receipt_id=idt.receipt_id
   --             and irc.receipt_id= dt.receipt_id and nvl(irc.consign_flag,'N')='Y'
   --             and
   --
   --        RETURN (v_total);
   --    END;

   FUNCTION po_submit_request (pf_batch_id NUMBER, v_user_id NUMBER, pf_org_id NUMBER)
      RETURN NUMBER
   IS
      v_req_id     NUMBER (20) := NULL;
      v_buyer_id   NUMBER;
   BEGIN
      v_buyer_id := get_buyerid (userid => v_user_id);


      IF v_buyer_id IS NOT NULL
      THEN
         BEGIN
            v_req_id :=
               fnd_request.submit_request ('PO',
                                           'POXPOPDOI',
                                           'Import Standard Purchase Orders',
                                           '',
                                           FALSE,
                                           v_buyer_id,                                                                                                                                  -- Default Buyer
                                           'STANDARD',                                                                                                                                  -- Document Type
                                           '',                                                                                                                                       -- Document SubType
                                           'Y',                                                                                                                       -- Create or Update Items (Yes:NO)
                                           '',                                                                                                                                  -- Create Sourcing Rules
                                           'APPROVED',                                                                                                                                -- Approval Status
                                           '',                                                                                                                              -- Release Generation Method
                                           pf_batch_id,                                                                                                                                      -- Batch ID
                                           pf_org_id,
                                           -- By  pcn @ 20100720 '',                                                                                      -- Operating Units
                                           '',                                                                                                                                       -- Global Agreement
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '',
                                           '');

            /*update Standard concurrent Import Standard Purchase Orders org_id(Null is Error)*/
            UPDATE /*+ CPU_COSTING  */
                  fnd_concurrent_requests
               SET org_id = pf_org_id
             WHERE request_id = v_req_id;

            COMMIT;
         EXCEPTION
            WHEN OTHERS
            THEN
               write_log ('Error on PO Interface Concurrent Request at  Memo Header ID =  ' || pf_batch_id || ' ' || SQLERRM, 'log');
         END;

         IF (v_req_id != 0)
         THEN
            RETURN (v_req_id);
         ELSE
            BEGIN
               v_req_id :=
                  fnd_request.submit_request ('PO',
                                              'POXPOPDOI',
                                              'Import Standard Purchase Orders',
                                              '',
                                              FALSE,
                                              v_buyer_id,                                                                                                                               -- Default Buyer
                                              'STANDARD',                                                                                                                               -- Document Type
                                              '',                                                                                                                                    -- Document SubType
                                              'Y',                                                                                                                    -- Create or Update Items (Yes:NO)
                                              '',                                                                                                                               -- Create Sourcing Rules
                                              'APPROVED',                                                                                                                             -- Approval Status
                                              '',                                                                                                                           -- Release Generation Method
                                              pf_batch_id,                                                                                                                                   -- Batch ID
                                              pf_org_id,
                                              -- By  pcn @ 20100720 '',                                                                                      -- Operating Units
                                              '',                                                                                                                                    -- Global Agreement
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '',
                                              '');

               /*update Standard concurrent Import Standard Purchase Orders org_id(Null is Error)*/
               UPDATE /*+ CPU_COSTING  */
                     fnd_concurrent_requests
                  SET org_id = pf_org_id
                WHERE request_id = v_req_id;

               COMMIT;

               IF (v_req_id != 0)
               THEN
                  RETURN (v_req_id);
               ELSE
                  BEGIN
                     v_req_id :=
                        fnd_request.submit_request ('PO',
                                                    'POXPOPDOI',
                                                    'Import Standard Purchase Orders',
                                                    '',
                                                    FALSE,
                                                    v_buyer_id,                                                                                                                         -- Default Buyer
                                                    'STANDARD',                                                                                                                         -- Document Type
                                                    '',                                                                                                                              -- Document SubType
                                                    'Y',                                                                                                              -- Create or Update Items (Yes:NO)
                                                    '',                                                                                                                         -- Create Sourcing Rules
                                                    'APPROVED',                                                                                                                       -- Approval Status
                                                    '',                                                                                                                     -- Release Generation Method
                                                    pf_batch_id,                                                                                                                             -- Batch ID
                                                    pf_org_id,
                                                    -- By  pcn @ 20100720 '',                                                                                      -- Operating Units
                                                    '',                                                                                                                              -- Global Agreement
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '',
                                                    '');
                     COMMIT;

                     /*update Standard concurrent Import Standard Purchase Orders org_id(Null is Error)*/
                     UPDATE /*+ CPU_COSTING  */
                           fnd_concurrent_requests
                        SET org_id = pf_org_id
                      WHERE request_id = v_req_id;

                     COMMIT;
                     RETURN (v_req_id);
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        write_log ('Error on PO Interface Concurrent Request at  Memo Header ID =  ' || pf_batch_id || ' ' || SQLERRM, 'log');
                  END;
               END IF;
            EXCEPTION
               WHEN OTHERS
               THEN
                  write_log ('Error on PO Interface Concurrent Request at  Memo Header ID =  ' || pf_batch_id || ' ' || SQLERRM, 'log');
            END;
         END IF;
      ELSE
         write_log ('Can not Find Buer ID on PO Interface Concurrent Request at  Memo Header ID =  ' || pf_batch_id, 'log');
         RETURN (0);
      END IF;
   END po_submit_request;

   FUNCTION get_buyerid (userid NUMBER)
      RETURN NUMBER
   IS
      buyer_id   NUMBER;

      CURSOR cur_user
      IS
         SELECT /*+ CPU_COSTING  */
               buyer.employee_id buyer_id
           FROM fnd_user usert, po_buyers_all_v buyer
          WHERE buyer.employee_id = usert.employee_id AND usert.user_id = userid;
   BEGIN
      OPEN cur_user;

      FETCH cur_user INTO buyer_id;

      write_log ('User id : ' || userid || ' Buyer id : ' || buyer_id, 'log');

      CLOSE cur_user;

      RETURN (buyer_id);
   END get_buyerid;

   FUNCTION get_uom (v_item_id NUMBER)
      RETURN VARCHAR2
   IS
      v_uom   VARCHAR2 (10);
   BEGIN
      SELECT /*+ CPU_COSTING  */
            primary_uom_code
        INTO v_uom
        FROM mtl_system_items
       WHERE inventory_item_id = v_item_id;

      RETURN (v_uom);
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN NULL;
   END;

   FUNCTION get_grade (rep_id NUMBER)
      RETURN VARCHAR2
   IS
      v_grade   VARCHAR2 (10);
   BEGIN
      SELECT /*+ CPU_COSTING  */
            grade
        INTO v_grade
        FROM von_receipt
       WHERE receipt_id = rep_id;

      RETURN (v_grade);
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN (NULL);
   END;

   PROCEDURE get_location (v_inv_org IN NUMBER, loc_code OUT VARCHAR2, loc_id OUT NUMBER)
   IS
      v_location_code   VARCHAR2 (10);
   BEGIN
      SELECT /*+ CPU_COSTING  */
            location_code, location_id
        INTO loc_code, loc_id
        FROM hr_organization_units_v
       WHERE organization_id = v_inv_org;
   --  (SELECT inv_org_id
   --  FROM von_purchase_memo_hd
   -- WHERE memo_header_id = v_memo_hd);

   --  RETURN (v_location_code);
   EXCEPTION
      WHEN OTHERS
      THEN
         loc_code := NULL;
         loc_id := NULL;
   -- RETURN (NULL);
   END;

   FUNCTION check_memoid_in_po_headers (v_memo_id NUMBER)
      RETURN VARCHAR2
   IS
      flag_val   VARCHAR2 (1);
   BEGIN
      SELECT /*+ CPU_COSTING  */
            'Y'
        INTO flag_val
        FROM po_headers poh
       WHERE poh.attribute10 = v_memo_id AND poh.attribute11 = 'PORM';

      write_log ('Check memo id in po headers ' || flag_val, 'log');
      RETURN (flag_val);
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN ('N');
      -- pcn 100630 Many PO been Interfaced
      WHEN TOO_MANY_ROWS
      THEN
         RETURN ('Y');
      WHEN OTHERS
      THEN
         RETURN ('E');
   END;

   PROCEDURE clear_interface_table (p_id NUMBER, vstatus OUT VARCHAR2)
   AS
   --      CURSOR cur_pohi
   --      IS
   --         SELECT   *
   --           FROM   po_headers_interface
   --          WHERE   process_code <> 'ACCEPTED';
   BEGIN
      DELETE FROM /*+ CPU_COSTING  */
                 po_distributions_interface
            WHERE interface_header_id IN (SELECT interface_header_id
                                            FROM po_headers_interface
                                           WHERE batch_id = p_id);

      COMMIT;

      DELETE FROM /*+ CPU_COSTING  */
                 po_lines_interface
            WHERE interface_header_id IN (SELECT interface_header_id
                                            FROM po_headers_interface
                                           WHERE batch_id = p_id);

      COMMIT;

      DELETE FROM /*+ CPU_COSTING  */
                 po_headers_interface
            WHERE batch_id = p_id;

      vstatus := 'C';
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         vstatus := 'E';
   --
   --      FOR x IN cur_pohi
   --      LOOP
   --         BEGIN
   --            DELETE FROM   po_headers_interface
   --                  WHERE   interface_header_id = x.interface_header_id;
   --         EXCEPTION
   --            WHEN OTHERS
   --            THEN
   --               vstatus := 'E';
   --               GOTO end_clear;
   --         END;
   --
   --         BEGIN
   --            DELETE FROM   po_lines_interface
   --                  WHERE   interface_header_id = x.interface_header_id;
   --         EXCEPTION
   --            WHEN OTHERS
   --            THEN
   --               vstatus := 'E';
   --               GOTO end_clear;
   --         END;
   --
   --         BEGIN
   --            DELETE FROM   po_distributions_interface
   --                  WHERE   interface_header_id = x.interface_header_id;
   --         EXCEPTION
   --            WHEN OTHERS
   --            THEN
   --               vstatus := 'E';
   --               GOTO end_clear;
   --         END;

   --         vstatus := 'C';
   --      END LOOP;

   --     <<end_clear>>
   --      IF vstatus IS NULL
   --      THEN
   --         vstatus := 'C';
   --      END IF;
   END clear_interface_table;

   PROCEDURE main (errbuf       OUT VARCHAR2,
                   retcode      OUT VARCHAR2,
                   userid           NUMBER,
                   p_org_id         NUMBER,
                   p_batch_no       VARCHAR2 DEFAULT NULL,
                   p_memo_num       VARCHAR2,
                   p_memo_row       NUMBER)
   AS
      v_batch_id                 NUMBER (20);
      v_header_id                NUMBER;
      v_buyer_id                 NUMBER;
      v_line_id                  NUMBER;
      v_poline_id                NUMBER;
      v_sts                      BOOLEAN;
      v_request_id               NUMBER (20) := NULL;
      v_phase                    VARCHAR2 (200);
      v_status                   VARCHAR2 (200);
      v_dev_phase                VARCHAR2 (200);
      v_dev_status               VARCHAR2 (200);
      v_message                  VARCHAR2 (200);
      v_org_id                   NUMBER;
      v_uom                      VARCHAR2 (10);
      v_grade                    VARCHAR (10);
      v_line_location_id         NUMBER;
      v_location                 VARCHAR2 (100);
      v_distribution_id          NUMBER;
      v_destination_type_code    VARCHAR2 (100);
      v_destination_type         VARCHAR2 (100);
      v_clear_status             VARCHAR2 (1);
      v_dis_clear_status         VARCHAR2 (10);
      v_po_num                   NUMBER;
      v_line_num                 NUMBER;
      v_process_code_flg         VARCHAR2 (1);
      v_inferrmsg                VARCHAR2 (5000);
      v_total_rec                NUMBER DEFAULT 0;
      v_po_complete              NUMBER DEFAULT 0;
      v_po_error                 NUMBER DEFAULT 0;
      v_rcv_complete             NUMBER DEFAULT 0;
      v_rcv_error                NUMBER DEFAULT 0;
      v_po_status                VARCHAR2 (1);
      v_cns_return_status        VARCHAR2 (10000);
      v_max_wait                 NUMBER;

      CURSOR cur_hd
      IS
           SELECT /*+ CPU_COSTING  */
                  DISTINCT hd.memo_header_id,
                           hd.memo_number,
                           hd.memo_date,
                           hd.vendor_code,
                           po.vendor_name,
                           'PORM' interface_source_code,
                           'NEW' process_code,
                           'ORIGINAL' action,
                           'DEFAULT' group_code,
                           hd.ou_org_id org_id,
                           hd.inv_org_id,
                           hd.vendor_id vendor_id,
                           hd.vendor_site_id vendor_site_id,
                           hr.location_id ship_to_location_id,
                           hr.location_id bill_to_location_id,
                           'THB' currentcy_code,
                           'STANDARD' document_type_code,
                           'STANDARD' document_subtype,
                           hd.item_no,
                           hd.item_id,
                           hd.whse_code,
                           MIN (dt.po_status) po_status,
                           MIN (dt.rcv_status) rcv_status,
                           MIN (dt.ap_inv_status) ap_inv_status,
                           (SELECT grade_control_flag
                              FROM mtl_system_items_fvl
                             WHERE inventory_item_id = hd.item_id AND organization_id = hd.ou_org_id)
                              grade_control
             /*,
                                                                                            dt.RECEIPT_ID*/
             -- By pcn @ 100630
             FROM von_purchase_memo_hd hd,
                  von_purchase_memo_dt dt,
                  hr_organization_units_v hr,
                  po_vendors po
            WHERE     hd.memo_header_id = dt.memo_header_id
                  AND po.vendor_id = hd.vendor_id
                  AND hr.organization_id = hd.ou_org_id
                  AND hd.ou_org_id = p_org_id
                  AND (   NVL (dt.po_status, 'N') <> 'Y'
                       OR NVL (dt.rcv_status, 'N') <> 'Y'
                       OR NVL (dt.ap_inv_status, 'N') <> 'Y'
                       -- Begin Add for Support Consignment Return By pcn @ 100524
                       OR ( (SELECT DECODE (NVL (rec.consign_flag, 'N'), 'Y', NVL (dt.consign_status, 'E'), 'N')
                               FROM von_receipt rec
                              WHERE rec.receipt_id = dt.receipt_id)) NOT IN ('Y', 'N')                                                         -- End Add for Support Consignment Return By pcn @ 100524
                                                                                      )
                  --
                  -- AND (SELECT DECODE (NVL (rec.consign_flag, 'N'),
                  --                                            'Y', NVL (dt.consign_status, 'E'),
                  --                                            'N')
                  --                               FROM von_receipt rec
                  --                              WHERE rec.receipt_id = dt.receipt_id) NOT IN
                  --                             ('Y', 'N')

                  AND hd.memo_number LIKE (p_memo_num || '%')
                  AND hd.document_status IN (3020, 3030)
         GROUP BY hd.memo_header_id,
                  hd.memo_number,
                  hd.memo_date,
                  hd.vendor_code,
                  po.vendor_name,
                  hd.ou_org_id,
                  hd.inv_org_id,
                  hd.vendor_id,
                  hd.vendor_site_id,
                  hr.location_id,
                  hr.location_id,
                  hd.item_no,
                  hd.item_id,
                  hd.whse_code
         ORDER BY hd.memo_date, hd.memo_number;

      CURSOR cur_dt (headerid NUMBER)
      IS
         SELECT /*+ CPU_COSTING  */
               dt.memo_header_id,
                dt.memo_line_id,
                dt.memo_line_num line_num,
                dt.receipt_id,
                dt.memo_net_weight quantity,
                dt.memo_net_price unit_price,
                dt.memo_gross_weight secondary_quantity,
                receipt.grade,
                receipt.inv_org_id,
                receipt.sub_inventory,
                receipt.consign_flag,
                receipt.receipt_date,
                receipt.ou_org_id
           FROM von_purchase_memo_dt dt, von_receipt receipt
          WHERE receipt.receipt_id = dt.receipt_id AND dt.memo_header_id = headerid;

      --      CURSOR cur_error
      --      IS
      --         SELECT *
      --           FROM po_headers_interface
      --          WHERE TRUNC (creation_date) = TRUNC (SYSDATE);

      inf_row                    po_headers_interface%ROWTYPE;
      dreceipt_date              DATE;
      n_interface_seq            NUMBER;
      ntransaction_action_id     NUMBER;
      ndistribution_account_id   NUMBER;
      vsegment1                  VARCHAR2 (25);
      vsegment2                  VARCHAR2 (25);
      vsegment3                  VARCHAR2 (25);
      vsegment4                  VARCHAR2 (25);
      vsegment5                  VARCHAR2 (25);
      vsegment6                  VARCHAR2 (25);
      vsegment7                  VARCHAR2 (25);
      vsegment8                  VARCHAR2 (25);
      vdistribution_account_id   VARCHAR2 (100);
      ntransaction_type_id       NUMBER;
      l_lot_rec                  mtl_lot_numbers%ROWTYPE;

      o_msg_count                NUMBER;
      o_msg_data                 VARCHAR2 (200);
      o_return_status            VARCHAR2 (1);

      p_msg_count                NUMBER;
      p_msg_data                 VARCHAR2 (10000);
      p_return_status            VARCHAR2 (10000);
      v_batch                    VARCHAR2 (20);
      v_batch1                   VARCHAR2 (20);
      v_batch2                   VARCHAR2 (20);
      v_org_code                 VARCHAR2 (5);

      or_transaction_qty         NUMBER;
      or_second_trans_qty        NUMBER;
      or_lot_number              VARCHAR2 (30);
      nline_type_id              NUMBER;
      nrownum                    NUMBER := 1;
      nrownum_display            NUMBER;

      ninvoice_amount            NUMBER := 0;
      ntotal_invoice_amount      NUMBER := 0;
      ntotal_inv_amt             NUMBER;
      ngrand_total_inv_amt       NUMBER := 0;
      v_po_header_id             NUMBER;
   BEGIN
      -- get AP Batch Number 4 send to von_invoice procedure
      BEGIN
         SELECT /*+ CPU_COSTING  */
               SUBSTR (org.location_code, -2)
           INTO v_org_code
           FROM hr_organization_units_v org, hr_organization_information_v inf
          WHERE inf.organization_id = org.organization_id AND inf.org_information1_meaning = 'Operating Unit' AND org.organization_id = p_org_id;
      EXCEPTION
         WHEN OTHERS
         THEN
            write_log ('Others Error Batch xxx ' || CHR (13), 'log');
      END;


      --         SELECT   SUBSTR (batch_name, 1, 11), TO_NUMBER (SUBSTR (batch_name, -3)) + 1
      --           INTO   v_batch1, v_batch2
      --           FROM   ap_batches_all
      --          WHERE   batch_date = TO_DATE (TO_CHAR (SYSDATE, 'MM/DD/YYYY'), 'MM/DD/YYYY') AND SUBSTR (batch_name, 1, 3) = 'NRP-'                      ---;
      --                  AND TO_NUMBER (SUBSTR (batch_name, -3)) =
      --                        (SELECT   MAX (TO_NUMBER (SUBSTR (batch_name, -3)))
      --                           FROM   ap_batches_all
      --                          WHERE   batch_date = TO_DATE (TO_CHAR (SYSDATE, 'MM/DD/YYYY'), 'MM/DD/YYYY') AND SUBSTR (batch_name, 1, 3) = 'NRP');
      IF p_batch_no IS NULL
      THEN
         BEGIN
            SELECT /*+ CPU_COSTING  */
                  SUBSTR (batch_name, 1, 14), TO_NUMBER (SUBSTR (batch_name, -3)) + 1
              INTO v_batch1, v_batch2
              FROM ap_batches_all
             WHERE     batch_date = TO_DATE (TO_CHAR (SYSDATE, 'MM/DD/YYYY'), 'MM/DD/YYYY')
                   AND SUBSTR (batch_name, 1, 6) = 'NRP-' || v_org_code                                                                                                                             ---;
                   AND TO_NUMBER (SUBSTR (batch_name, -3)) = (SELECT MAX (TO_NUMBER (SUBSTR (batch_name, -3)))
                                                                FROM ap_batches_all
                                                               WHERE batch_date = TO_DATE (TO_CHAR (SYSDATE, 'MM/DD/YYYY'), 'MM/DD/YYYY') AND SUBSTR (batch_name, 1, 6) = 'NRP-' || v_org_code);

            v_batch := v_batch1 || LPAD (v_batch2, 3, '0');
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               v_batch := 'NRP-' || v_org_code || '-' || TO_CHAR (SYSDATE, 'YYMMDD') || '-001';
            WHEN OTHERS
            THEN
               v_batch := 'NRP-' || v_org_code || '-' || TO_CHAR (SYSDATE, 'YYMMDD') || '-001';
               write_log ('Others Error Batch Code ' || CHR (13), 'log');
         END;
      ELSE
         v_batch := p_batch_no;
      END IF;

      --      write_log ('########## Begin Program ##########' || CHR (13) || 'Clear Interface table : ' || v_dis_clear_status, 'log');              /*1 End*/
      write_log ('########## Begin Program ##########' || CHR (13), 'log');

      --      /*3 Start*/
      --      IF v_clear_status <> 'E'
      --      THEN
      v_buyer_id := get_buyerid (userid => userid);

      /*4 Start*/
      IF v_buyer_id IS NULL
      THEN
         write_log ('Please use buyer to run program.', 'LOG');
         GOTO end_now;
      END IF;                                                                                                                                                                                  /*4 End*/

      -- Limit to 10 Records -- By PCN @100630
      BEGIN
         nrownum_display := NVL (p_memo_row, count_total_memo (p_org_id => p_org_id, p_memo_num => NVL (p_memo_num, 11)));
      EXCEPTION
         WHEN OTHERS
         THEN
            IF p_memo_num IS NULL
            THEN
               nrownum_display := 5;
            ELSE
               nrownum_display := 2;
            END IF;
      END;

      write_log ('Total Record to be Interface' || nrownum_display, 'line');

      FOR one IN cur_hd
      LOOP
         EXIT WHEN NOT (nrownum <= NVL (p_memo_row, 10)) AND NOT (nrownum <= 10);

         /*2 End if*/

         write_log ('########## Begin Program  PO Interface- ' || CHR (13), 'log');

         v_total_rec := v_total_rec + 1;
         write_log (CHR (13) || v_total_rec || '. Memo number : ' || one.memo_number || ' [ ' || one.vendor_code || ' ' || one.vendor_name || ' ]. ', 'line');
         write_log (v_total_rec || '. Memo number : ' || one.memo_number || ' [ ' || one.vendor_code || ' ' || one.vendor_name || ' ] ', 'log');

         -- *********** npp
         -- Checkg Memo Date
         IF apps.inv_txn_manager_pub.get_open_period (one.inv_org_id, one.memo_date, 1) = 0 OR apps.inv_txn_manager_pub.get_open_period (one.inv_org_id, one.memo_date, 1) IS NULL
         THEN
            -- Peroid Memo Date is Close
            IF apps.inv_txn_manager_pub.get_open_period (one.inv_org_id, SYSDATE, 1) = 0 OR apps.inv_txn_manager_pub.get_open_period (one.inv_org_id, SYSDATE, 1) IS NULL
            THEN
               ---rcv_error := rcv_error + 1;
               ---x_return_status := 'Receipt Date and Current Date not in Open Period';
               apps.von_pormtopo.write_log ('Receipt Date and Current Date not in Open Period.', 'log');
               GOTO end_one_loop;
            END IF;
         END IF;

         --// ********** npp

         /*5 Check data exist in po_headers with attribute10*/
         IF NVL (one.po_status, 'N') <> 'Y'
         THEN
            write_log ('PO_STATUS not equal ''Y'' and Memo Header ID = ' || one.memo_header_id, 'log');
            v_batch_id := one.memo_header_id;
            write_log ('Interface batch id is ' || v_batch_id, 'LOG');


            -- Get location
            get_location (v_inv_org => one.inv_org_id, loc_code => v_location, loc_id => v_line_location_id);

            IF check_memoid_in_po_headers (v_memo_id => one.memo_header_id) = 'N'
            THEN
               /*1 Clear interface table*/
               clear_interface_table (p_id => one.memo_header_id, vstatus => v_clear_status);
               write_log ('########## HEADER ##########', 'LOG');
               write_log ('Memo header id' || one.memo_header_id, 'log');

               /*PO header interface sequence */
               SELECT po_headers_interface_s.NEXTVAL INTO v_header_id FROM DUAL;

               write_log ('Interface header id is ' || v_header_id, 'LOG');

               /*Batch id is memo header id*/


               /*Insert PO_HEADERS_INTERFACE*/
               INSERT INTO /*+ CPU_COSTING  */
                          po_headers_interface (batch_id,
                                                interface_header_id,
                                                action,
                                                org_id,
                                                document_type_code,
                                                vendor_id,
                                                vendor_site_id,
                                                effective_date,
                                                agent_id,
                                                comments,
                                                attribute10,
                                                attribute11,
                                                ship_to_location_id,
                                                creation_date,
                                                created_by,
                                                last_update_date,
                                                last_updated_by,
                                                last_update_login)
                    VALUES (v_batch_id,
                            v_header_id,
                            one.action,
                            one.org_id,
                            one.document_type_code,
                            one.vendor_id,
                            one.vendor_site_id,
                            SYSDATE,
                            v_buyer_id,
                            one.memo_number,
                            v_batch_id,
                            'PORM',
                            v_line_location_id,
                            SYSDATE,
                            userid,
                            SYSDATE,
                            userid,
                            userid);
            ELSE
               v_po_header_id := NULL;

               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        po_header_id
                    INTO v_po_header_id
                    FROM po_headers poh
                   WHERE poh.attribute10 = v_batch_id AND poh.attribute11 = 'PORM';

                  SELECT po_interface_header_id
                    INTO v_header_id
                    FROM von_purchase_memo_hd
                   WHERE memo_header_id = v_batch_id;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     v_po_header_id := NULL;
                     v_header_id := NULL;
               END;

               IF v_header_id IS NULL
               THEN
                  BEGIN
                     SELECT interface_header_id
                       INTO v_header_id
                       FROM po_headers_interface
                      WHERE attribute10 = v_batch_id AND attribute11 = 'PORM';
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        SELECT po_headers_interface_s.NEXTVAL INTO v_header_id FROM DUAL;
                  END;
               END IF;

               /*1 Clear interface table*/
               clear_interface_table (p_id => one.memo_header_id, vstatus => v_clear_status);
            END IF;                                                                                                                                                            -- New Exitsing PO Header



            /*V_ORG_ID Use for update org_id in Standard concurrent "Import Standard Purchase Orders" */
            v_org_id := one.org_id;

            FOR two IN cur_dt (one.memo_header_id)
            LOOP
               write_log ('########## DETAIL ##########', 'LOG');

               -- *********** npp
               -- Checkg Receipt Date
               IF apps.inv_txn_manager_pub.get_open_period (two.inv_org_id, two.receipt_date, 1) = 0 OR apps.inv_txn_manager_pub.get_open_period (two.inv_org_id, two.receipt_date, 1) IS NULL
               THEN
                  -- Peroid Receipt Date is Close
                  IF apps.inv_txn_manager_pub.get_open_period (two.inv_org_id, SYSDATE, 1) = 0 OR apps.inv_txn_manager_pub.get_open_period (two.inv_org_id, SYSDATE, 1) IS NULL
                  THEN
                     ---rcv_error := rcv_error + 1;
                     ---x_return_status := 'Receipt Date and Current Date not in Open Period';
                     apps.von_pormtopo.write_log ('Receipt Date and Current Date not in Open Period.', 'log');
                     GOTO end_two_loop;
                  ELSE
                     dreceipt_date := SYSDATE;
                  END IF;
               ELSE
                  -- Period Reciept Date is Open
                  dreceipt_date := two.receipt_date;
               END IF;

               -- // ********** npp

               /*PO line interface sequence*/
               SELECT /*+ CPU_COSTING  */
                     po_lines_interface_s.NEXTVAL INTO v_line_id FROM DUAL;

               write_log ('Interface Line id is ' || v_line_id, 'LOG');

               /*po line sequence*/
               SELECT /*+ CPU_COSTING  */
                     po_lines_s.NEXTVAL INTO v_poline_id FROM DUAL;

               write_log ('PO line id is ' || v_poline_id, 'LOG');
               /*UOM*/
               v_uom := get_uom (v_item_id => one.item_id);

               /*Grade*/
               -- Move to use two.receipt_id by PCN
               IF NVL (one.grade_control, 'N') = 'Y'
               THEN
                  v_grade := get_grade (rep_id => two.receipt_id);
               ELSE
                  v_grade := NULL;
               END IF;

               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        line_type_id
                    INTO nline_type_id
                    FROM po_line_types_val_v
                   WHERE line_type = 'Raw Material';
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     nline_type_id := 0;
               END;

               /*Insert PO_LINES_INTERFACE*/
               IF NVL (two.consign_flag, 'N') IN ('Y', 'E')
               THEN
                  INSERT INTO /*+ CPU_COSTING  */
                             po_lines_interface (interface_line_id,
                                                 interface_header_id,
                                                 line_num,
                                                 shipment_num,
                                                 ship_to_location_id,
                                                 ship_to_location,
                                                 ship_to_organization_id,
                                                 ship_to_organization_code,
                                                 effective_date,
                                                 expiration_date,
                                                 item,
                                                 item_id,
                                                 uom_code,
                                                 unit_price,
                                                 quantity,
                                                 secondary_quantity,
                                                 promised_date,
                                                 need_by_date,
                                                 note_to_vendor,
                                                 line_attribute10,
                                                 line_attribute11,
                                                 preferred_grade,
                                                 last_update_date,
                                                 last_updated_by,
                                                 last_update_login,
                                                 creation_date,
                                                 created_by,
                                                 line_type,
                                                 line_type_id                                                                                                                                        --,
                                                             --                                                        SHIP_TO_LOCATION_ID
                                                 ,
                                                 po_header_id)
                       VALUES (v_line_id,
                               v_header_id,
                               two.line_num,
                               NULL,
                               v_line_location_id,
                               v_location,
                               two.inv_org_id,
                               one.whse_code,
                               NULL,
                               NULL,
                               one.item_no,
                               one.item_id,
                               v_uom,
                               two.unit_price,
                               two.quantity,
                               two.secondary_quantity,
                               one.memo_date,
                               one.memo_date,
                               one.memo_number,
                               two.memo_line_id,
                               'PORM',
                               v_grade,
                               SYSDATE,
                               userid,
                               userid,
                               SYSDATE,
                               userid,
                               'Raw Material',
                               nline_type_id                                                                                                                                                         --,
                                            --                                    v_line_location_id
                               ,
                               v_po_header_id);
               ELSE
                  INSERT INTO /*+ CPU_COSTING  */
                             po_lines_interface (interface_line_id,
                                                 interface_header_id,
                                                 line_num,
                                                 shipment_num,
                                                 ship_to_location_id,
                                                 ship_to_location,
                                                 ship_to_organization_id,
                                                 ship_to_organization_code,
                                                 effective_date,
                                                 expiration_date,
                                                 item,
                                                 item_id,
                                                 uom_code,
                                                 unit_price,
                                                 quantity,
                                                 secondary_quantity,
                                                 promised_date,
                                                 need_by_date,
                                                 note_to_vendor,
                                                 line_attribute10,
                                                 line_attribute11,
                                                 preferred_grade,
                                                 last_update_date,
                                                 last_updated_by,
                                                 last_update_login,
                                                 creation_date,
                                                 created_by,
                                                 line_type,
                                                 line_type_id,
                                                 po_header_id)
                       VALUES (v_line_id,
                               v_header_id,
                               two.line_num,
                               NULL,
                               v_line_location_id,
                               v_location,
                               two.inv_org_id,
                               one.whse_code,
                               NULL,
                               NULL,
                               one.item_no,
                               one.item_id,
                               v_uom,
                               two.unit_price,
                               two.quantity,
                               two.secondary_quantity,
                               one.memo_date,
                               one.memo_date,
                               one.memo_number,
                               two.memo_line_id,
                               'PORM',
                               v_grade,
                               SYSDATE,
                               userid,
                               userid,
                               SYSDATE,
                               userid,
                               'Raw Material',
                               nline_type_id,
                               v_po_header_id);
               END IF;



               SELECT /*+ CPU_COSTING  */
                     po_distributions_interface_s.NEXTVAL INTO v_distribution_id FROM DUAL;

               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        lookup_code, meaning
                    INTO v_destination_type_code, v_destination_type
                    FROM fnd_lookup_values
                   WHERE lookup_type = 'DESTINATION TYPE' AND lookup_code = 'INVENTORY';
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_destination_type_code := NULL;
                     v_destination_type := NULL;
               END;

               INSERT INTO /*+ CPU_COSTING  */
                          po_distributions_interface (interface_line_id,
                                                      interface_header_id,
                                                      interface_distribution_id,
                                                      distribution_num,
                                                      org_id,
                                                      quantity_ordered,
                                                      destination_type,
                                                      destination_type_code,
                                                      destination_organization_id,
                                                      destination_subinventory)
                    VALUES (v_line_id,
                            v_header_id,
                            v_distribution_id,
                            1,
                            v_org_id,
                            two.quantity,
                            v_destination_type,
                            v_destination_type_code,
                            two.inv_org_id,
                            two.sub_inventory);

               -- update interface_line_id, interface_header_id, to Memo Line
               -- pcn @ 100630
               UPDATE von_purchase_memo_dt dt
                  SET dt.po_interface_header_id = v_line_id, dt.po_interface_line_id = v_header_id
                WHERE dt.memo_line_id = two.memo_line_id;
            END LOOP two;

           <<end_two_loop>>
            apps.von_pormtopo.write_log (CHR (13), 'log');
            --END IF;  -- pcn @ 100630 Exception Interface HD                                                                                                                     /*5 End*/

            write_log ('Org id : ' || one.org_id || CHR (13) || 'User id : ' || userid, 'log');
            /*Submit Standard concurrent "Import Standard Purchase Orders"*/
            v_max_wait := 0;
            v_request_id := po_submit_request (pf_batch_id => v_batch_id, v_user_id => userid, pf_org_id => v_org_id);
            COMMIT;



            /*Wait For Request 1*/
            IF (v_request_id != 0)
            THEN
              /*Wait For Request 2*/
              <<wait_req>>
               IF fnd_concurrent.wait_for_request (v_request_id,
                                                   10,
                                                   1800,
                                                   v_phase,
                                                   v_status,
                                                   v_dev_phase,
                                                   v_dev_status,
                                                   v_message)
               THEN
                  write_log (v_dev_status || ' : ' || v_message, 'LOG');

                  /*Check Concurrent status 1*/
                  IF v_dev_phase != 'COMPLETE' OR v_dev_status != 'NORMAL'
                  THEN
                     IF v_max_wait >= 3
                     THEN
                        v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
                        v_process_code_flg := 'N';
                     ELSE
                        v_max_wait := v_max_wait + 1;
                        GOTO wait_req;
                     END IF;
                  ELSE
                     /*Update memo status 'INTERFACE' 1*/
                     BEGIN
                        SELECT /*+ CPU_COSTING  */
                              'Y'
                          INTO v_process_code_flg
                          FROM po_headers_interface hd_inf
                         WHERE hd_inf.process_code = 'ACCEPTED' AND hd_inf.attribute10 = v_batch_id AND hd_inf.attribute11 = 'PORM';
                     EXCEPTION
                        WHEN NO_DATA_FOUND
                        THEN
                           -- Check All Interface Line
                           -- By PCN @ 100630
                           BEGIN
                              SELECT /*+ CPU_COSTING  */
                                    'N'
                                INTO v_process_code_flg
                                FROM po_lines_interface line_inf
                               WHERE     line_inf.process_code <> 'ACCEPTED'
                                     AND line_inf.line_attribute10 IN (SELECT hd_inf.memo_line_id
                                                                         FROM von_purchase_memo_dt hd_inf
                                                                        WHERE hd_inf.memo_header_id = v_batch_id);
                           EXCEPTION
                              WHEN NO_DATA_FOUND
                              THEN
                                 BEGIN
                                    SELECT /*+ CPU_COSTING  */
                                          'Y'
                                      INTO v_process_code_flg
                                      FROM po_lines_interface line_inf
                                     WHERE     line_inf.process_code = 'ACCEPTED'
                                           AND line_inf.line_attribute10 IN (SELECT hd_inf.memo_line_id
                                                                               FROM von_purchase_memo_dt hd_inf
                                                                              WHERE hd_inf.memo_header_id = v_batch_id);
                                 EXCEPTION
                                    WHEN TOO_MANY_ROWS
                                    THEN
                                       v_process_code_flg := 'Y';
                                    WHEN NO_DATA_FOUND
                                    THEN
                                       v_process_code_flg := 'N';
                                 END;
                              WHEN TOO_MANY_ROWS
                              THEN
                                 v_process_code_flg := 'N';
                           END;
                     END;

                     v_inferrmsg := NULL;

                     /*Check Process code flag 1*/
                     IF v_process_code_flg = 'Y'
                     THEN
                        BEGIN
                           write_log ('Update interface status to ' || one.memo_number, 'log');

                           UPDATE /*+ CPU_COSTING  */
                                 von_purchase_memo_hd
                              SET document_status = 3030
                            WHERE memo_header_id = one.memo_header_id;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              write_log ('Can not update table von_purchase_memo_hd' || CHR (13) || SQLERRM, 'Log');
                              ROLLBACK;
                        END;
                     ELSE
                        BEGIN
                           SELECT 'PO Interface_Transaction_Id => ' || interface_transaction_id || ' Column => ' || column_name || CHR (13) || 'Table => ' || table_name || CHR (13) || error_message
                             INTO v_inferrmsg
                             FROM po_interface_errors
                            WHERE interface_header_id = (SELECT hd_inf.interface_header_id
                                                           FROM po_headers_interface hd_inf
                                                          WHERE hd_inf.attribute10 = v_batch_id AND hd_inf.attribute11 = 'PORM');
                        EXCEPTION
                           WHEN TOO_MANY_ROWS
                           THEN
                              FOR z IN (SELECT *
                                          /*+ CPU_COSTING  */
                                          FROM po_interface_errors
                                         WHERE interface_header_id = (SELECT hd_inf.interface_header_id
                                                                        FROM po_headers_interface hd_inf
                                                                       WHERE hd_inf.attribute10 = v_batch_id AND hd_inf.attribute11 = 'PORM'))
                              LOOP
                                 v_inferrmsg :=
                                       v_inferrmsg
                                    || CHR (10)
                                    || 'PO Interface_Transaction_Id => '
                                    || z.interface_transaction_id
                                    || ' Column => '
                                    || z.column_name
                                    || CHR (13)
                                    || 'Table => '
                                    || z.table_name
                                    || CHR (13)
                                    || z.error_message;
                              END LOOP;
                           WHEN NO_DATA_FOUND
                           THEN
                              -- Add Case  Getting Error from PO_LINE_INTERFACE
                              -- By PCN @ 100630
                              FOR i IN (SELECT *
                                          /*+ CPU_COSTING  */
                                          FROM po_interface_errors
                                         WHERE interface_line_id IN (SELECT hd_inf.memo_line_id
                                                                       FROM von_purchase_memo_dt hd_inf
                                                                      WHERE hd_inf.memo_header_id = v_batch_id))
                              LOOP
                                 v_inferrmsg :=
                                       v_inferrmsg
                                    || CHR (10)
                                    || 'PO Interface_Transaction_Id => '
                                    || i.interface_transaction_id
                                    || ' Column => '
                                    || i.column_name
                                    || CHR (13)
                                    || 'Table => '
                                    || i.table_name
                                    || CHR (13)
                                    || i.error_message;
                              END LOOP;

                              IF v_inferrmsg IS NULL
                              THEN
                                 write_log ('Error But Can not find error message in PO_INTERFACE_ERRORS', 'log');
                              END IF;
                        END;

                        write_log ('PO Interface Log: ', 'log');
                        write_log (v_inferrmsg, 'log');
                     END IF;                                                                                                                                           /*Check Process code flag 1 end*/
                  END IF;                                                                                                                                              /*Check Concurrent status 1 end*/
               END IF;                                                                                                                                                        /*Wait For Request 2 end*/
            END IF;                                                                                                                                                           /*Wait For Request 1 end*/

            /*########## Update flag in von_purchase_memo_dt ###########*/
            FOR x IN (SELECT /*+ CPU_COSTING  */
                            dt.*
                        FROM von_purchase_memo_dt dt
                       WHERE dt.memo_header_id = one.memo_header_id AND NVL (dt.po_status, 'N') <> 'Y')
            LOOP
               IF v_po_num IS NOT NULL
               THEN
                  BEGIN
                     UPDATE /*+ CPU_COSTING  */
                           von_purchase_memo_hd
                        SET po_number = v_po_num, po_interface_header_id = v_header_id
                      WHERE memo_header_id = x.memo_header_id;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        write_log ('can not update po number to ' || one.memo_number, 'log');
                  END;
               END IF;

               FOR y IN (SELECT /*+ CPU_COSTING  */
                               *
                           FROM po_headers_interface
                          WHERE     batch_id = x.memo_header_id
                                AND interface_header_id = (SELECT MAX (interface_header_id)
                                                             FROM po_headers_interface
                                                            WHERE batch_id = x.memo_header_id))
               LOOP
                  IF y.process_code = 'ACCEPTED'
                  THEN
                     write_log ('PO Process code: ' || y.process_code, 'log');

                     BEGIN
                        SELECT /*+ CPU_COSTING  */
                              segment1
                          INTO v_po_num
                          FROM po_headers_all
                         WHERE po_header_id = y.po_header_id;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           v_po_num := NULL;
                     END;

                     write_log ('PO Number รอบ 2 : ' || v_po_num, 'log');

                     BEGIN
                        SELECT /*+ CPU_COSTING  */
                              line_num
                          INTO v_line_num
                          FROM po_lines_all
                         WHERE attribute10 = x.memo_line_id AND po_header_id = y.po_header_id;                                                                                          -- by PCN 100630
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           v_line_num := NULL;
                     END;

                     IF v_po_num IS NOT NULL AND v_line_num IS NOT NULL
                     THEN
                        BEGIN
                           UPDATE /*+ CPU_COSTING  */
                                 von_purchase_memo_dt
                              SET po_status = 'Y', po_number = v_po_num, po_line_num = v_line_num
                            WHERE memo_line_id = x.memo_line_id;

                           IF v_po_status IS NOT NULL
                           THEN
                              IF v_po_status <> 'Y'
                              THEN
                                 v_po_status := 'N';
                              END IF;
                           ELSE
                              v_po_status := 'Y';
                           END IF;

                           v_po_complete := v_po_complete + 1;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              write_log ('Can not update ''Y'' to memo line id ' || x.memo_line_id || ' memo number ' || one.memo_number, 'log');
                        END;
                     END IF;
                  ELSIF y.process_code <> 'ACCEPTED'
                  THEN
                     write_log ('PO Process code: ' || y.process_code, 'log');

                     BEGIN
                        UPDATE /*+ CPU_COSTING  */
                              von_purchase_memo_dt
                           SET po_status = 'N'
                         WHERE memo_line_id = x.memo_line_id;

                        v_po_status := 'N';
                        write_log ('PO Error.', 'line');
                        v_po_error := v_po_error + 1;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           write_log ('Can not update PO status memo number ' || one.memo_number, 'log');
                     END;
                  END IF;

                  write_log (' PO number : ' || v_po_num, 'log');
               END LOOP y;

               IF v_po_num IS NOT NULL AND v_po_status = 'Y'
               THEN
                  write_log ('PO Complete : ' || v_po_num, 'line');
               END IF;
            END LOOP x;

            COMMIT;
         END IF;                                                                                                                                                                           /*PO STATUS*/

         /*########## Run receive interface ###########*/

         IF NVL (one.rcv_status, 'N') <> 'Y'
         THEN                                                                                                                                                                             /*RCV STATUS*/
            --write_log (CHR (13) || '########## Run receive interface ###########' || CHR (13), 'log');

            FOR rcv IN (SELECT /*+ CPU_COSTING  */
                               DISTINCT dt.po_number
                          FROM von_purchase_memo_dt dt
                         WHERE dt.memo_header_id = one.memo_header_id AND NVL (dt.po_status, 'E') = 'Y' AND NVL (dt.rcv_status, 'N') <> 'Y')
            LOOP
               BEGIN
                  write_log ('Org id : ' || one.org_id                                                                                                                                   --rcv.ou_org_id
                                                      || ', User id : ' || userid || ', PO number : ' || rcv.po_number, 'log');
                  apps.von_pormtopo.receive_po (x_org_id             => one.org_id,
                                                userid               => userid,
                                                x_ponum              => rcv.po_number,
                                                rcv_complete         => v_rcv_complete,
                                                rcv_error            => v_rcv_error,
                                                o_lot_number         => or_lot_number,
                                                o_transaction_qty    => or_transaction_qty,
                                                o_second_trans_qty   => or_second_trans_qty);
                  von_pormtopo.write_log (' End RCV: ', 'log');
               END;
            -- <<end_rcvi>>
            --  NULL;
            END LOOP rcv;
         END IF;                                                                                                                                                                          /*RCV STATUS*/

         /*########## Check Consign  Return ###########*/

         --write_log (CHR (13) || '########## Run Consign Return interface ###########' || CHR (13), 'log');

         ---xxxx
         FOR consign IN (SELECT /*+ CPU_COSTING  */
                                DISTINCT dt.memo_line_id, dt.receipt_id
                           FROM von_purchase_memo_dt dt
                          WHERE     dt.memo_header_id = one.memo_header_id
                                AND NVL (dt.po_status, 'E') = 'Y'
                                AND NVL (dt.rcv_status, 'E') = 'Y'
                                AND ( (SELECT DECODE (NVL (rec.consign_return, 'N'), 'Y', 'Y', NVL (dt.consign_status, 'E'))
                                         FROM von_receipt rec
                                        WHERE rec.receipt_id = dt.receipt_id)) NOT IN ('Y', 'N')                                                                                  -- fIX BUG @ 29 Jul 10
                                AND ( (SELECT DECODE (NVL (rec.consign_flag, 'N'), 'Y', NVL (dt.consign_status, 'E'), 'N')
                                         FROM von_receipt rec
                                        WHERE rec.receipt_id = dt.receipt_id)) NOT IN ('Y', 'N'))
         LOOP
            BEGIN
               write_log ('Org id : ' || one.org_id                                                                                                                                      --rcv.ou_org_id
                                                   || ', User id : ' || userid || ', Receipt ID : ' || consign.receipt_id, 'log');
               apps.von_pormtopo.consign_return_memo (x_org_id             => one.org_id,
                                                      x_userid             => userid,
                                                      x_memo_line_id       => consign.memo_line_id,
                                                      x_receipt_id         => consign.receipt_id,
                                                      x_lot_number         => or_lot_number,
                                                      x_transaction_qty    => or_transaction_qty,
                                                      x_second_trans_qty   => or_transaction_qty,
                                                      rcv_complete         => v_rcv_complete,
                                                      rcv_error            => v_rcv_error,
                                                      x_return_status      => v_cns_return_status);
               von_pormtopo.write_log (' End Consign Return : result=' || v_cns_return_status, 'log');
            END;
         END LOOP consign;

         IF g_cns_status_result IS NOT NULL
         THEN
            apps.von_pormtopo.write_log (g_cns_status_result, 'line');
         END IF;

         /*########## Interface AP Invoice ###########*/
         IF NVL (one.ap_inv_status, 'N') <> 'Y' AND get_po_status (one.memo_header_id) = 'Y' AND get_rcv_status (one.memo_header_id) = 'Y'
         THEN
            apps.von_pormtopo.von_invoices (errbuf          => errbuf,
                                            retcode         => retcode,
                                            userid          => userid,
                                            p_login_id      => fnd_profile.VALUE ('LOGIN_ID'),
                                            memo_num        => one.memo_number,
                                            batch_num       => v_batch,
                                            total_inv_amt   => ntotal_inv_amt);
            ngrand_total_inv_amt := ngrand_total_inv_amt + ntotal_inv_amt;
         END IF;

         /* 3 End */

         nrownum := nrownum + 1;

        <<end_one_loop>>
         NULL;
      END LOOP one;

      apps.von_pormtopo.write_log ('--------------------------------------------------------------------------------------------- Total ' || TO_CHAR (ngrand_total_inv_amt, '999,999,999.99'), 'line');

     <<end_now>>
      COMMIT;

     --      END IF;                                                                                                                                /*3 End*/

     <<end_main>>
      NULL;
   -- write_log ( 'Total Record : '
   -- || v_total_rec
   -- || CHR (13)
   -- || 'Purchase order'
   -- || CHR (13)
   -- || '     Complete '
   -- || v_po_complete
   -- || CHR (13)
   -- || '     Error '
   -- || v_po_error
   -- || CHR (13)
   -- || 'Receive'
   -- || CHR (13)
   -- || '     Complete '
   -- || v_rcv_complete
   -- || CHR (13)
   -- || '     Error '
   -- || v_rcv_error,
   -- 'line'
   -- );
   END main;


   PROCEDURE receive_po (x_org_id             IN     NUMBER,
                         userid               IN     NUMBER,
                         x_ponum              IN     VARCHAR2,
                         rcv_complete            OUT NUMBER,
                         rcv_error               OUT NUMBER,
                         o_lot_number            OUT VARCHAR2,
                         o_transaction_qty       OUT NUMBER,
                         o_second_trans_qty      OUT NUMBER)
   AS
      v_agent_id                       NUMBER;
      v_sts                            BOOLEAN;
      v_request_id                     NUMBER (20) := NULL;
      v_phase                          VARCHAR2 (200);
      v_status                         VARCHAR2 (200);
      v_dev_phase                      VARCHAR2 (200);
      v_dev_status                     VARCHAR2 (200);
      v_message                        VARCHAR2 (200);
      v_req_id                         NUMBER (20) := NULL;
      v_header_interface_id            NUMBER;
      v_group_id                       NUMBER;
      v_chk_interface                  NUMBER;
      v_chk_transaction                NUMBER;
      v_chk_interface_transaction_id   NUMBER;
      v_final_chk_all                  NUMBER;
      v_final_chk_y                    NUMBER;
      v_process_status                 VARCHAR2 (20);
      v_flag                           VARCHAR2 (1);
      v_lot_no                         VARCHAR2 (30);
      v_grade                          VARCHAR2 (5);
      v_rcv_number                     von_purchase_memo_hd.rcv_number%TYPE;
      v_rcv_status                     VARCHAR2 (1);
      v_inferrmsg                      VARCHAR2 (2000);
      v_del_hd_total                   NUMBER;
      v_del_hd_n_flg                   NUMBER;
      v_del_hd_meaning                 VARCHAR2 (1);
      v_sob_id                         NUMBER;

      CURSOR cur_po
      IS
           SELECT /*+ CPU_COSTING  */
                 a.po_number,
                  a.po_header_id,
                  a.vendor_id,
                  a.vendor_site_id,
                  a.expected_receipt_date,
                  a.po_line_id,
                  a.po_line_number,
                  a.po_line_location_id,
                  a.po_release_id,
                  (SELECT segment1
                     FROM mtl_system_items
                    WHERE inventory_item_id = a.item_id AND organization_id = a.to_organization_id)
                     item_number,
                  (SELECT lot_control_code
                     FROM mtl_system_items_fvl
                    WHERE inventory_item_id = a.item_id AND organization_id = a.to_organization_id)
                     lot_control,
                  a.item_id,
                  a.to_organization_id,
                  a.source_type_code,
                  a.source,
                  a.primary_uom uom,
                  a.deliver_to_location_id,
                  a.rcv_shipment_header_id,
                  a.rcv_shipment_number,
                  a.rcv_shipment_line_id,
                  a.rcv_line_number,
                  a.po_release_number,
                  a.po_shipment_number,
                  a.ship_to_location,
                  a.ship_to_location_id,
                  a.order_type,
                  a.destination_subinventory sub_inventory,
                  a.secondary_ordered_qty,
                  a.secondary_ordered_uom,
                  (pov.quantity - (pov.quantity_received - pov.quantity_billed) - pov.quantity_cancelled - pov.quantity_billed) remainding_qty
             FROM rcv_enter_receipts_supplier_v a, po_line_locations pov
            WHERE     receipt_source_code IN ('INVENTORY', 'INTERNAL ORDER', 'VENDOR')
                  AND ( (a.source_type_code = 'VENDOR' OR a.source_type_code = 'ASN' OR a.source_type_code = 'INTERNAL'))
                  AND ( (NVL (a.closed_code, 'OPEN') NOT IN ('CLOSED', 'CLOSED FOR RECEIVING')))
                  AND pov.line_location_id = a.po_line_location_id
                  AND a.po_number LIKE RTRIM (LTRIM (x_ponum)) || '%'
                  AND (pov.quantity - (pov.quantity_received - pov.quantity_billed) - pov.quantity_cancelled - pov.quantity_billed) > 0
         ORDER BY a.expected_receipt_date,
                  a.need_by_date,
                  a.po_line_location_id,
                  a.rcv_line_number;

      v_line_memo_id                   NUMBER;

      CURSOR cur_memo (line_memo_id NUMBER)
      IS
         SELECT /*+ CPU_COSTING  */
               dt.rcv_no,
                dt.inv_transaction_interface_id,
                dt.receipt_id,
                dt.rcv_status,
                dt.rcv_interface_groups_id,
                dt.rcv_headers_interface_id,
                dt.rcv_interface_transaction_id,
                dt.memo_line_id,
                hd.vendor_code,
                hd.whse_code,
                hd.memo_number || '-' || dt.memo_line_num acomment,
                hd.memo_date,
                hd.memo_header_id,
                hd.inv_org_id,
                rc.receipt_date
           FROM von_purchase_memo_dt dt, von_purchase_memo_hd hd, von_receipt rc
          WHERE dt.memo_line_id = line_memo_id AND dt.memo_header_id = hd.memo_header_id AND dt.receipt_id = rc.receipt_id;

      next_rec_num                     NUMBER;
      v_next_rec_num                   NUMBER;
      chk_ship_hd                      NUMBER;
      chk_ship_hd1                     NUMBER;
      rec_num                          NUMBER;
      ship_hd                          NUMBER;
      v_locator                        VARCHAR2 (81);
      v_memo_header_id                 NUMBER;
      dreceipt_date                    DATE;
      v_rcv_headers_inf                rcv_headers_interface%ROWTYPE;
   BEGIN
      rcv_complete := 0;
      rcv_error := 0;
      apps.von_pormtopo.write_log ('##########Start Receive Interface########', 'log');
      mo_global.set_policy_context ('S', x_org_id);

      FOR one IN cur_po
      LOOP
         --header information in variables
         apps.von_pormtopo.write_log ('Entering Receiving Insert Procedure', 'log');
         apps.von_pormtopo.write_log ('Po number =' || x_ponum || '$', 'log');

         BEGIN
            SELECT /*+ CPU_COSTING  */
                  attribute10
              INTO v_line_memo_id
              FROM po_lines
             WHERE po_header_id = one.po_header_id AND po_line_id = one.po_line_id;

            apps.von_pormtopo.write_log ('PO header id ' || one.po_header_id || CHR (13) || 'PO line id ' || one.po_line_id, 'log');
            apps.von_pormtopo.write_log ('Line memo id = ' || v_line_memo_id, 'log');
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               apps.von_pormtopo.write_log ('No memo line id.', 'log');
               GOTO end_one_loop;
            WHEN TOO_MANY_ROWS
            THEN
               apps.von_pormtopo.write_log ('Check po_lines_all where po_header_id=' || one.po_header_id || ' and PO_LINE_ID=' || one.po_line_id || '.', 'log');
               GOTO end_one_loop;
            WHEN OTHERS
            THEN
               apps.von_pormtopo.write_log (SQLERRM, 'log');
               GOTO end_one_loop;
         END;

         FOR memo IN cur_memo (v_line_memo_id)
         LOOP
            apps.von_pormtopo.write_log (
                  memo.rcv_no
               || ','
               || memo.inv_transaction_interface_id
               || ','
               || memo.receipt_id
               || ','
               || memo.rcv_status
               || ','
               || memo.rcv_interface_groups_id
               || ','
               || memo.rcv_headers_interface_id
               || ','
               || memo.rcv_interface_transaction_id
               || ','
               || memo.memo_line_id
               || ','
               || memo.vendor_code
               || ','
               || memo.whse_code
               || ','
               || memo.acomment
               || ','
               || memo.memo_date
               || ','
               || memo.memo_header_id,
               'log');
            next_rec_num := memo.rcv_no;
            v_memo_header_id := memo.memo_header_id;
            apps.von_pormtopo.write_log ('<B>RECEIVE NUMBER : </B>' || next_rec_num || CHR (13) || 'memo header id : ' || v_memo_header_id, 'log');
            apps.von_pormtopo.write_log ('Transaction interface id => ' || memo.inv_transaction_interface_id, 'log');

            SELECT /*+ CPU_COSTING  */
                  n_flg, total, DECODE (total - n_flg, 0, 'Y', 'N') mean
              INTO v_del_hd_n_flg, v_del_hd_total, v_del_hd_meaning
              FROM (SELECT (  SELECT /*+ CPU_COSTING  */
                                    COUNT (*)
                                FROM von_purchase_memo_dt
                               WHERE memo_header_id = v_memo_header_id AND rcv_status = 'N'
                            GROUP BY memo_header_id)
                              n_flg,
                           (  SELECT /*+ CPU_COSTING  */
                                    COUNT (*)
                                FROM von_purchase_memo_dt
                               WHERE memo_header_id = v_memo_header_id
                            GROUP BY memo_header_id)
                              total
                      FROM DUAL);

            IF v_del_hd_meaning = 'Y'
            THEN
               apps.von_pormtopo.write_log ('Delete rcv_headers_interface with receipt_num : ' || next_rec_num, 'log');

               DELETE FROM /*+ CPU_COSTING  */
                          rcv_headers_interface
                     WHERE header_interface_id = memo.rcv_headers_interface_id;

               apps.von_pormtopo.write_log ('update von_purchase_memo_hd ' || CHR (13) || 'SET rcv_interface_groups_id = NULL' || CHR (13) || 'WHERE memo_header_id = ' || v_memo_header_id, 'log');

               -- WHERE receipt_num = next_rec_num;
               UPDATE /*+ CPU_COSTING  */
                     von_purchase_memo_hd
                  SET rcv_interface_groups_id = NULL
                WHERE memo_header_id = v_memo_header_id;

               apps.von_pormtopo.write_log ('Update complete!', 'log');
               COMMIT;
               apps.von_pormtopo.write_log ('Delete Complete!', 'log');
            END IF;

            IF memo.rcv_interface_transaction_id IS NOT NULL
            THEN
               apps.von_pormtopo.write_log ('Delete  rcv_transactions_interface with interface_transaction_id : ' || memo.rcv_interface_transaction_id, 'log');

               DELETE FROM /*+ CPU_COSTING  */
                          rcv_transactions_interface
                     WHERE interface_transaction_id = memo.rcv_interface_transaction_id;

               COMMIT;
               apps.von_pormtopo.write_log ('Delete Complete!', 'log');
               apps.von_pormtopo.write_log ('Delete  mtl_transaction_lots_interface with transaction_interface_id : ' || memo.inv_transaction_interface_id, 'log');

               DELETE FROM /*+ CPU_COSTING  */
                          mtl_transaction_lots_interface
                     WHERE transaction_interface_id = memo.inv_transaction_interface_id;

               COMMIT;
               apps.von_pormtopo.write_log ('Delete Complete!', 'log');
            END IF;


            -- @ 31 May 10
            -- SOB ID
            BEGIN
               SELECT DISTINCT inv.set_of_books_id
                 INTO v_sob_id
                 FROM inv_organization_info_v inv
                WHERE inv.organization_id = one.to_organization_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_sob_id := 2027;
            END;


            --  Checkg Receipt Date

            -- Peroid Receipt Date is Close
            IF    apps.inv_txn_manager_pub.get_open_period (memo.inv_org_id, memo.memo_date, 1) = 0
               OR apps.inv_txn_manager_pub.get_open_period (memo.inv_org_id, memo.memo_date, 1) IS NULL
               OR po_dates_s.val_open_period (memo.memo_date,
                                              v_sob_id,
                                              'SQLGL',
                                              one.to_organization_id) = FALSE
               OR po_dates_s.val_open_period (memo.memo_date,
                                              v_sob_id,
                                              'PO',
                                              one.to_organization_id) = FALSE
            THEN
               -- Peroid Memo Date is Close
               IF    apps.inv_txn_manager_pub.get_open_period (memo.inv_org_id, SYSDATE, 1) = 0
                  OR apps.inv_txn_manager_pub.get_open_period (memo.inv_org_id, SYSDATE, 1) IS NULL
                  OR po_dates_s.val_open_period (SYSDATE,
                                                 v_sob_id,
                                                 'SQLGL',
                                                 one.to_organization_id) = FALSE
                  OR po_dates_s.val_open_period (SYSDATE,
                                                 v_sob_id,
                                                 'PO',
                                                 one.to_organization_id) = FALSE
               THEN
                  --                       rcv_error := rcv_error + 1;
                  v_status := 'Receipt Date and Current Date not in Open Period';
                  GOTO end_one_loop;
               ELSE
                  dreceipt_date := SYSDATE;
               END IF;
            ELSE
               dreceipt_date := memo.memo_date;
            END IF;


            /*
             -- GL Period by PCN @ 100816
             IF po_dates_s.val_open_period (memo.receipt_date,
                                            2027,
                                            'SQLGL',
                                            one.to_organization_id)
             THEN
                -- Period Reciept Date is Open
                dreceipt_date := memo.receipt_date;
             ELSIF po_dates_s.val_open_period (memo.memo_date,
                                               2027,
                                               'SQLGL',
                                               one.to_organization_id)
             THEN
                dreceipt_date := memo.memo_date;
             ELSIF po_dates_s.val_open_period (SYSDATE,
                                               2027,
                                               'SQLGL',
                                               one.to_organization_id)
             THEN
                dreceipt_date := SYSDATE;
             ELSE
                v_status := 'Receipt Date and Current Date not in GL Open Period';
                GOTO end_one_loop;
             END IF;
             */


            v_chk_interface_transaction_id := NULL;

            SELECT /*+ CPU_COSTING  */
                  COUNT (*)
              INTO v_chk_interface_transaction_id
              FROM von_purchase_memo_hd
             WHERE (rcv_interface_groups_id IS NOT NULL OR rcv_headers_interface_id IS NOT NULL) AND memo_header_id = v_memo_header_id;

            apps.von_pormtopo.write_log ('Count Interface group id : ' || v_chk_interface_transaction_id, 'log');

            IF v_chk_interface_transaction_id = 0
            THEN
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        MAX (next_receipt_num) + 1
                    INTO next_rec_num
                    FROM rcv_parameters
                   WHERE organization_id = one.to_organization_id;
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     next_rec_num := NULL;
               END;


               BEGIN
                  UPDATE /*+ CPU_COSTING  */
                        rcv_parameters rcv
                     SET rcv.next_receipt_num = next_rec_num
                   WHERE rcv.organization_id = one.to_organization_id;

                  COMMIT;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     apps.von_pormtopo.write_log ('########## Update RCV_PARAMETERS ' || next_rec_num || ' ##########', 'log');
                     apps.von_pormtopo.write_log (SQLERRM, 'log');
               END;


               -- Check Receipt in Interface or not By pcn @ 20100715
               BEGIN
                  SELECT *
                    INTO v_rcv_headers_inf
                    FROM rcv_headers_interface
                   WHERE receipt_num = next_rec_num AND ship_to_organization_id = one.to_organization_id;

                  BEGIN
                     v_next_rec_num := NULL;

                     SELECT /*+ CPU_COSTING  */
                           MAX (next_receipt_num) + 1
                       INTO v_next_rec_num
                       FROM rcv_parameters
                      WHERE organization_id = one.to_organization_id;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        v_next_rec_num := NULL;
                  END;

                  IF v_next_rec_num > next_rec_num
                  THEN
                     next_rec_num := v_next_rec_num;
                  ELSE
                     next_rec_num := next_rec_num + 1;
                  END IF;


                  BEGIN
                     UPDATE /*+ CPU_COSTING  */
                           rcv_parameters rcv
                        SET rcv.next_receipt_num = next_rec_num
                      WHERE rcv.organization_id = one.to_organization_id;

                     COMMIT;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        apps.von_pormtopo.write_log ('########## Update RCV_PARAMETERS ' || next_rec_num || ' ##########', 'log');
                        apps.von_pormtopo.write_log (SQLERRM, 'log');
                  END;
               EXCEPTION
                  WHEN TOO_MANY_ROWS
                  THEN
                     BEGIN
                        BEGIN
                           v_next_rec_num := NULL;

                           SELECT /*+ CPU_COSTING  */
                                 MAX (next_receipt_num) + 1
                             INTO v_next_rec_num
                             FROM rcv_parameters
                            WHERE organization_id = one.to_organization_id;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              v_next_rec_num := NULL;
                        END;

                        IF v_next_rec_num > next_rec_num
                        THEN
                           next_rec_num := v_next_rec_num;
                        ELSE
                           next_rec_num := next_rec_num + 1;
                        END IF;

                        BEGIN
                           UPDATE /*+ CPU_COSTING  */
                                 rcv_parameters rcv
                              SET rcv.next_receipt_num = next_rec_num
                            WHERE rcv.organization_id = one.to_organization_id;

                           COMMIT;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              apps.von_pormtopo.write_log ('########## Update RCV_PARAMETERS ' || next_rec_num || ' ##########', 'log');
                              apps.von_pormtopo.write_log (SQLERRM, 'log');
                        END;
                     END;
                  WHEN NO_DATA_FOUND
                  THEN
                     BEGIN
                        UPDATE /*+ CPU_COSTING  */
                              rcv_parameters rcv
                           SET rcv.next_receipt_num = next_rec_num
                         WHERE rcv.organization_id = one.to_organization_id;

                        COMMIT;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           apps.von_pormtopo.write_log ('########## Update RCV_PARAMETERS ' || next_rec_num || ' ##########', 'log');
                           apps.von_pormtopo.write_log (SQLERRM, 'log');
                     END;
                  WHEN OTHERS
                  THEN
                     BEGIN
                        BEGIN
                           v_next_rec_num := NULL;

                           SELECT /*+ CPU_COSTING  */
                                 MAX (next_receipt_num) + 1
                             INTO v_next_rec_num
                             FROM rcv_parameters
                            WHERE organization_id = one.to_organization_id;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              v_next_rec_num := NULL;
                        END;

                        IF v_next_rec_num > next_rec_num
                        THEN
                           next_rec_num := v_next_rec_num;
                        ELSE
                           next_rec_num := next_rec_num + 1;
                        END IF;

                        BEGIN
                           UPDATE /*+ CPU_COSTING  */
                                 rcv_parameters rcv
                              SET rcv.next_receipt_num = next_rec_num
                            WHERE rcv.organization_id = one.to_organization_id;

                           COMMIT;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              apps.von_pormtopo.write_log ('########## Update RCV_PARAMETERS ' || next_rec_num || ' ##########', 'log');
                              apps.von_pormtopo.write_log (SQLERRM, 'log');
                        END;
                     END;
               END;

               BEGIN
                  UPDATE /*+ CPU_COSTING  */
                        rcv_parameters rcv
                     SET rcv.next_receipt_num = next_rec_num
                   WHERE rcv.organization_id = one.to_organization_id;

                  COMMIT;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     apps.von_pormtopo.write_log ('########## Update RCV_PARAMETERS ' || next_rec_num || ' ##########', 'log');
                     apps.von_pormtopo.write_log (SQLERRM, 'log');
               END;

               apps.von_pormtopo.write_log ('<B># RECEIVE NUMBER : </B> : ' || next_rec_num, 'log');

               SELECT /*+ CPU_COSTING  */
                     rcv_interface_groups_s.NEXTVAL INTO v_group_id FROM DUAL;

               apps.von_pormtopo.write_log ('Group id : ' || v_group_id, 'log');

               SELECT /*+ CPU_COSTING  */
                     rcv_headers_interface_s.NEXTVAL INTO v_header_interface_id FROM DUAL;

               apps.von_pormtopo.write_log ('Header interface id : ' || v_header_interface_id, 'log');
               apps.von_pormtopo.write_log ('########## Insert RCV_HEADERS_INTERFACE ##########', 'log');



               INSERT INTO /*+ CPU_COSTING  */
                          rcv_headers_interface (header_interface_id,
                                                 GROUP_ID,
                                                 processing_status_code,
                                                 receipt_source_code,
                                                 transaction_type,
                                                 last_update_login,
                                                 last_update_date,
                                                 last_updated_by,
                                                 creation_date,
                                                 created_by,
                                                 vendor_id,
                                                 vendor_site_id,
                                                 receipt_header_id,
                                                 receipt_num,
                                                 expected_receipt_date,
                                                 validation_flag,
                                                 auto_transact_code,
                                                 ship_to_organization_code,
                                                 ship_to_organization_id,
                                                 vendor_num,
                                                 attribute10,
                                                 attribute11,
                                                 comments,
                                                 conversion_rate_date)
                    VALUES (v_header_interface_id,
                            v_group_id,
                            'PENDING',
                            'VENDOR',
                            'NEW',
                            userid,
                            dreceipt_date,                                                                                                                                                   ---SYSDATE,
                            userid,
                            dreceipt_date,                                                                                                                                                   ---SYSDATE,
                            userid,
                            one.vendor_id,
                            one.vendor_site_id,
                            NULL,
                            next_rec_num,
                            dreceipt_date,                                                                                                                                  --one.expected_receipt_date,
                            'Y',
                            'DELIVER',
                            memo.whse_code,
                            one.to_organization_id,
                            memo.vendor_code,
                            memo.memo_line_id,
                            'PORM',
                            memo.acomment,
                            memo.memo_date);

               COMMIT;
               apps.von_pormtopo.write_log ('########## Insert RCV_HEADERS_INTERFACE Complete! ##########', 'log');

               BEGIN
                  UPDATE /*+ CPU_COSTING  */
                        von_purchase_memo_hd
                     SET rcv_interface_groups_id = v_group_id, rcv_number = next_rec_num, rcv_headers_interface_id = v_header_interface_id
                   WHERE memo_header_id = v_memo_header_id;

                  v_rcv_number := next_rec_num;
                  apps.von_pormtopo.write_log ('update memo header id : ' || v_memo_header_id, 'log');
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     apps.von_pormtopo.write_log (SQLERRM, 'log');
               END;



               COMMIT;
            ELSE
               apps.von_pormtopo.write_log ('same memo header id !!!', 'log');

               SELECT /*+ CPU_COSTING  */
                     rcv_interface_groups_id
                 INTO v_group_id
                 FROM von_purchase_memo_hd
                WHERE memo_header_id = v_memo_header_id;

               apps.von_pormtopo.write_log ('Group id =>' || v_group_id, 'log');
            END IF;

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     agent_id
                 INTO v_agent_id
                 FROM po_headers_all
                WHERE po_header_id = one.po_header_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_agent_id := NULL;
                  GOTO end_one_loop;
            END;

            apps.von_pormtopo.write_log ('Agent ID : ' || v_agent_id, 'log');

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     locator
                 INTO v_locator
                 FROM von_receipt
                WHERE receipt_id = memo.receipt_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  von_pormtopo.write_log ('No locator.', 'log');
                  GOTO end_one_loop;
            END;

            apps.von_pormtopo.write_log ('Locator : ' || v_locator, 'log');
            apps.von_pormtopo.write_log ('########## Insert RCV_TRANSACTIONS_INTERFACE ##########', 'log');

            INSERT INTO /*+ CPU_COSTING  */
                       rcv_transactions_interface (interface_transaction_id,
                                                   GROUP_ID,
                                                   last_update_date,
                                                   last_updated_by,
                                                   creation_date,
                                                   created_by,
                                                   last_update_login,
                                                   transaction_type,
                                                   transaction_date,
                                                   processing_status_code,
                                                   processing_mode_code,
                                                   transaction_status_code,
                                                   po_line_id,
                                                   item_id,
                                                   quantity,
                                                   unit_of_measure,
                                                   po_line_location_id,
                                                   auto_transact_code,
                                                   receipt_source_code,
                                                   to_organization_code,
                                                   source_document_code,
                                                   document_num,
                                                   header_interface_id,
                                                   validation_flag,
                                                   subinventory,
                                                   locator,
                                                   deliver_to_location_id,
                                                   destination_type_code,
                                                   ship_to_location_id,
                                                   packing_slip,
                                                   shipped_date,
                                                   item_num,
                                                   po_release_id,
                                                   vendor_num,
                                                   secondary_quantity,
                                                   secondary_unit_of_measure,
                                                   employee_id,
                                                   vendor_id,
                                                   po_header_id,
                                                   currency_conversion_date)
                 VALUES (rcv_transactions_interface_s.NEXTVAL,
                         v_group_id,
                         SYSDATE,
                         userid,
                         SYSDATE,
                         userid,
                         userid,
                         'RECEIVE',
                         dreceipt_date,                                                                                                                                               ---memo.memo_date,
                         'PENDING',
                         'BATCH',
                         'PENDING',
                         one.po_line_id,
                         one.item_id,
                         one.remainding_qty,
                         one.uom,
                         one.po_line_location_id,
                         'DELIVER',
                         'VENDOR',
                         memo.whse_code,
                         'PO',
                         one.po_number,
                         v_header_interface_id,
                         'Y',
                         -- rcv_headers_interface_s.CURRVAL, 'Y',
                         one.sub_inventory,
                         v_locator,
                         one.ship_to_location_id,
                         'INVENTORY',
                         one.ship_to_location_id,
                         NULL,
                         NULL,
                         one.item_number,
                         one.po_release_id,
                         memo.vendor_code,
                         one.secondary_ordered_qty,
                         one.secondary_ordered_uom,
                         v_agent_id,
                         one.vendor_id,
                         one.po_header_id,
                         memo.memo_date);

            apps.von_pormtopo.write_log ('item number= ' || one.item_number, 'log');

            /*################check Lot control####################*/
            IF one.lot_control <> 1 AND one.lot_control IS NOT NULL
            THEN
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        TO_CHAR (receipt_date, 'YYMMDD') || '/' || receipt_number
                    INTO v_lot_no
                    FROM von_receipt
                   WHERE receipt_id = memo.receipt_id;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_lot_no := TO_CHAR (SYSDATE, 'YYMMDD') || '/' || '????';
               END;

               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        grade
                    INTO v_grade
                    FROM von_receipt
                   WHERE receipt_id = memo.receipt_id;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_grade := 'AA';
               END;

               apps.von_pormtopo.write_log ('#####lot control.#####', 'log');
               apps.von_pormtopo.write_log ('########## Insert MTL_TRANSACTION_LOTS_INTERFACE ##########', 'log');

               o_transaction_qty := one.remainding_qty;
               o_second_trans_qty := one.secondary_ordered_qty;
               o_lot_number := v_lot_no;

               INSERT INTO /*+ CPU_COSTING  */
                          mtl_transaction_lots_interface (transaction_interface_id,
                                                          last_update_date,
                                                          last_updated_by,
                                                          creation_date,
                                                          created_by,
                                                          last_update_login,
                                                          lot_number,
                                                          transaction_quantity,
                                                          primary_quantity,
                                                          secondary_transaction_quantity,
                                                          product_code,
                                                          product_transaction_id,
                                                          grade_code)
                    VALUES (mtl_material_transactions_s.NEXTVAL,
                            SYSDATE,
                            userid,
                            SYSDATE,
                            userid,
                            userid,
                            v_lot_no,
                            one.remainding_qty,
                            one.remainding_qty,
                            one.secondary_ordered_qty,
                            'RCV',
                            rcv_transactions_interface_s.CURRVAL,
                            v_grade);

               apps.von_pormtopo.write_log ('##### Update VON_PURCHASE_MEMO_DT #####', 'log');

               UPDATE /*+ CPU_COSTING  */
                     von_purchase_memo_dt
                  SET inv_transaction_interface_id = mtl_material_transactions_s.CURRVAL
                WHERE memo_line_id = memo.memo_line_id;
            ELSE
               apps.von_pormtopo.write_log ('#####No lot control.#####', 'log');
            END IF;

            UPDATE /*+ CPU_COSTING  */
                  von_purchase_memo_dt
               SET rcv_no = next_rec_num,
                   rcv_interface_groups_id = v_group_id,
                   rcv_headers_interface_id = v_header_interface_id,
                   --v_memo_header_id,
                   rcv_interface_transaction_id = rcv_transactions_interface_s.CURRVAL
             WHERE memo_line_id = memo.memo_line_id;

            COMMIT;
         END LOOP memo;

        <<end_one_loop>>
         apps.von_pormtopo.write_log (CHR (13), 'log');
      END LOOP one;

      von_pormtopo.write_log ('Group Id : ' || v_group_id, 'log');
      v_req_id :=
         fnd_request.submit_request ('PO',                                                                                                                                                -- application
                                     'RVCTP',                                                                                                                                        --program name/.rdf
                                     '',
                                     '',
                                     FALSE,
                                     'BATCH',
                                     v_group_id,
                                     x_org_id);
      COMMIT;

      IF (v_req_id != 0)
      THEN
         IF fnd_concurrent.wait_for_request (v_req_id,
                                             6,
                                             1500,
                                             v_phase,
                                             v_status,
                                             v_dev_phase,
                                             v_dev_status,
                                             v_message)
         THEN
            von_pormtopo.write_log (v_dev_status || ' : ' || v_message, 'log waiting 1');

            IF fnd_concurrent.wait_for_request (v_req_id,
                                                6,
                                                100,
                                                v_phase,
                                                v_status,
                                                v_dev_phase,
                                                v_dev_status,
                                                v_message)
            THEN
               von_pormtopo.write_log (v_dev_status || ' : ' || v_message, 'log waiting 2');
            END IF;

            IF v_dev_phase != 'COMPLETE' OR v_dev_status != 'NORMAL'
            THEN
               v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
            ELSE
               von_pormtopo.write_log ('##### Check Processing_status_code #####', 'log');

               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        processing_status_code
                    INTO v_process_status
                    FROM rcv_headers_interface
                   WHERE header_interface_id = v_header_interface_id;
               EXCEPTION
                  WHEN OTHERS
                  THEN
                     v_process_status := 'ERROR WITH QUERY';
               END;

               IF v_process_status <> 'SUCCESS'
               THEN
                  v_flag := 'E';
               ELSIF v_process_status = 'ERROR WITH QUERY'
               THEN
                  von_pormtopo.write_log ('Please check query in rcv_headers_interface with condition ''where header_interface_id= ' || v_header_interface_id || '''', 'log');
                  GOTO end_now;
               ELSE
                  v_flag := 'S';
               END IF;
            END IF;
         END IF;
      END IF;

      DBMS_LOCK.sleep (40);

      FOR two IN (SELECT /*+ CPU_COSTING  */
                        dt.rcv_interface_transaction_id, dt.memo_line_id, hd.rcv_headers_interface_id
                    FROM von_purchase_memo_dt dt, von_purchase_memo_hd hd
                   WHERE dt.memo_header_id = v_memo_header_id AND hd.memo_header_id = dt.memo_header_id)
      LOOP
         apps.von_pormtopo.write_log ('########## Check for update Memo Receive status ##########', 'log');
         apps.von_pormtopo.write_log ('interface transaction id : ' || two.rcv_interface_transaction_id || ' Memo line id : ' || two.memo_line_id, 'log');

         SELECT /*+ CPU_COSTING  */
               COUNT (*)
           INTO v_chk_interface
           FROM rcv_transactions_interface
          WHERE interface_transaction_id = two.rcv_interface_transaction_id;

         apps.von_pormtopo.write_log ('Transactions_interface found ' || v_chk_interface, 'log');

         SELECT /*+ CPU_COSTING  */
               COUNT (*)
           INTO v_chk_transaction
           FROM rcv_transactions
          WHERE interface_transaction_id = two.rcv_interface_transaction_id;

         apps.von_pormtopo.write_log ('Receive Transactions found ' || v_chk_transaction, 'log');

         IF v_chk_interface = 0 AND v_chk_transaction <> 0
         THEN
            apps.von_pormtopo.write_log ('Update ''Y''' || v_chk_interface || ' = 0 ' || v_chk_transaction || ' <> 0', 'log');

            UPDATE /*+ CPU_COSTING  */
                  von_purchase_memo_dt
               SET rcv_status = 'Y'
             WHERE memo_line_id = two.memo_line_id;

            IF v_rcv_status IS NOT NULL
            THEN
               IF v_rcv_status <> 'Y'
               THEN
                  v_rcv_status := 'N';
               END IF;
            ELSE
               v_rcv_status := 'Y';
            END IF;

            apps.von_pormtopo.write_log ('Receive Complete : ' || v_rcv_number, 'line');
            rcv_complete := rcv_complete + 1;
         ELSE
            apps.von_pormtopo.write_log ('Update ''N''' || v_chk_interface || ' = 0 ' || v_chk_transaction || ' <> 0', 'log');

            UPDATE /*+ CPU_COSTING  */
                  von_purchase_memo_dt
               SET rcv_status = 'N'
             WHERE memo_line_id = two.memo_line_id;

            v_rcv_status := 'N';
            apps.von_pormtopo.write_log ('Receive Error.', 'line');

            /*##########Got Error Message##########*/

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     'Column => ' || column_name || CHR (13) || 'Table => ' || table_name || CHR (13) || error_message
                 INTO v_inferrmsg
                 FROM po_interface_errors
                WHERE interface_header_id = two.rcv_headers_interface_id;
            -- (SELECT hd_inf.interface_header_id
            --  FROM po_headers_interface hd_inf
            -- WHERE hd_inf.attribute10 = v_memo_header_id
            -- AND hd_inf.attribute11 = 'PORM');
            EXCEPTION
               WHEN OTHERS
               THEN
                  apps.von_pormtopo.write_log ('Can not find error message', 'line');
            END;

            apps.von_pormtopo.write_log (v_inferrmsg, 'log');

            /*#########Got Error Message##########*/
            rcv_error := rcv_error + 1;
         END IF;

         COMMIT;
      END LOOP two;

      SELECT /*+ CPU_COSTING  */
            COUNT (*)
        INTO v_final_chk_all
        FROM von_purchase_memo_dt
       WHERE memo_header_id = v_memo_header_id;

      SELECT /*+ CPU_COSTING  */
            COUNT (*)
        INTO v_final_chk_y
        FROM von_purchase_memo_dt
       WHERE memo_header_id = v_memo_header_id AND rcv_status = 'Y';

      IF v_final_chk_all <> 0 AND v_final_chk_y <> 0 AND v_final_chk_y = v_final_chk_all
      THEN
         apps.von_pormtopo.write_log ('Update ''Y'' to Header ', 'log');
      END IF;

     <<end_now>>
      NULL;
   END receive_po;

   PROCEDURE consign_return_memo (x_org_id             IN            NUMBER,
                                  x_userid             IN            NUMBER,
                                  x_memo_line_id       IN            NUMBER,
                                  x_receipt_id         IN            NUMBER,
                                  x_lot_number         IN            VARCHAR2,
                                  x_transaction_qty    IN            NUMBER,                                                                                                  -- receive from receive_po
                                  x_second_trans_qty   IN            NUMBER,                                                                                                  -- receive from receive_po
                                  rcv_complete            OUT        NUMBER,
                                  rcv_error               OUT        NUMBER,
                                  x_return_status         OUT NOCOPY VARCHAR2)
   IS
      CURSOR cur_consign_dt
      IS
         SELECT /*+ CPU_COSTING  */
               ou_org_id,
                rc.whse_code,
                rc.receipt_date,
                cons.*,
                rc.locator,
                rc.item_no,
                rc.receipt_number,
                ---rc.net_weight net_weight,
                cons.memo_net_weight net_weight,
                mi.secondary_uom_code,
                cons.memo_gross_weight gross_weight,
                rc.item_id item_id,
                rc.inv_org_id,
                rc.sub_inventory,
                rc.grade,
                mi.lot_control_code,
                mi.grade_control_flag,
                mi.primary_uom_code,
                mi.primary_unit_of_measure,
                (SELECT loc.inventory_location_id
                   FROM mtl_item_locations loc
                  WHERE loc.organization_id = rc.inv_org_id AND loc.segment1 = rc.locator)
                   locator_id,
                rc.vendor_id,
                (SELECT DISTINCT hd.memo_date
                   FROM von_purchase_memo_hd hd
                  WHERE hd.memo_header_id = cons.memo_header_id)
                   memo_date,
                cons.ROWID r_id
           FROM von_purchase_memo_dt cons, von_receipt rc, mtl_system_items mi
          WHERE cons.memo_line_id = x_memo_line_id AND rc.receipt_id = cons.receipt_id AND rc.item_id = mi.inventory_item_id AND rc.inv_org_id = mi.organization_id;

      v_process_status           VARCHAR2 (100);
      v_sts                      BOOLEAN;
      v_phase                    VARCHAR2 (200);
      v_status                   VARCHAR2 (200);
      v_dev_phase                VARCHAR2 (200);
      v_dev_status               VARCHAR2 (200);
      v_message                  VARCHAR2 (200);

      vsegment1                  VARCHAR2 (25);
      vsegment2                  VARCHAR2 (25);
      vsegment3                  VARCHAR2 (25);
      vsegment4                  VARCHAR2 (25);
      vsegment5                  VARCHAR2 (25);
      vsegment6                  VARCHAR2 (25);
      vsegment7                  VARCHAR2 (25);
      vsegment8                  VARCHAR2 (25);
      vdistribution_account_id   VARCHAR2 (100);
      ndistribution_account_id   NUMBER;
      v_req_id                   NUMBER (20) := NULL;
      n_interface_seq            NUMBER;
      ntransaction_type_id       NUMBER;
      ntransaction_action_id     NUMBER;
      dreceipt_date              DATE;
      l_lot_rec                  mtl_lot_numbers%ROWTYPE;
      o_return_status            VARCHAR2 (1);
      o_row_id                   ROWID;
      p_msg_count                NUMBER;
      p_msg_data                 VARCHAR2 (10000);
      o_msg_count                NUMBER;
      o_msg_data                 VARCHAR2 (200);
      p_return_status            VARCHAR2 (10000);
      v_sob_id                   NUMBER;
   BEGIN
      FOR x IN cur_consign_dt
      LOOP
         --if cns_transaction_interface_id is not null l then delete mtl_transactions_interface
         --and mtl_transaction_lots_interface where transaction_interface_id = cns_transaction_interface_id

         IF x.cns_transaction_interface_id IS NOT NULL
         THEN
            DELETE /*+ CPU_COSTING  */
                  mtl_transaction_lots_interface
             WHERE transaction_interface_id = x.cns_transaction_interface_id;

            DELETE /*+ CPU_COSTING  */
                  mtl_transactions_interface
             WHERE transaction_interface_id = x.cns_transaction_interface_id;                                                                                                        -- con_trans_id ???

            COMMIT;
         END IF;

         -- Get Consign Return Transaction Type ID. Transaction Aciton ID
         BEGIN
            SELECT /*+ CPU_COSTING  */
                  transaction_type_id, transaction_action_id
              INTO ntransaction_type_id, ntransaction_action_id
              FROM mtl_transaction_types
             WHERE transaction_type_name = 'Consign Return';
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               rcv_error := rcv_error + 1;
               x_return_status := 'Undefined Transaction Type Setup';
               GOTO end_now;
            WHEN TOO_MANY_ROWS
            THEN
               rcv_error := rcv_error + 1;
               x_return_status := 'To Many Transaction Types Setup for Consignment';
               GOTO end_now;
         END;

         -- SOB ID
         BEGIN
            SELECT DISTINCT inv.set_of_books_id
              INTO v_sob_id
              FROM inv_organization_info_v inv
             WHERE inv.organization_id = x.inv_org_id;
         EXCEPTION
            WHEN OTHERS
            THEN
               v_sob_id := 2027;
         END;                                                                                                                                                                    
         
         --  Checkg Receipt Date
         -- Peroid Receipt Date is Close
         IF    apps.inv_txn_manager_pub.get_open_period (x.inv_org_id, x.memo_date, 1) = 0
            OR apps.inv_txn_manager_pub.get_open_period (x.inv_org_id, x.memo_date, 1) IS NULL
            OR po_dates_s.val_open_period (x.memo_date,
                                           v_sob_id,
                                           'SQLGL',
                                           x.inv_org_id) = FALSE
            OR po_dates_s.val_open_period (x.memo_date,
                                           v_sob_id,
                                           'PO',
                                           x.inv_org_id) = FALSE
         THEN
            -- Peroid Memo Date is Close
            IF    apps.inv_txn_manager_pub.get_open_period (x.inv_org_id, SYSDATE, 1) = 0
               OR apps.inv_txn_manager_pub.get_open_period (x.inv_org_id, SYSDATE, 1) IS NULL
               OR po_dates_s.val_open_period (SYSDATE,
                                              v_sob_id,
                                              'SQLGL',
                                              x.inv_org_id) = FALSE
               OR po_dates_s.val_open_period (SYSDATE,
                                              v_sob_id,
                                              'PO',
                                              x.inv_org_id) = FALSE
            THEN
               rcv_error := rcv_error + 1;
               x_return_status := 'Receipt Date and Current Date not in Open Period';
               GOTO end_now;
            ELSE
               dreceipt_date := SYSDATE;
            END IF;
         ELSE
            dreceipt_date := x.memo_date;
         END IF;

         /*
         -- GL Period by PCN @ 100816
         IF po_dates_s.val_open_period (x.receipt_date,
                                        2027,
                                        'SQLGL',
                                        x.inv_org_id)
         THEN
            -- Period Reciept Date is Open
            dreceipt_date := x.receipt_date;
         ELSIF po_dates_s.val_open_period (x.memo_date,
                                           2027,
                                           'SQLGL',
                                           x.inv_org_id)
         THEN
            dreceipt_date := x.memo_date;
         ELSIF po_dates_s.val_open_period (SYSDATE,
                                           2027,
                                           'SQLGL',
                                           x.inv_org_id)
         THEN
            dreceipt_date := SYSDATE;
         ELSE
            rcv_error := rcv_error + 1;
            x_return_status := 'Receipt Date and Current Date not in GL Open Period';
            GOTO end_now;
         END IF;
        */


         -- Checking Accounting Distrubute
         BEGIN
            --
            SELECT /*+ CPU_COSTING  */
                  segment1,
                   segment2,
                   segment3,
                   segment4,
                   segment5,
                   segment6,
                   segment7,
                   segment8,
                   account,
                   code_combination_id
              INTO vsegment1,
                   vsegment2,
                   vsegment3,
                   vsegment4,
                   vsegment5,
                   vsegment6,
                   vsegment7,
                   vsegment8,
                   vdistribution_account_id,
                   ndistribution_account_id
              FROM von_acc_mapping
             WHERE whse_code = x.whse_code AND acc_type = 1040 AND item_id = x.item_id AND ou_org_id = x.ou_org_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               vsegment1 := NULL;
               vsegment2 := NULL;
               vsegment3 := NULL;
               vsegment4 := NULL;
               vsegment5 := NULL;
               vsegment6 := NULL;
               vsegment7 := NULL;
               vsegment8 := NULL;
               vdistribution_account_id := NULL;
               ndistribution_account_id := NULL;

               rcv_error := rcv_error + 1;
               x_return_status := 'Undefined Account Code Setup';
               GOTO end_now;
         END;

         -- Get  n_interface_seq
         SELECT /*+ CPU_COSTING  */
               mtl_transactions_interface_s.NEXTVAL INTO n_interface_seq FROM DUAL;

         -- Get Data mtl_transactions_interface
         INSERT INTO /*+ CPU_COSTING  */
                    mtl_transactions_interface (transaction_interface_id,
                                                source_code,
                                                source_line_id,
                                                source_header_id,
                                                process_flag,
                                                lock_flag,
                                                request_id,
                                                program_id,
                                                transaction_mode,
                                                validation_required,
                                                transaction_action_id,
                                                last_update_date,
                                                last_updated_by,
                                                creation_date,
                                                created_by,
                                                last_update_login,
                                                organization_id,
                                                transaction_quantity,
                                                transaction_uom,
                                                transaction_date,
                                                transaction_type_id,
                                                inventory_item_id,
                                                transaction_source_name,
                                                item_segment1,
                                                subinventory_code,
                                                locator_id,
                                                locator_name,
                                                loc_segment1,
                                                secondary_transaction_quantity,
                                                secondary_uom_code,
                                                transaction_cost,
                                                dst_segment1,
                                                dst_segment2,
                                                dst_segment3,
                                                dst_segment4,
                                                dst_segment5,
                                                dst_segment6,
                                                dst_segment7,
                                                dst_segment8,
                                                distribution_account_id)
              VALUES (n_interface_seq,                                                                                                                                      -- transaction_interface_id,
                      x.receipt_number,
                      --TO_CHAR ('CONSIGN_RC_' || rec.receipt_number),                                                            --     source_code,
                      x.receipt_id,                                                                                                                                                   -- source_line_id,
                      x.receipt_id,                                                                                                                                                 -- source_header_id,
                      1,                                                                                                                                                            -- process_flag is 1
                      NULL,                                                                                                                                                                  --lock_flag
                      NULL,                                                                                                                                                                -- request_ID
                      NULL,                                                                                                                                                                -- Program ID
                      3,                                                                                                                                             -- transaction_mode Backgroung is 3
                      1,                                                                                                                                                         -- validation_required,
                      ntransaction_action_id,                                                                                                                                 --  transaction_action_id,
                      SYSDATE,                                                                                                                                                       --last_update_date,
                      x_userid,                                                                                                                                                             -- update By
                      SYSDATE,                                                                                                                                                            -- Create Date
                      x_userid,                                                                                                                                                            --- Create By
                      fnd_global.login_id,                                                                                                                                          --last_update_login,
                      x.inv_org_id,                                                                                                                                                  -- organization_id,
                      ROUND (TO_NUMBER (x.net_weight), 5) * -1,                                                                                                                 -- transaction_quantity,
                      x.primary_uom_code,                                                                                                                                             -- transaction_uom
                      dreceipt_date,                                                                                                                                                 -- transaction_date
                      ntransaction_type_id,                                                                                                               --nTransaction_type_id, --transaction_type_id,
                      x.item_id,                                                                                                                                                   -- inventory_item_id,
                      x.receipt_number,                                                                                                                                       --transaction_source_name,
                      x.item_no,                                                                                                                                                       -- item_segment1,
                      x.sub_inventory,                                                                                                                                             -- subinventory_code,
                      x.locator_id,                                                                                                                                                       -- locator_id,
                      x.locator,                                                                                                                                                        -- locator_name,
                      x.locator,                                                                                                                                                        -- loc_segment1,
                      ROUND (TO_NUMBER (x.gross_weight), 5) * -1,                                                                                                      -- secondary_transaction_quantity
                      x.secondary_uom_code,                                                                                                                                       -- secondary_uom_code,
                      NULL,                                                                                                                                                           --TRANSACTION_COST
                      vsegment1,
                      vsegment2,
                      vsegment3,                                                                                                                                                          --DST_SEGMENT3
                      vsegment4,                                                                                                                                                          --DST_SEGMENT4
                      vsegment5,                                                                                                                                                          --DST_SEGMENT5
                      vsegment6,                                                                                                                                                          --DST_SEGMENT6
                      vsegment7,                                                                                                                                                          --DST_SEGMENT7
                      vsegment8,                                                                                                                                                         --DST_SEGMENT8,
                      ndistribution_account_id);

         COMMIT;

         -- Check Grade Control
         -- Get Data to mtl_transaction_lots_interface
         -- Interface

         -- Lot Control
         IF x.lot_control_code = 2                                                                                                                                                            -- Control
         THEN
            IF x.grade_control_flag = 'Y'
            THEN
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        *
                    INTO l_lot_rec
                    FROM mtl_lot_numbers
                   WHERE organization_id = x.ou_org_id AND inventory_item_id = x.item_id AND lot_number = TO_CHAR (x.receipt_date, 'YYMMDD') || '/' || x.receipt_number;

                  GOTO insert_lot_inf;
                  o_return_status := 'S';
                  o_msg_count := 1;
                  o_msg_data := 'Already Exist';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     -- Create Lot
                     -- Define Lot
                     l_lot_rec.organization_id := x.inv_org_id;                                                                                                                        -- rec.ou_org_id;
                     l_lot_rec.inventory_item_id := x.item_id;
                     l_lot_rec.lot_number := x_lot_number;
                     l_lot_rec.grade_code := x.grade;
                     l_lot_rec.status_id := 1;

                     apps.von_opm.create_lot (p_orgn_id         => x.inv_org_id,
                                              p_item_id         => x.item_id,
                                              p_lot_no          => TO_CHAR (x.receipt_date, 'YYMMDD') || '/' || x.receipt_number,
                                              p_grade           => x.grade,
                                              p_date            => x.receipt_date,
                                              p_conv            => TO_CHAR (x.net_weight / x.gross_weight),
                                              p_uom_code        => x.primary_uom_code,
                                              p_uom_code2       => x.secondary_uom_code,
                                              p_user_id         => x_userid,
                                              o_msg_count       => p_msg_count,
                                              o_msg_data        => p_msg_data,
                                              o_return_status   => p_return_status);
                     COMMIT;

                     IF NVL (p_return_status, 'E') = 'S'
                     THEN
                        DBMS_OUTPUT.put_line ('Success');
                        ---apps.von_pormtopo.write_log ('Success !! : ', 'line'); --- today
                        g_cns_status_result := 'Success !!';
                        GOTO insert_lot_inf;
                     ELSE
                        -- Error on Create Lot
                        DBMS_OUTPUT.put_line ('No_Success');
                        ---apps.von_pormtopo.write_log ('Error !! : '||p_msg_data, 'line');
                        g_cns_status_result := 'Error !!';
                        DBMS_OUTPUT.put_line (p_msg_data);
                        rcv_error := rcv_error + 1;
                        x_return_status := p_msg_data;
                        GOTO end_now;
                     END IF;
               END;
            ELSE                                                                                                                                                             -- GRADE_CONTROL_FLAG = 'Y'
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        *
                    INTO l_lot_rec
                    FROM mtl_lot_numbers
                   WHERE organization_id = x.ou_org_id AND inventory_item_id = x.item_id AND lot_number = TO_CHAR (x.receipt_date, 'YYMMDD') || '/' || x.receipt_number;

                  GOTO insert_lot_inf;
                  o_return_status := 'S';
                  o_msg_count := 1;
                  o_msg_data := 'Already Exist';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     -- Create Lot
                     -- Define Lot
                     l_lot_rec.organization_id := x.ou_org_id;
                     l_lot_rec.inventory_item_id := x.inv_org_id;
                     --                            l_lot_rec.lot_number := TO_CHAR (x.receipt_date, 'YYMMDD') || '/' || x.receipt_number;
                     l_lot_rec.lot_number := x_lot_number;
                     l_lot_rec.status_id := 1;

                     apps.von_opm.create_lot (p_orgn_id         => x.inv_org_id,
                                              p_item_id         => x.item_id,
                                              p_lot_no          => TO_CHAR (x.receipt_date, 'YYMMDD') || '/' || x.receipt_number,
                                              p_grade           => x.grade,
                                              p_date            => x.receipt_date,
                                              p_conv            => TO_CHAR (x.net_weight / x.gross_weight),
                                              p_uom_code        => x.primary_uom_code,
                                              p_uom_code2       => x.secondary_uom_code,
                                              p_user_id         => x_userid,
                                              o_msg_count       => p_msg_count,
                                              o_msg_data        => p_msg_data,
                                              o_return_status   => p_return_status);

                     IF NVL (p_return_status, 'E') = 'S'
                     THEN
                        --                        DBMS_OUTPUT.put_line ('Success');
                        ---apps.von_pormtopo.write_log (' not Lot Control : Success !! : ', 'line'); --- today
                        g_cns_status_result := 'not Lot Control : Success !!';
                        GOTO insert_lot_inf;
                     ELSE
                        -- Error on Create Lot
                        DBMS_OUTPUT.put_line ('No_Success');
                        ---apps.von_pormtopo.write_log ('not Lot Control - No_Success !! : '||p_msg_data, 'line');
                        g_cns_status_result := 'not Lot Control : Error !!' || p_msg_data;
                        --                        DBMS_OUTPUT.put_line (p_msg_data);
                        rcv_error := rcv_error + 1;
                        x_return_status := p_msg_data;
                        GOTO end_now;
                     END IF;
               END;
            END IF;

            apps.von_pormtopo.write_log ('Chk Point 2 consign_return_memo  !! : ' || g_cns_status_result, 'LOG');                                                                              --- today

           <<insert_lot_inf>>
            INSERT INTO /*+ CPU_COSTING  */
                       mtl_transaction_lots_interface (transaction_interface_id,
                                                       process_flag,
                                                       last_update_date,
                                                       last_updated_by,
                                                       creation_date,
                                                       created_by,
                                                       last_update_login,
                                                       lot_number,
                                                       transaction_quantity,
                                                       --primary_quantity,
                                                       secondary_transaction_quantity,
                                                       grade_code,
                                                       source_code,
                                                       source_line_id,
                                                       vendor_id,
                                                       vendor_name,
                                                       attribute1)
                 VALUES (n_interface_seq,
                         1,                                                                                                                                                             -- process_flag,
                         SYSDATE,
                         x_userid,                                                                                                                                                 --fnd_global.user_id,
                         SYSDATE,
                         x_userid,                                                                                                                                                 --fnd_global.user_id,
                         fnd_global.login_id,
                         TO_CHAR (x.receipt_date, 'YYMMDD') || '/' || x.receipt_number,
                         ROUND (TO_NUMBER (x.net_weight), 5) * -1,                                                                                                               -- transaction_quantity
                         --ROUND (TO_NUMBER (rec.net_weight), 5), -- primary_quantity,
                         ROUND (TO_NUMBER (x.gross_weight), 5) * -1,                                                                                                   --secondary_transaction_quantity,
                         x.grade,
                         x.receipt_number,
                         --TO_CHAR ('CONSIGN_RC_' || rec.receipt_number),                                                       --     source_code,
                         x.receipt_id,
                         x.vendor_id,
                         NULL,                                                                                                                                                  -- Addtional Vendor Name
                         x.receipt_number);

            COMMIT;
         ELSIF x.lot_control_code = 1
         THEN
            g_cns_status_result := 'not Lot Control : Success !!';
            apps.von_pormtopo.write_log ('Chk Point 3 In consign_return_memo not Lot Control : Success !! : ', 'LOG');                                                                         --- today
            COMMIT;
         END IF;

         v_req_id :=
            fnd_request.submit_request ('INV',                                                                                                                                            -- application
                                        'INCTCM',                                                                                                                                             -- program
                                        'Consignment Inventory Transactions Processor',
                                        --NULL,         --'Inventory Transaction Worker', description optional)
                                        NULL,                                                                                                                                   -- start_time (optional)
                                        FALSE,                                                                                                                                            -- sub_request
                                        '',                                                                                                 --to_char(v_group_id), -- header id to process not used here
                                        '1',                                                                                                                 -- 1 is the interface table 2 is temp table
                                        '',                                                                                                                                 -- group to process required
                                        x.receipt_number,                                                                                                                           -- source to process
                                        --chr(0),
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '');

         --            DBMS_OUTPUT.put_line ('reg id=' || v_req_id);
         COMMIT;

         ----         DBMS_LOCK.sleep (40);
         /*
          IF (v_req_id != 0)
          THEN
             IF fnd_concurrent.wait_for_request (v_req_id,
                                                 6,
                                                 1800,
                                                 v_phase,
                                                 v_status,
                                                 v_dev_phase,
                                                 v_dev_status,
                                                 v_message)
             THEN
                DBMS_LOCK.sleep (5);
                COMMIT;

                von_pormtopo.write_log (v_dev_status || ' : ' || v_message, 'log');

                IF v_dev_phase != 'COMPLETE' OR v_dev_status != 'NORMAL'
                THEN
                   v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
                   rcv_error := NVL (rcv_error, 0) + 1;
                   rcv_complete := 0;
                   GOTO end_now;
                ELSE
                   von_pormtopo.write_log ('##### CONSIGN Return :  Check Processing_status_code #####', 'log');

                   BEGIN
                      SELECT   ERROR_CODE, error_explanation
                        INTO   v_process_status, x_return_status
                        FROM   mtl_transactions_interface
                       WHERE   transaction_interface_id = n_interface_seq;
                   EXCEPTION
                      WHEN NO_DATA_FOUND
                      THEN
                         rcv_complete := NVL (rcv_complete, 0) + 1;
                         rcv_error := 0;
                         GOTO end_now;
                   END;

                   write_log (' # 1 Check Bug  : x_return_status=' || x_return_status || ', v_process_status=' || v_process_status, 'log');

                   IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                   THEN
                      rcv_error := NVL (rcv_error, 0) + 1;
                      rcv_complete := 0;
                      GOTO end_now;
                   ELSE
                      --
                      IF fnd_concurrent.wait_for_request (v_req_id,
                                                          16,
                                                          1800,
                                                          v_phase,
                                                          v_status,
                                                          v_dev_phase,
                                                          v_dev_status,
                                                          v_message)
                      THEN
                         BEGIN
                            DBMS_LOCK.sleep (5);

                            SELECT   ERROR_CODE, error_explanation
                              INTO   v_process_status, x_return_status
                              FROM   mtl_transactions_interface
                             WHERE   transaction_interface_id = n_interface_seq;

                            von_pormtopo.write_log (
                               'on Consign Return Check 1.1 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status,
                               'log'
                            );
                         EXCEPTION
                            WHEN NO_DATA_FOUND
                            THEN
                               -- Complete
                               rcv_complete := NVL (rcv_complete, 0) + 1;
                               rcv_error := 0;
                               GOTO end_now;
                         END;

                         IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                         THEN
                            von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                            rcv_error := NVL (rcv_error, 0) + 1;
                            rcv_complete := 0;
                            GOTO end_now;
                         ELSE
                            BEGIN
                               DBMS_LOCK.sleep (10);

                               SELECT   ERROR_CODE, error_explanation
                                 INTO   v_process_status, x_return_status
                                 FROM   mtl_transactions_interface
                                WHERE   transaction_interface_id = n_interface_seq;

                               von_pormtopo.write_log (
                                  'on Consign Return Check 1.2 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status,
                                  'log'
                               );
                            EXCEPTION
                               WHEN NO_DATA_FOUND
                               THEN
                                  -- Complete
                                  rcv_complete := NVL (rcv_complete, 0) + 1;
                                  rcv_error := 0;
                                  GOTO end_now;
                            END;

                            IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                            THEN
                               von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                               rcv_error := NVL (rcv_error, 0) + 1;
                               rcv_complete := 0;
                               GOTO end_now;
                            ELSE
                               von_pormtopo.write_log ('Return -> Record does not process  ', 'log');
                               x_return_status := 'Record does not process ' || v_process_status || ' , ' || x_return_status;
                               rcv_error := NVL (rcv_error, 0) + 1;
                               rcv_complete := 0;
                               GOTO end_now;
                            END IF;
                         END IF;
                      ELSE
                         BEGIN
                            DBMS_LOCK.sleep (10);

                            SELECT   ERROR_CODE, error_explanation
                              INTO   v_process_status, x_return_status
                              FROM   mtl_transactions_interface
                             WHERE   transaction_interface_id = n_interface_seq;

                            von_pormtopo.write_log ('Check 1.2 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status,
                                                    'log');
                         EXCEPTION
                            WHEN NO_DATA_FOUND
                            THEN
                               -- Complete
                               rcv_complete := NVL (rcv_complete, 0) + 1;
                               rcv_error := 0;
                               GOTO end_now;
                         END;

                         IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                         THEN
                            von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                            rcv_error := NVL (rcv_error, 0) + 1;
                            rcv_complete := 0;
                            GOTO end_now;
                         ELSE
                            von_pormtopo.write_log ('Return -> Record does not process  ', 'log');
                            x_return_status := 'Record does not process ' || v_process_status || ' , ' || x_return_status;
                            rcv_error := NVL (rcv_error, 0) + 1;
                            rcv_complete := 0;
                            GOTO end_now;
                         END IF;
                      END IF;
                   --



                   --                     x_return_status := 'Record does not process';
                   --                     rcv_error := NVL (rcv_error, 0) + 1;
                   --                     rcv_complete := 0;
                   --                     GOTO end_now;
                   END IF;

                   apps.von_pormtopo.write_log ('#CONSIGN RETURN Complete : Transaction_Interface_ID =' || n_interface_seq, 'line');
                END IF;
             END IF;
          END IF;*/
         -- pcn @100630 - Delete Waiting
         rcv_complete := NVL (rcv_complete, 0) + 1;
         rcv_error := 0;

        -- pcn @100630 - Delete Waiting

        -- update to CNS_MSG, CNS_TRANSACTION_INTERFACE_ID,CONSIGN_STATUS
        -- Success CONSIGN_STATUS = 'Y' Error CONSIGN_STATUS ='E'

        <<end_now>>
         IF NVL (rcv_complete, 0) > 0
         THEN
            UPDATE /*+ CPU_COSTING  */
                  von_purchase_memo_dt
               SET cns_msg = p_msg_data,
                   cns_transaction_interface_id = n_interface_seq,
                   consign_status = 'Y',
                   last_updated_date = SYSDATE,
                   last_updated_by = x_userid,
                   last_update_login = fnd_global.login_id
             WHERE memo_line_id = x.memo_line_id;

            COMMIT;

            UPDATE von_receipt
               SET consign_return = 'Y',                                                                                                                                                  -- @ 29 Jul 10
                   last_updated_date = SYSDATE,
                   last_updated_by = x_userid,
                   last_update_login = fnd_global.login_id
             WHERE receipt_id = x_receipt_id;

            COMMIT;
         ELSE
            UPDATE /*+ CPU_COSTING  */
                  von_purchase_memo_dt
               SET cns_msg = p_msg_data,
                   cns_transaction_interface_id = n_interface_seq,
                   consign_status = 'E',
                   last_updated_date = SYSDATE,
                   last_updated_by = x_userid,
                   last_update_login = fnd_global.login_id
             WHERE memo_line_id = x.memo_line_id;

            COMMIT;

            UPDATE von_receipt
               SET consign_return = 'E',                                                                                                                                                  -- @ 29 Jul 10
                   last_updated_date = SYSDATE,
                   last_updated_by = x_userid,
                   last_update_login = fnd_global.login_id
             WHERE receipt_id = x_receipt_id;

            COMMIT;
         END IF;

         COMMIT;
      END LOOP;
   END consign_return_memo;

   PROCEDURE von_invoices (errbuf          OUT VARCHAR2,
                           retcode         OUT NUMBER,
                           userid              NUMBER,
                           p_login_id          NUMBER,
                           memo_num            VARCHAR2,
                           batch_num           VARCHAR2,
                           total_inv_amt   OUT NUMBER)
   AS
      nrequestid                   NUMBER;

      CURSOR cur_rcv_dt (
         rcv_hd_id    rcv_shipment_headers.shipment_header_id%TYPE)
      IS
         SELECT /*+ CPU_COSTING  */
               rcv_hd.shipment_header_id,
                rcv_ln.line_num,
                rcv_trans.quantity,
                rcv_trans.po_unit_price,
                rcv_trans.unit_of_measure uom,
                rcv_trans.currency_code,
                NVL (rcv_trans.quantity, 0) * NVL (rcv_trans.po_unit_price, 0) amount,
                rcv_hd.receipt_num,
                rcv_hd.ship_to_org_id org_id,
                rcv_ln.po_header_id,
                rcv_ln.po_line_id,
                rcv_ln.po_distribution_id,
                rcv_hd.attribute10
           FROM rcv_shipment_headers rcv_hd, rcv_shipment_lines rcv_ln, rcv_transactions rcv_trans
          WHERE     rcv_hd.shipment_header_id = rcv_hd_id
                AND rcv_hd.shipment_header_id = rcv_ln.shipment_header_id
                AND rcv_trans.shipment_header_id = rcv_ln.shipment_header_id
                AND rcv_trans.destination_type_code = 'INVENTORY'
                AND rcv_trans.transaction_type = 'DELIVER'
                AND rcv_trans.shipment_line_id = rcv_ln.shipment_line_id;

      -- Change By pcn @ 100817 Because Doplicate on Transaction Due to Return Transactions
      /*rcv_hd.shipment_header_id = rcv_hd_id
      AND rcv_hd.shipment_header_id = rcv_ln.shipment_header_id
      AND rcv_trans.destination_context = 'INVENTORY'
      AND rcv_trans.shipment_line_id = rcv_ln.shipment_line_id
      AND rcv_trans.shipment_header_id = rcv_ln.shipment_header_id;*/

      CURSOR memo_hd
      IS
         SELECT /*+ CPU_COSTING  */
                DISTINCT hd.*
           FROM von_purchase_memo_hd hd, von_purchase_memo_dt dt, von_receipt rc                                                                                                                     ---
          --                ,hr_organization_units_v org
          WHERE     hd.memo_header_id = dt.memo_header_id
                AND rc.receipt_id = dt.receipt_id                                                                                                                                                    ---
                AND dt.po_status = 'Y'
                AND dt.rcv_status = 'Y'
                AND (dt.ap_inv_status IS NULL OR dt.ap_inv_status <> 'Y')
                AND hd.memo_number LIKE ('%' || memo_num || '%')
                AND hd.rcv_number IS NOT NULL;

      --                AND org.organization_id=hd.ou_org_id;

      TYPE line_type IS VARRAY (100) OF VARCHAR2 (50);

      --table of varchar2(50) index by ps_number --
      v_line_type                  line_type := line_type ();

      TYPE v_codecombination_id IS TABLE OF NUMBER
         INDEX BY VARCHAR2 (50);

      v_code_combination_id        v_codecombination_id;

      TYPE amount IS TABLE OF NUMBER
         INDEX BY VARCHAR2 (50);

      v_amount                     amount;
      /*#########*/
      v_vendor_name                po_vendors.vendor_name%TYPE;
      v_vendor_code                po_vendors.segment1%TYPE;
      v_memo_date                  von_purchase_memo_hd.memo_date%TYPE;
      v_vendor_site_code           po_vendor_sites.vendor_site_code%TYPE;
      v_invoice_id                 ap_invoices_interface.invoice_id%TYPE;
      v_shipment_header_id         rcv_shipment_headers.shipment_header_id%TYPE;
      v_dist_code_combination_id   po_distributions_all.code_combination_id%TYPE;
      v_po_number                  po_headers_all.segment1%TYPE;
      v_po_line_num                po_lines_all.line_num%TYPE;
      v_line_num_carry             NUMBER DEFAULT 0;
      v_addition_line              NUMBER DEFAULT 0;
      v_addition_amount            NUMBER DEFAULT 0;
      v_sts                        BOOLEAN;
      v_phase                      VARCHAR2 (200);
      v_status                     VARCHAR2 (200);
      v_dev_phase                  VARCHAR2 (200);
      v_dev_status                 VARCHAR2 (200);
      v_message                    VARCHAR2 (200);
      v_program_status             VARCHAR2 (1);
      v_line_desc                  VARCHAR2 (500);
      vvendor_name                 VARCHAR2 (250);
      ninvoice_amount              NUMBER := 0;
      ntotal_invoice_amount        NUMBER := 0;
      vorg_name                    VARCHAR2 (200);
      vorg_code                    VARCHAR2 (100);
      v_vendor_site_id             NUMBER;
   BEGIN
      apps.von_pormtopo.write_log ('##########Start AP Interface########', 'log');

      -- Delete Only AP Interface which Releate Memo
      DELETE FROM /*+ CPU_COSTING  */
                 ap_invoice_lines_interface
            WHERE     attribute11 = 'PORM'
                  AND invoice_id IN (SELECT invoice_id
                                       FROM ap_invoices_interface
                                      WHERE attribute11 = 'PORM' AND invoice_num = memo_num);

      DELETE FROM /*+ CPU_COSTING  */
                 ap_invoices_interface
            WHERE attribute11 = 'PORM' AND invoice_num = memo_num;

      COMMIT;

      FOR one IN memo_hd
      LOOP
         apps.von_pormtopo.write_log ('receive number ' || one.rcv_number, 'log');

         BEGIN
            SELECT /*+ CPU_COSTING  */
                  org.name
              INTO vorg_name
              FROM hr_organization_units_v org, hr_organization_information_v inf
             WHERE inf.organization_id = org.organization_id AND inf.org_information1_meaning = 'Operating Unit' AND org.organization_id = one.ou_org_id;

            IF vorg_name LIKE '%VON Bundit - Bangkok%'
            THEN
               vorg_code := 'RUB-INV-BK';
            ELSIF vorg_name LIKE '%VON Bundit - Phuket%'
            THEN
               vorg_code := 'RUB-INV-PK';
            ELSIF vorg_name LIKE '%VON Bundit - Suratthani%'
            THEN
               vorg_code := 'RUB-INV-SN';
            ELSIF vorg_name LIKE '%VON Bundit - Krabi%'
            THEN
               vorg_code := 'RUB-INV-KB';
            END IF;
         --VON Bundit - Bangkok      STD-INV-BK
         --VON Bundit - Phuket        STD-INV-PK
         --VON Bundit - Suratthani   STD-INV-SN
         --VON Bundit - Krabi          STD-INV-KB

         EXCEPTION
            WHEN OTHERS
            THEN
               vorg_name := NULL;
         END;

         BEGIN
            SELECT /*+ CPU_COSTING  */
                  ap_invoices_interface_s.NEXTVAL INTO v_invoice_id FROM DUAL;
         EXCEPTION
            WHEN OTHERS
            THEN
               apps.von_pormtopo.write_log (SQLERRM, 'log');
               GOTO end_novalidate;
         END;

         /*########## Calculate addition line ##########*/
         v_line_type.EXTEND (4);

         IF one.total_deduct_amount IS NOT NULL AND one.total_deduct_amount > 0
         THEN
            v_addition_line := v_addition_line + 1;
            apps.von_pormtopo.write_log ('#########Index : ' || v_addition_line || '##########', 'log');
            v_line_type (v_addition_line) := 'DEDUCT';
            apps.von_pormtopo.write_log ('Index : ' || v_addition_line || ' Line Type : ' || v_line_type (v_addition_line), 'log');

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     code_combination_id
                 INTO v_code_combination_id (v_line_type (v_addition_line))
                 FROM von_acc_mapping
                WHERE whse_code = one.whse_code                                                                                                                                                    --S31
                                               AND item_id = one.item_id                                                                                                                          --2001
                                                                        AND ou_org_id = one.ou_org_id                                                                                               --85
                                                                                                     AND inv_org_id = one.inv_org_id                                                               --264
                                                                                                                                    AND acc_type = 1020;
            EXCEPTION
               WHEN OTHERS
               THEN
                  apps.von_pormtopo.write_log ('Error not found code combination id', 'log');
            END;

            apps.von_pormtopo.write_log (
               'Index : ' || v_addition_line || ' Line Type : ' || v_line_type (v_addition_line) || ' Code combination : ' || v_code_combination_id (v_line_type (v_addition_line)),
               'log');
            v_amount (v_line_type (v_addition_line)) := one.total_deduct_amount;
            v_addition_amount := v_addition_amount + one.total_deduct_amount;
            apps.von_pormtopo.write_log (
                  'Index : '
               || v_addition_line
               || ' Line Type : '
               || v_line_type (v_addition_line)
               || ' Code combination : '
               || v_code_combination_id (v_line_type (v_addition_line))
               || ' Amount : '
               || v_amount (v_line_type (v_addition_line)),
               'log');
         END IF;

         IF one.total_labor_amount IS NOT NULL AND one.total_labor_amount > 0
         THEN
            v_addition_line := v_addition_line + 1;
            apps.von_pormtopo.write_log ('#########Index : ' || v_addition_line || '##########', 'log');
            v_line_type (v_addition_line) := 'LABOR';
            apps.von_pormtopo.write_log ('Index : ' || v_addition_line || ' Line Type : ' || v_line_type (v_addition_line), 'log');

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     code_combination_id
                 INTO v_code_combination_id (v_line_type (v_addition_line))
                 FROM von_acc_mapping
                WHERE whse_code = one.whse_code                                                                                                                                                    --S31
                                               AND item_id = one.item_id                                                                                                                          --2001
                                                                        AND ou_org_id = one.ou_org_id                                                                                               --85
                                                                                                     AND inv_org_id = one.inv_org_id                                                               --264
                                                                                                                                    AND acc_type = 1010;
            EXCEPTION
               WHEN OTHERS
               THEN
                  apps.von_pormtopo.write_log ('Error not found code combination id', 'log');
            END;

            apps.von_pormtopo.write_log (
               'Index : ' || v_addition_line || ' Line Type : ' || v_line_type (v_addition_line) || ' Code combination : ' || v_code_combination_id (v_line_type (v_addition_line)),
               'log');
            v_amount (v_line_type (v_addition_line)) := one.total_labor_amount;
            v_addition_amount := v_addition_amount + one.total_labor_amount;
            apps.von_pormtopo.write_log (
                  'Index : '
               || v_addition_line
               || ' Line Type : '
               || v_line_type (v_addition_line)
               || ' Code combination : '
               || v_code_combination_id (v_line_type (v_addition_line))
               || ' Amount : '
               || v_amount (v_line_type (v_addition_line)),
               'log');
         END IF;

         IF one.total_retention_amount IS NOT NULL AND one.total_retention_amount > 0
         THEN
            v_addition_line := v_addition_line + 1;
            apps.von_pormtopo.write_log ('#########Index : ' || v_addition_line || '##########', 'log');
            v_line_type (v_addition_line) := 'RETENTION';
            apps.von_pormtopo.write_log ('Index : ' || v_addition_line || ' Line Type : ' || v_line_type (v_addition_line), 'log');

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     code_combination_id
                 INTO v_code_combination_id (v_line_type (v_addition_line))
                 FROM von_acc_mapping
                WHERE whse_code = one.whse_code                                                                                                                                                    --S31
                                               AND item_id = one.item_id                                                                                                                          --2001
                                                                        AND ou_org_id = one.ou_org_id                                                                                               --85
                                                                                                     AND inv_org_id = one.inv_org_id                                                               --264
                                                                                                                                    AND acc_type = 1030;
            EXCEPTION
               WHEN OTHERS
               THEN
                  apps.von_pormtopo.write_log ('Error not found code combination id', 'log');
            END;

            apps.von_pormtopo.write_log (
               'Index : ' || v_addition_line || ' Line Type : ' || v_line_type (v_addition_line) || ' Code combination : ' || v_code_combination_id (v_line_type (v_addition_line)),
               'log');
            v_amount (v_line_type (v_addition_line)) := one.total_retention_amount;
            v_addition_amount := v_addition_amount + one.total_retention_amount;
            apps.von_pormtopo.write_log (
                  'Index : '
               || v_addition_line
               || ' Line Type : '
               || v_line_type (v_addition_line)
               || ' Code combination : '
               || v_code_combination_id (v_line_type (v_addition_line))
               || ' Amount : '
               || v_amount (v_line_type (v_addition_line)),
               'log');
         END IF;

         /*########## Calculate addition line ##########*/
         BEGIN
            BEGIN
               SELECT /*+ CPU_COSTING  */
                     vendor_name
                 INTO vvendor_name
                 FROM po_vendors
                WHERE vendor_id = one.vendor_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  vvendor_name := NULL;
            END;

            BEGIN
               SELECT vendor_site_code
                 INTO v_vendor_site_code
                 FROM po_vendor_sites_all
                WHERE vendor_site_id = one.vendor_site_id AND vendor_id = one.vendor_id AND inactive_date IS NULL;

               v_vendor_site_id := one.vendor_site_id;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  BEGIN
                     SELECT vendor_site_id
                       INTO v_vendor_site_id
                       FROM po_vendor_sites_all
                      WHERE vendor_site_code = one.vendor_site_code AND vendor_id = one.vendor_id AND inactive_date IS NULL AND vendor_site_id = one.vendor_site_id;

                     v_vendor_site_code := one.vendor_site_code;
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        BEGIN
                             SELECT vendor_site_id
                               INTO v_vendor_site_id
                               FROM po_vendor_sites_all
                              WHERE vendor_site_code = one.vendor_site_code AND vendor_id = one.vendor_id AND inactive_date IS NULL AND org_id = one.ou_org_id
                           ORDER BY vendor_site_id DESC;

                           v_vendor_site_code := one.vendor_site_code;
                        EXCEPTION
                           WHEN NO_DATA_FOUND
                           THEN
                              v_vendor_site_id := one.vendor_site_id;
                              v_vendor_site_code := one.vendor_site_code;
                        END;
                  END;
            END;

            /*
            -- By PCN Delete Existing Date @ 20100713
            */
            BEGIN
               DELETE FROM ap_invoices_interface
                     WHERE attribute10 = one.memo_header_id AND attribute11 = 'PORM';

               DELETE FROM ap_invoice_lines_interface
                     WHERE attribute10 = one.memo_header_id AND attribute11 = 'PORM';

               COMMIT;
            END;

            --            ninvoice_amount :=one.total_memo_amount - v_addition_amount;

            ninvoice_amount := one.total_memo_amount - v_addition_amount;
            ntotal_invoice_amount := NVL (ntotal_invoice_amount, 0) + ninvoice_amount;

            INSERT INTO /*+ CPU_COSTING  */
                       ap_invoices_interface (invoice_id,
                                              invoice_num,
                                              invoice_type_lookup_code,
                                              invoice_date,
                                              vendor_id,
                                              vendor_num,
                                              vendor_name,                                                                                                                                            --
                                              vendor_site_id,
                                              vendor_site_code,
                                              invoice_amount,
                                              invoice_currency_code,
                                              org_id,
                                              terms_name,
                                              description,
                                              created_by,
                                              creation_date,
                                              last_updated_by,
                                              last_update_date,
                                              last_update_login,
                                              attribute10,
                                              attribute11,
                                              source,
                                              doc_category_code,
                                              awt_group_name,                                                                                                                  -- @ 28 Jun 10 on Go live
                                              awt_group_id                                                                                                                     -- @ 28 Jun 10 on Go live
                                                          )
                 VALUES (v_invoice_id,
                         one.memo_number,
                         'STANDARD',
                         one.memo_date,
                         one.vendor_id,
                         one.vendor_code,
                         vvendor_name,                                                                                                                                                                --
                         v_vendor_site_id,
                         v_vendor_site_code,
                         one.total_memo_amount - v_addition_amount,
                         'THB',
                         one.ou_org_id,
                         'Immediately',
                         one.item_no || '_' || one.total_memo_net_weight,
                         userid,
                         SYSDATE,
                         userid,
                         SYSDATE,
                         userid,
                         one.memo_header_id,
                         'PORM',
                         'PORM',
                         vorg_code,                                                                                                                                                       -- @ 15 Jun 10
                         NULL,
                         NULL);

            apps.von_pormtopo.write_log ('Insert complete !', 'log');
         EXCEPTION
            WHEN OTHERS
            THEN
               apps.von_pormtopo.write_log (SQLERRM, 'log');
               GOTO end_novalidate;
         END;

         -- Advance Find shipment_header_id
         BEGIN
            SELECT /*+ CPU_COSTING  */
                  hd.shipment_header_id
              INTO v_shipment_header_id
              FROM rcv_shipment_headers hd
             WHERE hd.receipt_num = one.rcv_number AND ship_to_org_id = one.inv_org_id;
         EXCEPTION
            WHEN TOO_MANY_ROWS
            THEN
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                         (hd.shipment_header_id)
                    INTO v_shipment_header_id
                    FROM rcv_shipment_headers hd
                   WHERE hd.receipt_num = one.rcv_number AND ship_to_org_id = one.inv_org_id AND hd.vendor_id = one.vendor_id AND hd.vendor_site_id = one.vendor_site_id;
               EXCEPTION
                  WHEN TOO_MANY_ROWS
                  THEN
                     BEGIN
                        SELECT /*+ CPU_COSTING  */
                               (hd.shipment_header_id)
                          INTO v_shipment_header_id
                          FROM rcv_shipment_headers hd
                         WHERE     hd.receipt_num = one.rcv_number
                               AND ship_to_org_id = one.inv_org_id
                               AND hd.vendor_id = one.vendor_id
                               AND hd.vendor_site_id = one.vendor_site_id
                               AND hd.attribute10 IN (SELECT dt.memo_line_id
                                                        FROM von_purchase_memo_dt dt
                                                       WHERE dt.memo_header_id = one.memo_header_id);
                     EXCEPTION
                        WHEN TOO_MANY_ROWS
                        THEN
                           SELECT MAX /*+ CPU_COSTING  */
                                      (hd.shipment_header_id)
                             INTO v_shipment_header_id
                             FROM rcv_shipment_headers hd
                            WHERE     hd.receipt_num = one.rcv_number
                                  AND ship_to_org_id = one.inv_org_id
                                  AND hd.vendor_id = one.vendor_id
                                  AND hd.vendor_site_id = one.vendor_site_id
                                  AND hd.attribute10 IN (SELECT dt.memo_line_id
                                                           FROM von_purchase_memo_dt dt
                                                          WHERE dt.memo_header_id = one.memo_header_id);
                        WHEN NO_DATA_FOUND
                        THEN
                           apps.von_pormtopo.write_log ('No Header_id found on receipt number : ' || one.rcv_number || ' Please check in rcv_shipment_headers.', 'log');
                           GOTO end_novalidate;
                     END;
                  WHEN NO_DATA_FOUND
                  THEN
                     apps.von_pormtopo.write_log ('No Header_id found on receipt number : ' || one.rcv_number || ' Please check in rcv_shipment_headers.', 'log');
                     GOTO end_novalidate;
               END;
            WHEN NO_DATA_FOUND
            THEN
               apps.von_pormtopo.write_log ('No Header_id found on receipt number : ' || one.rcv_number || ' Please check in rcv_shipment_headers.', 'log');
               GOTO end_novalidate;
         END;

         apps.von_pormtopo.write_log ('RCV Shipment Header id : ' || v_shipment_header_id, 'log');

         FOR two IN cur_rcv_dt (rcv_hd_id => v_shipment_header_id)
         LOOP
            BEGIN
               SELECT /*+ CPU_COSTING  */
                     code_combination_id
                 INTO v_dist_code_combination_id
                 FROM po_distributions_all
                WHERE po_distribution_id = two.po_distribution_id AND po_header_id = two.po_header_id AND po_line_id = two.po_line_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_dist_code_combination_id := NULL;
                  apps.von_pormtopo.write_log ('Error PO distribution code combination id .', 'log');
                  GOTO endlooptwo;
            END;

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     segment1 po_number
                 INTO v_po_number
                 FROM po_headers_all hd
                WHERE hd.po_header_id = two.po_header_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_po_number := NULL;
            END;

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     pol.line_num
                 INTO v_po_line_num
                 FROM po_lines_all pol
                WHERE pol.po_line_id = two.po_line_id;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_po_line_num := NULL;
            END;

            apps.von_pormtopo.write_log ('Memo line id: ' || two.attribute10, 'log');

            BEGIN
               SELECT /*+ CPU_COSTING  */
                     hd.item_no || '  ' || dt.memo_gross_weight || ' * ' || dt.memo_unit_price description
                 INTO v_line_desc
                 FROM von_purchase_memo_hd hd, von_purchase_memo_dt dt
                WHERE hd.memo_header_id = dt.memo_header_id AND dt.memo_line_id = two.attribute10;
            EXCEPTION
               WHEN OTHERS
               THEN
                  v_line_desc := 'error';
            END;

            BEGIN
               INSERT INTO /*+ CPU_COSTING  */
                          ap_invoice_lines_interface (invoice_id,
                                                      invoice_line_id,
                                                      line_number,
                                                      line_type_lookup_code,
                                                      amount,
                                                      quantity_invoiced,
                                                      unit_price,
                                                      unit_of_meas_lookup_code,
                                                      accounting_date,
                                                      awt_group_name,
                                                      created_by,
                                                      creation_date,
                                                      last_updated_by,
                                                      last_update_date,
                                                      last_update_login,
                                                      attribute10,
                                                      attribute11,
                                                      attribute12,
                                                      description,
                                                      po_header_id,
                                                      po_number,
                                                      po_line_id,
                                                      po_line_number,
                                                      receipt_number,
                                                      receipt_line_number,
                                                      match_option,
                                                      inventory_item_id)
                    VALUES (v_invoice_id,
                            ap_invoice_lines_interface_s.NEXTVAL,
                            two.line_num,
                            'ITEM',
                            ROUND (two.amount, 2),
                            ROUND (two.quantity, 0),
                            two.po_unit_price,
                            two.uom,
                            one.memo_date,
                            one.wht_code,
                            userid,
                            SYSDATE,
                            userid,
                            SYSDATE,
                            userid,
                            one.memo_header_id,
                            'PORM',
                            'ITEM_LINE',
                            v_line_desc,
                            two.po_header_id,
                            v_po_number,
                            two.po_line_id,
                            v_po_line_num,
                            two.receipt_num,
                            two.line_num,
                            'R',
                            one.item_id);
            EXCEPTION
               WHEN OTHERS
               THEN
                  GOTO end_novalidate;
            END;

            IF two.line_num > v_line_num_carry
            THEN
               v_line_num_carry := two.line_num;
            END IF;

           <<endlooptwo>>
            NULL;
         END LOOP two;

         FOR addition IN 1 .. v_addition_line
         LOOP
            BEGIN
               INSERT INTO /*+ CPU_COSTING  */
                          ap_invoice_lines_interface (invoice_id,
                                                      invoice_line_id,
                                                      line_number,
                                                      line_type_lookup_code,
                                                      amount,
                                                      accounting_date,
                                                      dist_code_combination_id,
                                                      description,
                                                      created_by,
                                                      creation_date,
                                                      last_updated_by,
                                                      last_update_date,
                                                      last_update_login,
                                                      attribute10,
                                                      attribute11,
                                                      attribute12,
                                                      awt_group_name,                                                                                                         -- @ 28 June 10 on Go live
                                                      awt_group_id                                                                                                            -- @ 28 June 10 on Go live
                                                                  )
                    VALUES (v_invoice_id,
                            ap_invoice_lines_interface_s.NEXTVAL,
                            v_line_num_carry + addition,
                            'ITEM',
                            ROUND (v_amount (v_line_type (addition)), 2) * (-1),
                            one.memo_date,
                            v_code_combination_id (v_line_type (addition)),
                            v_line_type (addition),
                            userid,
                            SYSDATE,
                            userid,
                            SYSDATE,
                            userid,
                            one.memo_header_id,
                            'PORM',
                            'ADDTIONAL_LINE',
                            NULL,
                            NULL);
            EXCEPTION
               WHEN OTHERS
               THEN
                  ROLLBACK;
                  GOTO end_novalidate;
            END;
         END LOOP addition;

         COMMIT;

         nrequestid :=
            fnd_request.submit_request (application   => 'SQLAP',
                                        program       => 'APXIIMPT',
                                        -- Payables Open Interface Import
                                        argument1     => NULL,
                                        argument2     => 'PORM',
                                        argument3     => NULL,
                                        argument4     => batch_num,
                                        argument5     => NULL,
                                        argument6     => NULL,
                                        argument7     => NULL,
                                        argument8     => 'N',
                                        argument9     => 'N',
                                        argument10    => 'N',
                                        argument11    => 'N',
                                        argument12    => '1000',
                                        argument13    => userid,
                                        argument14    => p_login_id);

         COMMIT;

         IF (nrequestid != 0)
         THEN
            IF fnd_concurrent.wait_for_request (nrequestid,
                                                6,
                                                1800,
                                                v_phase,
                                                v_status,
                                                v_dev_phase,
                                                v_dev_status,
                                                v_message)
            THEN
               apps.von_pormtopo.write_log (v_dev_status || ' : ' || v_message, 'log');

               IF v_dev_phase != 'COMPLETE' OR v_dev_status != 'NORMAL'
               THEN
                  BEGIN
                     SELECT /*+ CPU_COSTING  */
                           DECODE (status,  'REJECT', 'E',  'PROCESSED', 'C',  'E')
                       INTO v_program_status
                       FROM ap_invoices_interface
                      WHERE invoice_id = v_invoice_id;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        v_program_status := 'E';
                  END;

                  IF v_program_status = 'C'
                  THEN
                     BEGIN
                        UPDATE /*+ CPU_COSTING  */
                              von_purchase_memo_dt
                           SET ap_inv_status = 'Y', ap_inv_number = one.memo_number
                         WHERE memo_header_id = one.memo_header_id;

                        BEGIN
                           UPDATE /*+ CPU_COSTING  */
                                 ap_invoice_lines_all
                              SET awt_group_id = NULL
                            WHERE match_type = 'NOT_MATCHED' AND line_type_lookup_code = 'ITEM' AND invoice_id = v_invoice_id;

                           COMMIT;

                           UPDATE /*+ CPU_COSTING  */
                                 ap_invoice_lines_all
                              SET awt_group_id = NULL
                            WHERE     match_type = 'NOT_MATCHED'
                                  AND line_type_lookup_code = 'ITEM'
                                  AND invoice_id IN (SELECT hd.invoice_id
                                                       FROM ap_invoices_all hd
                                                      WHERE hd.invoice_num = one.memo_number);

                           COMMIT;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              apps.von_pormtopo.write_log ('CAN NOT UPDATE AP_INVOICE_LINES_ALL', 'LOG');
                        END;

                        BEGIN
                           UPDATE /*+ CPU_COSTING  */
                                 ap_invoices_all
                              SET awt_group_id = NULL, awt_flag = NULL
                            WHERE invoice_id = v_invoice_id;

                           COMMIT;

                           UPDATE /*+ CPU_COSTING  */
                                 ap_invoices_all
                              SET awt_group_id = NULL, awt_flag = NULL
                            WHERE invoice_id IN (SELECT hd.invoice_id
                                                   FROM ap_invoices_all hd
                                                  WHERE hd.invoice_num = one.memo_number);

                           COMMIT;
                        EXCEPTION
                           WHEN OTHERS
                           THEN
                              apps.von_pormtopo.write_log ('CAN NOT UPDATE AP_INVOICES_ALL', 'LOG');
                        END;

                        apps.von_pormtopo.write_log (
                           '#INVOICE Complete : ' || batch_num || '/' || one.memo_number || ' ----------------------- ยอด ' || TO_CHAR (ninvoice_amount, '999,999,999.99'),
                           'line');

                        UPDATE /*+ CPU_COSTING  */
                              von_purchase_memo_hd
                           SET ap_inv_batch_no = batch_num
                         WHERE memo_header_id = one.memo_header_id;

                        UPDATE /*+ CPU_COSTING  */
                              von_purchase_memo_dt
                           SET ap_inv_batch_no = batch_num
                         WHERE memo_header_id = one.memo_header_id;

                        COMMIT;
                     END;
                  ELSE
                     v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
                     apps.von_pormtopo.write_log ('Error: Invoice Interface on Memo: ' || one.memo_number || ') ', 'LOG');

                     UPDATE /*+ CPU_COSTING  */
                           von_purchase_memo_dt
                        SET ap_inv_status = v_program_status, ap_inv_number = one.memo_number
                      WHERE memo_header_id = one.memo_header_id;

                     COMMIT;
                  END IF;

                  COMMIT;
               ELSE
                  apps.von_pormtopo.write_log ('invoice id => ' || v_invoice_id, 'log');

                  BEGIN
                     SELECT /*+ CPU_COSTING  */
                           DECODE (status,  'REJECT', 'E',  'PROCESSED', 'C',  'E')
                       INTO v_program_status
                       FROM ap_invoices_interface
                      WHERE invoice_id = v_invoice_id;
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        v_program_status := 'E';
                  END;

                  apps.von_pormtopo.write_log ('Program status : ' || v_program_status || CHR (13) || 'Memo number : ' || one.memo_number || CHR (13) || 'Memo header id : ' || one.memo_header_id,
                                               'log');

                  IF v_program_status = 'C'
                  THEN
                     UPDATE /*+ CPU_COSTING  */
                           von_purchase_memo_dt
                        SET ap_inv_status = 'Y', ap_inv_number = one.memo_number
                      WHERE memo_header_id = one.memo_header_id;

                     BEGIN
                        SELECT /*+ CPU_COSTING  */
                              invoice_id
                          INTO v_invoice_id
                          FROM ap_invoices_all
                         WHERE invoice_num = one.memo_number;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           v_invoice_id := -1;
                           apps.von_pormtopo.write_log ('CAN NOT FIND INVOICE ID (INVOICE NUMBER ' || one.memo_number || ') ', 'LOG');
                     END;

                     BEGIN
                        UPDATE /*+ CPU_COSTING  */
                              ap_invoice_lines_all
                           SET awt_group_id = NULL
                         WHERE match_type = 'NOT_MATCHED' AND line_type_lookup_code = 'ITEM' AND invoice_id = v_invoice_id;

                        COMMIT;

                        UPDATE /*+ CPU_COSTING  */
                              ap_invoice_lines_all
                           SET awt_group_id = NULL
                         WHERE     match_type = 'NOT_MATCHED'
                               AND line_type_lookup_code = 'ITEM'
                               AND invoice_id IN (SELECT hd.invoice_id
                                                    FROM ap_invoices_all hd
                                                   WHERE hd.invoice_num = one.memo_number);

                        COMMIT;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           apps.von_pormtopo.write_log ('CAN NOT UPDATE AP_INVOICE_LINES_ALL', 'LOG');
                     END;

                     BEGIN
                        UPDATE /*+ CPU_COSTING  */
                              ap_invoices_all
                           SET awt_group_id = NULL, awt_flag = NULL
                         WHERE invoice_id = v_invoice_id;


                        UPDATE /*+ CPU_COSTING  */
                              ap_invoices_all
                           SET awt_group_id = NULL, awt_flag = NULL
                         WHERE invoice_id IN (SELECT hd.invoice_id
                                                FROM ap_invoices_all hd
                                               WHERE hd.invoice_num = one.memo_number);

                        COMMIT;
                     EXCEPTION
                        WHEN OTHERS
                        THEN
                           apps.von_pormtopo.write_log ('CAN NOT UPDATE AP_INVOICES_ALL', 'LOG');
                     END;

                     apps.von_pormtopo.write_log (
                        '#INVOICE Complete : ' || batch_num || '/' || one.memo_number || ' ----------------------- ยอด ' || TO_CHAR (ninvoice_amount, '999,999,999.99'),
                        'line');

                     --                     apps.von_pormtopo.
                     --                     write_log ('--------------------------------------------------------------- ยอด '
                     --                     || ninvoice_amount
                     --                     ,
                     --                                'line');

                     UPDATE /*+ CPU_COSTING  */
                           von_purchase_memo_hd
                        SET ap_inv_batch_no = batch_num
                      WHERE memo_header_id = one.memo_header_id;

                     UPDATE /*+ CPU_COSTING  */
                           von_purchase_memo_dt
                        SET ap_inv_batch_no = batch_num
                      WHERE memo_header_id = one.memo_header_id;

                     COMMIT;
                  ELSE
                     UPDATE /*+ CPU_COSTING  */
                           von_purchase_memo_dt
                        SET ap_inv_status = v_program_status, ap_inv_number = one.memo_number
                      WHERE memo_header_id = one.memo_header_id;

                     v_sts := fnd_concurrent.set_completion_status ('ERROR', 'Check output file for warnings');
                     apps.von_pormtopo.write_log ('Error: Invoice Interface on Memo: ' || one.memo_number || ') ', 'LOG');
                     COMMIT;
                  END IF;
               END IF;
            END IF;
         END IF;

         COMMIT;
         v_line_num_carry := 0;
         v_addition_line := 0;
         v_addition_amount := 0;
      END LOOP one;

      COMMIT;

      total_inv_amt := ntotal_invoice_amount;

     <<end_novalidate>>
      NULL;
   END von_invoices;

   FUNCTION get_po_status (p_memo_id NUMBER)
      RETURN VARCHAR2
   IS
      v   VARCHAR2 (1);
   BEGIN
      SELECT /*+ CPU_COSTING  */
             DISTINCT po_status
        INTO v
        FROM von_purchase_memo_dt
       WHERE memo_header_id = p_memo_id;

      RETURN v;
   EXCEPTION
      WHEN TOO_MANY_ROWS
      THEN
         RETURN 'E';
      WHEN NO_DATA_FOUND
      THEN
         RETURN 'E';
   END;

   FUNCTION get_rcv_status (p_memo_id NUMBER)
      RETURN VARCHAR2
   IS
      v   VARCHAR2 (1);
   BEGIN
      SELECT /*+ CPU_COSTING  */
             DISTINCT rcv_status
        INTO v
        FROM von_purchase_memo_dt
       WHERE memo_header_id = p_memo_id;

      RETURN v;
   EXCEPTION
      WHEN TOO_MANY_ROWS
      THEN
         RETURN 'E';
      WHEN NO_DATA_FOUND
      THEN
         RETURN 'E';
   END;

   FUNCTION get_cns_status (p_memo_id NUMBER)
      RETURN VARCHAR2
   IS
      v   VARCHAR2 (1);
   BEGIN
      --    select distinct cns_status into v
      --    from von_purchase_memo_dt
      --    where memo_header_id=p_memo_id;
      --
      RETURN NULL;
   EXCEPTION
      WHEN TOO_MANY_ROWS
      THEN
         RETURN 'E';
      WHEN NO_DATA_FOUND
      THEN
         RETURN 'E';
   END;

   PROCEDURE consign_receive (x_org_id          IN            NUMBER,
                              userid            IN            NUMBER,
                              x_receipt_id      IN            NUMBER,
                              rcv_complete         OUT        NUMBER,
                              rcv_error            OUT        NUMBER,
                              x_return_status      OUT NOCOPY VARCHAR2)
   AS
      v_agent_id                       NUMBER;
      v_sts                            BOOLEAN;
      v_request_id                     NUMBER (20) := NULL;
      v_phase                          VARCHAR2 (200);
      v_status                         VARCHAR2 (200);
      v_dev_phase                      VARCHAR2 (200);
      v_dev_status                     VARCHAR2 (200);
      v_message                        VARCHAR2 (200);
      v_req_id                         NUMBER (20) := NULL;
      v_header_interface_id            NUMBER;
      v_group_id                       NUMBER;
      v_chk_interface                  NUMBER;
      v_chk_transaction                NUMBER;
      v_chk_interface_transaction_id   NUMBER;
      v_final_chk_all                  NUMBER;
      v_final_chk_y                    NUMBER;
      v_process_status                 VARCHAR2 (100);
      v_flag                           VARCHAR2 (1);
      v_lot_no                         VARCHAR2 (30);
      v_grade                          VARCHAR2 (5);
      v_rcv_number                     NUMBER;                                                                                                                  ---von_purchase_memo_hd.rcv_number%TYPE;
      v_rcv_status                     VARCHAR2 (1);
      v_inferrmsg                      VARCHAR2 (2000);
      v_del_hd_total                   NUMBER;
      v_del_hd_n_flg                   NUMBER;
      v_del_hd_meaning                 VARCHAR2 (1);

      v_line_memo_id                   NUMBER;

      CURSOR cur_rec (c_line_id NUMBER)
      IS
         SELECT /*+ CPU_COSTING  */
               rc.receipt_number,
                --   NULL inv_transaction_interface_id,
                rc.receipt_id receipt_id,
                --   rc.receipt_numbe xr,
                --   NULL rcv_status,
                --   NULL rcv_interface_groups_id,
                --   NULL rcv_headers_interface_id,
                --   1 memo_line_id,
                rc.vendor_id vendor_id,
                rc.whse_code,
                --   null acomment,
                rc.receipt_date receipt_date,
                rc.grade grade,
                rc.item_no item_no,
                rc.ou_org_id ou_org_id,
                (SELECT loc.inventory_location_id
                   FROM mtl_item_locations loc
                  WHERE loc.organization_id = rc.inv_org_id AND loc.segment1 = rc.locator)
                   locator_id,
                rc.locator,
                rc.net_weight net_weight,
                mi.secondary_uom_code,
                rc.gross_weight gross_weight,
                rc.item_id item_id,
                rc.inv_org_id,
                rc.sub_inventory,
                mi.lot_control_code,
                mi.grade_control_flag,
                mi.primary_uom_code,
                mi.primary_unit_of_measure,
                rc.con_transaction_interface_id,
                rc.ROWID r_id
           FROM von_receipt rc, mtl_system_items mi
          WHERE document_status = 1020 AND NVL (consign_flag, 'E') <> 'Y' AND receipt_id = c_line_id AND rc.item_id = mi.inventory_item_id AND rc.inv_org_id = mi.organization_id;

      next_rec_num                     NUMBER;
      chk_ship_hd                      NUMBER;
      chk_ship_hd1                     NUMBER;
      rec_num                          NUMBER;
      ship_hd                          NUMBER;
      v_locator                        VARCHAR2 (81);
      v_memo_header_id                 NUMBER;
      n_interface_seq                  NUMBER;
      vsegment1                        VARCHAR2 (25);
      vsegment2                        VARCHAR2 (25);
      vsegment3                        VARCHAR2 (25);
      vsegment4                        VARCHAR2 (25);
      vsegment5                        VARCHAR2 (25);
      vsegment6                        VARCHAR2 (25);
      vsegment7                        VARCHAR2 (25);
      vsegment8                        VARCHAR2 (25);
      vdistribution_account_id         VARCHAR2 (100);
      ndistribution_account_id         NUMBER;
      ntransaction_type_id             NUMBER;
      ntransaction_action_id           NUMBER;
      nreceipt_date                    DATE;

      o_msg_count                      NUMBER;
      o_msg_data                       VARCHAR2 (200);
      o_return_status                  VARCHAR2 (1);
      o_row_id                         ROWID;
      o_lot_rec                        mtl_lot_numbers%ROWTYPE;
      o_sequence                       NUMBER;

      l_lot_rec                        mtl_lot_numbers%ROWTYPE;
      p_lot_rec                        mtl_lot_numbers%ROWTYPE;
      l_lot_uom_conv_rec               mtl_lot_uom_class_conversions%ROWTYPE;
      l_qty_update_tbl                 mtl_lot_uom_conv_pub.quantity_update_rec_type;

      p_msg_count                      NUMBER;
      p_msg_data                       VARCHAR2 (10000);
      p_return_status                  VARCHAR2 (10000);
   BEGIN
      rcv_complete := 0;
      rcv_error := 0;
      x_return_status := NULL;

      FOR rec IN cur_rec (x_receipt_id)
      LOOP
         IF rec.con_transaction_interface_id IS NOT NULL
         THEN
            DELETE /*+ CPU_COSTING  */
                  mtl_transaction_lots_interface
             WHERE transaction_interface_id = rec.con_transaction_interface_id;

            DELETE /*+ CPU_COSTING  */
                  mtl_transactions_interface
             WHERE transaction_interface_id = rec.con_transaction_interface_id;

            COMMIT;
         END IF;

         -- Checking Transaction Type
         BEGIN
            SELECT /*+ CPU_COSTING  */
                  transaction_type_id, transaction_action_id
              INTO ntransaction_type_id, ntransaction_action_id
              FROM mtl_transaction_types
             WHERE transaction_type_name = 'Consign Receive';
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               rcv_error := rcv_error + 1;
               x_return_status := 'Undefined Transaction Type Setup';
               GOTO end_now;
            WHEN TOO_MANY_ROWS
            THEN
               rcv_error := rcv_error + 1;
               x_return_status := 'To Many Transaction Types Setup for Consignment';
               GOTO end_now;
         END;

         -- Checking Accounting Distrubute
         BEGIN
            --
            SELECT /*+ CPU_COSTING  */
                  segment1,
                   segment2,
                   segment3,
                   segment4,
                   segment5,
                   segment6,
                   segment7,
                   segment8,
                   account,
                   code_combination_id
              INTO vsegment1,
                   vsegment2,
                   vsegment3,
                   vsegment4,
                   vsegment5,
                   vsegment6,
                   vsegment7,
                   vsegment8,
                   vdistribution_account_id,
                   ndistribution_account_id
              FROM von_acc_mapping
             WHERE whse_code = rec.whse_code AND acc_type = 1040 AND item_id = rec.item_id AND ou_org_id = rec.ou_org_id;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               vsegment1 := NULL;
               vsegment2 := NULL;
               vsegment3 := NULL;
               vsegment4 := NULL;
               vsegment5 := NULL;
               vsegment6 := NULL;
               vsegment7 := NULL;
               vsegment8 := NULL;
               vdistribution_account_id := NULL;
               ndistribution_account_id := NULL;


               rcv_error := rcv_error + 1;
               x_return_status := 'Undefined Account Code Setup';
               GOTO end_now;
         END;

         -- Defind Transactoin SQ
         SELECT /*+ CPU_COSTING  */
               mtl_transactions_interface_s.NEXTVAL INTO n_interface_seq FROM DUAL;

         -- Checkg Receipt Date
         IF apps.inv_txn_manager_pub.get_open_period (rec.inv_org_id, rec.receipt_date, 1) = 0 OR apps.inv_txn_manager_pub.get_open_period (rec.inv_org_id, rec.receipt_date, 1) IS NULL
         THEN
            -- Peroid Receipt Date is Close
            IF apps.inv_txn_manager_pub.get_open_period (rec.inv_org_id, SYSDATE, 1) = 0 OR apps.inv_txn_manager_pub.get_open_period (rec.inv_org_id, SYSDATE, 1) IS NULL
            THEN
               rcv_error := rcv_error + 1;
               x_return_status := 'Receipt Date and Current Date not in Open Period';
               GOTO end_now;
            ELSE
               nreceipt_date := SYSDATE;
            END IF;
         ELSE
            -- Period Reciept Date is Open
            nreceipt_date := rec.receipt_date;
         END IF;

         --MTL_transactions_interface
         --PROCESS_FLAG = 1, LOCK_FLAG =NULL,REQUEST_ID = NULL,PROGRAM_ID = NULL

         -- Not use
         /*xxwip_wolc_plc_insert (p_item_number => rec.item_no, --Part Number
                p_organization_id => rec.inv_org_id, --Organization ID
               p_transaction_quantity => rec.net_weight, --Transaction Quantity
              p_transaction_uom => rec.secondary_uom_code, --Transaction UOM
                p_source_code => 'Consign_RC as ' || TO_CHAR (SYSDATE, 'DD-MON-RRRR'),    --Source Code, 6 characters
                p_interface_seq => n_interface_seq,
                p_result => p_return_status
               );*/

         INSERT INTO /*+ CPU_COSTING  */
                    mtl_transactions_interface (transaction_interface_id,
                                                source_code,
                                                source_line_id,
                                                source_header_id,
                                                process_flag,
                                                lock_flag,
                                                request_id,
                                                program_id,
                                                transaction_mode,
                                                validation_required,
                                                transaction_action_id,
                                                last_update_date,
                                                last_updated_by,
                                                creation_date,
                                                created_by,
                                                last_update_login,
                                                organization_id,
                                                transaction_quantity,
                                                transaction_uom,
                                                transaction_date,
                                                transaction_type_id,
                                                inventory_item_id,
                                                transaction_source_name,
                                                item_segment1,
                                                subinventory_code,
                                                locator_id,
                                                locator_name,
                                                loc_segment1,
                                                secondary_transaction_quantity,
                                                secondary_uom_code,
                                                transaction_cost,
                                                dst_segment1,
                                                dst_segment2,
                                                dst_segment3,
                                                dst_segment4,
                                                dst_segment5,
                                                dst_segment6,
                                                dst_segment7,
                                                dst_segment8,
                                                distribution_account_id)
              VALUES (n_interface_seq,                                                                                                                                      -- transaction_interface_id,
                      rec.receipt_number,
                      --TO_CHAR ('CONSIGN_RC_' || rec.receipt_number),                                                            --     source_code,
                      rec.receipt_id,                                                                                                                                                 -- source_line_id,
                      rec.receipt_id,                                                                                                                                               -- source_header_id,
                      1,                                                                                                                                                            -- process_flag is 1
                      NULL,                                                                                                                                                                  --lock_flag
                      NULL,                                                                                                                                                                -- request_ID
                      NULL,                                                                                                                                                                -- Program ID
                      3,                                                                                                                                             -- transaction_mode Backgroung is 3
                      1,                                                                                                                                                         -- validation_required,
                      ntransaction_action_id,                                                                                                                                 --  transaction_action_id,
                      SYSDATE,                                                                                                                                                       --last_update_date,
                      userid,                                                                                                                                                               -- update By
                      SYSDATE,                                                                                                                                                            -- Create Date
                      userid,                                                                                                                                                              --- Create By
                      fnd_global.login_id,                                                                                                                                          --last_update_login,
                      rec.inv_org_id,                                                                                                                                                -- organization_id,
                      ROUND (TO_NUMBER (rec.net_weight), 5),                                                                                                                    -- transaction_quantity,
                      rec.primary_uom_code,                                                                                                                                           -- transaction_uom
                      nreceipt_date,                                                                                                                                                 -- transaction_date
                      ntransaction_type_id,                                                                                                               --nTransaction_type_id, --transaction_type_id,
                      rec.item_id,                                                                                                                                                 -- inventory_item_id,
                      rec.receipt_number,                                                                                                                                     --transaction_source_name,
                      rec.item_no,                                                                                                                                                     -- item_segment1,
                      rec.sub_inventory,                                                                                                                                           -- subinventory_code,
                      rec.locator_id,                                                                                                                                                     -- locator_id,
                      rec.locator,                                                                                                                                                      -- locator_name,
                      rec.locator,                                                                                                                                                      -- loc_segment1,
                      ROUND (TO_NUMBER (rec.gross_weight), 5),                                                                                                         -- secondary_transaction_quantity
                      rec.secondary_uom_code,                                                                                                                                     -- secondary_uom_code,
                      NULL,                                                                                                                                                           --TRANSACTION_COST
                      vsegment1,
                      vsegment2,
                      vsegment3,                                                                                                                                                          --DST_SEGMENT3
                      vsegment4,                                                                                                                                                          --DST_SEGMENT4
                      vsegment5,                                                                                                                                                          --DST_SEGMENT5
                      vsegment6,                                                                                                                                                          --DST_SEGMENT6
                      vsegment7,                                                                                                                                                          --DST_SEGMENT7
                      vsegment8,                                                                                                                                                         --DST_SEGMENT8,
                      ndistribution_account_id);
                      
         /*+ CPU_COSTING  */
         INSERT INTO 
                    mtl_transactions_iface_logs (transaction_interface_id,
                                                source_code,
                                                source_line_id,
                                                source_header_id,
                                                process_flag,
                                                lock_flag,
                                                request_id,
                                                program_id,
                                                transaction_mode,
                                                validation_required,
                                                transaction_action_id,
                                                last_update_date,
                                                last_updated_by,
                                                creation_date,
                                                created_by,
                                                last_update_login,
                                                organization_id,
                                                transaction_quantity,
                                                transaction_uom,
                                                transaction_date,
                                                transaction_type_id,
                                                inventory_item_id,
                                                transaction_source_name,
                                                item_segment1,
                                                subinventory_code,
                                                locator_id,
                                                locator_name,
                                                loc_segment1,
                                                secondary_transaction_quantity,
                                                secondary_uom_code,
                                                transaction_cost,
                                                dst_segment1,
                                                dst_segment2,
                                                dst_segment3,
                                                dst_segment4,
                                                dst_segment5,
                                                dst_segment6,
                                                dst_segment7,
                                                dst_segment8,
                                                distribution_account_id)
              VALUES (n_interface_seq,                                                                                                                                 
                      rec.receipt_number,
                      rec.receipt_id,                                                                                                                                              
                      rec.receipt_id,                                                                                                                                           
                      1,                                                                                                                                                          
                      NULL,                                                                                                                                                             
                      NULL,                                                                                                                                                             
                      NULL,                                                                                                                                                             
                      3,                                                                                                                                            
                      1,                                                                                                                                                     
                      ntransaction_action_id,                                                                                                                                
                      SYSDATE,                                                                                                                                                    
                      userid,                                                                                                                                                            
                      SYSDATE,                                                                                                                                                         
                      userid,                                                                                                                                                             
                      fnd_global.login_id,                                                                                                                                        
                      rec.inv_org_id,                                                                                                                                               
                      ROUND (TO_NUMBER (rec.net_weight), 5),                                                                                                                   
                      rec.primary_uom_code,                                                                                                                                          
                      nreceipt_date,                                                                                                                                                 
                      ntransaction_type_id,                                                                                                              
                      rec.item_id,                                                                                                                                            
                      rec.receipt_number,                                                                                                                                    
                      rec.item_no,                                                                                                                                                    
                      rec.sub_inventory,                                                                                                                                         
                      rec.locator_id,                                                                                                                                                    
                      rec.locator,                                                                                                                                                    
                      rec.locator,                                                                                                                                                 
                      ROUND (TO_NUMBER (rec.gross_weight), 5),                                                                                                       
                      rec.secondary_uom_code,                                                                                                                                  
                      NULL,                                                                                                                                                         
                      vsegment1,
                      vsegment2,
                      vsegment3,                                                                                                                                                          
                      vsegment4,                                                                                                                                                          
                      vsegment5,                                                                                                                                                          
                      vsegment6,                                                                                                                                                         
                      vsegment7,                                                                                                                                                          
                      vsegment8,                                                                                                                                                        
                      ndistribution_account_id);

         COMMIT;

         -- Lot Control
         IF rec.lot_control_code = 2                                                                                                                                                          -- Control
         THEN
            IF rec.grade_control_flag = 'Y'
            THEN
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        *
                    INTO l_lot_rec
                    FROM mtl_lot_numbers
                   WHERE organization_id = rec.ou_org_id AND inventory_item_id = rec.item_id AND lot_number = TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number;

                  o_return_status := 'S';
                  o_msg_count := 1;
                  o_msg_data := 'Already Exist';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     -- Create Lot
                     -- Define Lot
                     l_lot_rec.organization_id := rec.inv_org_id;                                                                                                                      -- rec.ou_org_id;
                     l_lot_rec.inventory_item_id := rec.item_id;
                     l_lot_rec.lot_number := TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number;
                     l_lot_rec.grade_code := rec.grade;
                     l_lot_rec.status_id := 1;

                     apps.von_opm.create_lot (p_orgn_id         => rec.inv_org_id,
                                              p_item_id         => rec.item_id,
                                              p_lot_no          => TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number,
                                              p_grade           => rec.grade,
                                              p_date            => rec.receipt_date,
                                              p_conv            => TO_CHAR (rec.net_weight / rec.gross_weight),
                                              p_uom_code        => rec.primary_uom_code,
                                              p_uom_code2       => rec.secondary_uom_code,
                                              p_user_id         => userid,
                                              o_msg_count       => p_msg_count,
                                              o_msg_data        => p_msg_data,
                                              o_return_status   => p_return_status);
                     COMMIT;

                     IF NVL (p_return_status, 'E') = 'S'
                     THEN
                        DBMS_OUTPUT.put_line ('Success');
                        GOTO insert_lot_inf;
                     ELSE
                        -- Error on Create Lot
                        DBMS_OUTPUT.put_line ('No_Success');
                        DBMS_OUTPUT.put_line (p_msg_data);
                        rcv_error := rcv_error + 1;
                        x_return_status := p_msg_data;
                        GOTO end_now;
                     END IF;
               END;
            ELSE                                                                                                                                                             -- GRADE_CONTROL_FLAG = 'Y'
               BEGIN
                  SELECT /*+ CPU_COSTING  */
                        *
                    INTO l_lot_rec
                    FROM mtl_lot_numbers
                   WHERE organization_id = rec.ou_org_id AND inventory_item_id = rec.item_id AND lot_number = TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number;

                  o_return_status := 'S';
                  o_msg_count := 1;
                  o_msg_data := 'Already Exist';
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     -- Create Lot
                     -- Define Lot
                     l_lot_rec.organization_id := rec.ou_org_id;
                     l_lot_rec.inventory_item_id := rec.inv_org_id;
                     l_lot_rec.lot_number := TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number;
                     l_lot_rec.status_id := 1;

                     apps.von_opm.create_lot (p_orgn_id         => rec.inv_org_id,
                                              p_item_id         => rec.item_id,
                                              p_lot_no          => TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number,
                                              p_grade           => rec.grade,
                                              p_date            => rec.receipt_date,
                                              p_conv            => TO_CHAR (rec.net_weight / rec.gross_weight),
                                              p_uom_code        => rec.primary_uom_code,
                                              p_uom_code2       => rec.secondary_uom_code,
                                              p_user_id         => userid,
                                              o_msg_count       => p_msg_count,
                                              o_msg_data        => p_msg_data,
                                              o_return_status   => p_return_status);

                     IF NVL (p_return_status, 'E') = 'S'
                     THEN
                        DBMS_OUTPUT.put_line ('Success');
                        GOTO insert_lot_inf;
                     ELSE
                        -- Error on Create Lot
                        DBMS_OUTPUT.put_line ('No_Success');
                        DBMS_OUTPUT.put_line (p_msg_data);
                        rcv_error := rcv_error + 1;
                        x_return_status := p_msg_data;
                        GOTO end_now;
                     END IF;
               END;
            END IF;

           <<insert_lot_inf>>
            INSERT INTO /*+ CPU_COSTING  */
                       mtl_transaction_lots_interface (transaction_interface_id,
                                                       process_flag,
                                                       last_update_date,
                                                       last_updated_by,
                                                       creation_date,
                                                       created_by,
                                                       last_update_login,
                                                       lot_number,
                                                       transaction_quantity,
                                                       --primary_quantity,
                                                       secondary_transaction_quantity,
                                                       grade_code,
                                                       source_code,
                                                       source_line_id,
                                                       vendor_id,
                                                       vendor_name,
                                                       attribute1)
                 VALUES (n_interface_seq,
                         1,                                                                                                                                                             -- process_flag,
                         SYSDATE,
                         userid,                                                                                                                                                   --fnd_global.user_id,
                         SYSDATE,
                         userid,                                                                                                                                                   --fnd_global.user_id,
                         fnd_global.login_id,
                         TO_CHAR (rec.receipt_date, 'YYMMDD') || '/' || rec.receipt_number,
                         ROUND (TO_NUMBER (rec.net_weight), 5),                                                                                                                  -- transaction_quantity
                         --ROUND (TO_NUMBER (rec.net_weight), 5), -- primary_quantity,
                         ROUND (TO_NUMBER (rec.gross_weight), 5),                                                                                                      --secondary_transaction_quantity,
                         rec.grade,
                         rec.receipt_number,
                         --TO_CHAR ('CONSIGN_RC_' || rec.receipt_number),                                                       --     source_code,
                         rec.receipt_id,
                         rec.vendor_id,
                         NULL,                                                                                                                                                  -- Addtional Vendor Name
                         rec.receipt_number);


            COMMIT;
         END IF;

         -- Update Interface Transaction ID
         UPDATE /*+ CPU_COSTING  */
               von_receipt rc
            SET rc.con_transaction_interface_id = n_interface_seq
          WHERE rc.ROWID = rec.r_id;

         COMMIT;



         v_req_id :=
            fnd_request.submit_request ('INV',                                                                                                                                            -- application
                                        'INCTCM',                                                                                                                                             -- program
                                        'Consignment Inventory Transactions Processor',
                                        --NULL,         --'Inventory Transaction Worker', description optional)
                                        NULL,                                                                                                                                   -- start_time (optional)
                                        FALSE,                                                                                                                                            -- sub_request
                                        '',                                                                                                 --to_char(v_group_id), -- header id to process not used here
                                        '1',                                                                                                                 -- 1 is the interface table 2 is temp table
                                        '',                                                                                                                                 -- group to process required
                                        rec.receipt_number,                                                                                                                         -- source to process
                                        --chr(0),
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '',
                                        '');
         COMMIT;
         DBMS_OUTPUT.put_line ('reg id=' || v_req_id);

         IF (v_req_id != 0)
         THEN
            IF fnd_concurrent.wait_for_request (v_req_id,
                                                16,
                                                1800,
                                                v_phase,
                                                v_status,
                                                v_dev_phase,
                                                v_dev_status,
                                                v_message)
            THEN
               DBMS_LOCK.sleep (5);
               COMMIT;
               von_pormtopo.write_log (v_dev_status || ' : ' || v_message, 'log');

               IF v_dev_phase != 'COMPLETE' OR v_dev_status != 'NORMAL'
               THEN
                  von_pormtopo.write_log ('Return ->  Please Check on Transaction Interface output file for warnings', 'log');
                  v_sts := fnd_concurrent.set_completion_status ('ERROR', ' Please Check on Transaction Interface output file for warnings');
                  rcv_error := NVL (rcv_error, 0) + 1;
                  rcv_complete := 0;
                  GOTO end_now;
               ELSE
                  von_pormtopo.write_log ('##### Check Processing_status_code #####', 'log');

                  BEGIN
                     SELECT /*+ CPU_COSTING  */
                           ERROR_CODE, error_explanation
                       INTO v_process_status, x_return_status
                       FROM mtl_transactions_interface
                      WHERE transaction_interface_id = n_interface_seq;

                     von_pormtopo.write_log ('Check 1 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status, 'log');
                  EXCEPTION
                     WHEN NO_DATA_FOUND
                     THEN
                        -- Complete
                        rcv_complete := NVL (rcv_complete, 0) + 1;
                        rcv_error := 0;
                        GOTO end_now;
                  END;

                  IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                  THEN
                     von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                     rcv_error := NVL (rcv_error, 0) + 1;
                     rcv_complete := 0;
                     GOTO end_now;
                  ELSE
                     IF fnd_concurrent.wait_for_request (v_req_id,
                                                         16,
                                                         1800,
                                                         v_phase,
                                                         v_status,
                                                         v_dev_phase,
                                                         v_dev_status,
                                                         v_message)
                     THEN
                        BEGIN
                           DBMS_LOCK.sleep (5);

                           SELECT ERROR_CODE, error_explanation
                             INTO v_process_status, x_return_status
                             FROM mtl_transactions_interface
                            WHERE transaction_interface_id = n_interface_seq;

                           von_pormtopo.write_log ('Check 1.1 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status, 'log');
                        EXCEPTION
                           WHEN NO_DATA_FOUND
                           THEN
                              -- Complete
                              rcv_complete := NVL (rcv_complete, 0) + 1;
                              rcv_error := 0;
                              GOTO end_now;
                        END;

                        IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                        THEN
                           von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                           rcv_error := NVL (rcv_error, 0) + 1;
                           rcv_complete := 0;
                           GOTO end_now;
                        ELSE
                           BEGIN
                              DBMS_LOCK.sleep (10);

                              SELECT /*+ CPU_COSTING  */
                                    ERROR_CODE, error_explanation
                                INTO v_process_status, x_return_status
                                FROM mtl_transactions_interface
                               WHERE transaction_interface_id = n_interface_seq;

                              von_pormtopo.write_log ('Check 1.2 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status, 'log');
                           EXCEPTION
                              WHEN NO_DATA_FOUND
                              THEN
                                 -- Complete
                                 rcv_complete := NVL (rcv_complete, 0) + 1;
                                 rcv_error := 0;
                                 GOTO end_now;
                           END;

                           IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                           THEN
                              von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                              rcv_error := NVL (rcv_error, 0) + 1;
                              rcv_complete := 0;
                              GOTO end_now;
                           ELSE
                              von_pormtopo.write_log ('Return -> Record does not process  ', 'log');
                              x_return_status := 'Record does not process ' || v_process_status || ' , ' || x_return_status;
                              rcv_error := NVL (rcv_error, 0) + 1;
                              rcv_complete := 0;
                              GOTO end_now;
                           END IF;
                        END IF;
                     ELSE
                        BEGIN
                           DBMS_LOCK.sleep (10);

                           SELECT /*+ CPU_COSTING  */
                                 ERROR_CODE, error_explanation
                             INTO v_process_status, x_return_status
                             FROM mtl_transactions_interface
                            WHERE transaction_interface_id = n_interface_seq;

                           von_pormtopo.write_log ('Check 1.2 # v_process_status=> ' || v_process_status || ' Return_status= ' || x_return_status, 'log');
                        EXCEPTION
                           WHEN NO_DATA_FOUND
                           THEN
                              -- Complete
                              rcv_complete := NVL (rcv_complete, 0) + 1;
                              rcv_error := 0;
                              GOTO end_now;
                        END;

                        IF v_process_status IS NOT NULL OR x_return_status IS NOT NULL
                        THEN
                           von_pormtopo.write_log ('Return -> Record Error:' || ' ' || x_return_status, 'log');
                           rcv_error := NVL (rcv_error, 0) + 1;
                           rcv_complete := 0;
                           GOTO end_now;
                        ELSE
                           von_pormtopo.write_log ('Return -> Record does not process  ', 'log');
                           x_return_status := 'Record does not process ' || v_process_status || ' , ' || x_return_status;
                           rcv_error := NVL (rcv_error, 0) + 1;
                           rcv_complete := 0;
                           GOTO end_now;
                        END IF;
                     END IF;
                  END IF;
               END IF;
            ELSE
               von_pormtopo.write_log ('Return -> Request ID = 0 or Null', 'log');
               x_return_status := 'Return -> Request ID = 0 or Null';
               rcv_error := NVL (rcv_error, 0) + 1;
               rcv_complete := 0;
               GOTO end_now;
            END IF;
         END IF;

        <<end_now>>
         von_pormtopo.write_log ('Check 2 # rcv_complete_waiting => ' || rcv_complete, 'log');


         IF NVL (rcv_complete, 0) > 0
         THEN
            UPDATE /*+ CPU_COSTING  */
                  von_receipt rc
               SET consign_flag = 'Y',
                   con_if_msg = NULL,
                   con_transaction_interface_id = n_interface_seq,
                   last_updated_date = SYSDATE,
                   last_updated_by = userid,
                   last_update_login = fnd_global.login_id
             WHERE rc.ROWID = rec.r_id;
         ELSE
            UPDATE /*+ CPU_COSTING  */
                  von_receipt rc
               SET consign_flag = 'E',
                   con_if_msg = x_return_status,
                   con_transaction_interface_id = n_interface_seq,
                   last_updated_date = SYSDATE,
                   last_updated_by = userid,
                   last_update_login = fnd_global.login_id
             WHERE rc.ROWID = rec.r_id;

            v_sts := fnd_concurrent.set_completion_status ('ERROR', ' Please Check on Transaction Interface output file for warnings');
         END IF;

         COMMIT;
      END LOOP;
   END consign_receive;
   
FUNCTION get_mtl_duplicate_rows (p_source_code          VARCHAR2,
                                 p_source_line_id       NUMBER,
                                 p_source_header_id     NUMBER,
                                 p_transaction_date     DATE,
                                 p_subinventory_code    VARCHAR2)
   RETURN NUMBER
IS
   c_existing   NUMBER;
BEGIN
   SELECT COUNT (*)
     INTO c_existing
     FROM mtl_transactions_iface_logs mti
    WHERE     mti.source_code = p_source_code
          AND mti.source_line_id = p_source_line_id
          AND mti.source_header_id = p_source_header_id
          AND mti.transaction_date = p_transaction_date
          AND mti.subinventory_code = p_subinventory_code;

   RETURN c_existing;
EXCEPTION
   WHEN OTHERS
   THEN
      RETURN 0;
END;   
END von_pormtopo;
/