/* Formatted on 9/7/2017 12:54:14 PM (QP5 v5.256.13226.35538) */
DECLARE
   l_source_code         VARCHAR2 (200) := '0342-110-15000855';
   l_source_line_id      NUMBER;
   l_source_header_id    NUMBER;
   l_transaction_date    DATE;
   l_subinventory_code   VARCHAR2 (100);

   c_existing            NUMBER;
BEGIN
   SELECT transaction_date
     INTO l_transaction_date
     FROM mtl_transactions_iface_logs
    WHERE ROWNUM = 1;

   SELECT COUNT (*)
     INTO c_existing
     FROM mtl_transactions_iface_logs mti
    WHERE     mti.source_code = l_source_code
          AND mti.transaction_date = l_transaction_date;

   /*Print output to debuging*/
   DBMS_OUTPUT.put_line (
         ' rows exist : '
      || TO_CHAR (c_existing)
      || CHR (10)
      || ' source code : '
      || TO_CHAR (l_source_code)
      || CHR (10)
      || ' source line id : '
      || TO_CHAR (l_source_line_id)
      || CHR (10)
      || ' source header id : '
      || TO_CHAR (l_source_header_id)
      || CHR (10)
      || ' transaction date : '
      || TO_CHAR (l_transaction_date, 'dd/mm/yyyy hh:mm:ss')
      || CHR (10)
      || ' subinventory code : '
      || TO_CHAR (l_subinventory_code));
EXCEPTION
   WHEN OTHERS
   THEN
      /*Print output to debuging*/
      DBMS_OUTPUT.put_line (' rows exist : 0');
END;