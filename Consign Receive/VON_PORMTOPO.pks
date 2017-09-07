CREATE OR REPLACE PACKAGE APPS.von_pormtopo
   AUTHID CURRENT_USER
AS
   PROCEDURE write_log (buff VARCHAR2, logtype VARCHAR2);


   FUNCTION get_buyerid (userid NUMBER)
      RETURN NUMBER;

   FUNCTION upd_memo (p_header_id NUMBER)
      RETURN VARCHAR2;

   PROCEDURE main (errbuf       OUT VARCHAR2,
                   retcode      OUT VARCHAR2,
                   userid           NUMBER,
                   p_org_id         NUMBER,
                   p_batch_no       VARCHAR2 DEFAULT NULL ,
                   p_memo_num       VARCHAR2,
                   p_memo_row       NUMBER);

   PROCEDURE receive_po (x_org_id             IN     NUMBER,
                         userid               IN     NUMBER,
                         x_ponum              IN     VARCHAR2,
                         rcv_complete            OUT NUMBER,
                         rcv_error               OUT NUMBER,
                         o_lot_number            OUT VARCHAR2,
                         o_transaction_qty       OUT NUMBER,
                         o_second_trans_qty      OUT NUMBER);

   PROCEDURE consign_return_memo (x_org_id             IN            NUMBER,
                                  x_userid             IN            NUMBER,
                                  x_memo_line_id       IN            NUMBER,
                                  x_receipt_id         IN            NUMBER,
                                  x_lot_number         IN            VARCHAR2,
                                  x_transaction_qty    IN            NUMBER,                                                -- receive from receive_po
                                  x_second_trans_qty   IN            NUMBER,                                                -- receive from receive_po
                                  rcv_complete            OUT        NUMBER,
                                  rcv_error               OUT        NUMBER,
                                  x_return_status         OUT NOCOPY VARCHAR2);

   PROCEDURE von_invoices (errbuf          OUT VARCHAR2,
                           retcode         OUT NUMBER,
                           userid              NUMBER,
                           p_login_id          NUMBER,
                           memo_num            VARCHAR2,
                           batch_num           VARCHAR2,
                           total_inv_amt   OUT NUMBER);

   FUNCTION get_po_status (p_memo_id NUMBER)
      RETURN VARCHAR2;

   FUNCTION get_rcv_status (p_memo_id NUMBER)
      RETURN VARCHAR2;

   FUNCTION get_cns_status (p_memo_id NUMBER)
      RETURN VARCHAR2;

   PROCEDURE consign_receive (x_org_id          IN            NUMBER,
                              userid            IN            NUMBER,
                              x_receipt_id      IN            NUMBER,
                              rcv_complete         OUT        NUMBER,
                              rcv_error            OUT        NUMBER,
                              x_return_status      OUT NOCOPY VARCHAR2);

   PROCEDURE clear_interface_table (p_id NUMBER, vstatus OUT VARCHAR2);
END von_pormtopo;
/