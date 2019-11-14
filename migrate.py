from db import query_db,close, copy1t1, copy1t1orders
# TODO change hardcode tables prefixes
def a(r):
    # loyalty points to voucher
    if r['c']>0 and query_db("SELECT count(*) c FROM ps_customer WHERE id_customer=%s AND email=%s", [r['id_customer'], r['email']], "new", one=True)['c'] == 1 and query_db("SELECT count(*) e FROM psNew2_customer_group WHERE id_customer=%s AND id_group=4", [r['id_customer']], "old", one=True)['e'] == 0:
        code = "FID" + str(r['id_customer'])
        query_db("INSERT INTO ps_cart_rule(reduction_tax,partial_use,highlight,minimum_amount_currency, group_restriction, cart_rule_restriction, id_customer, date_from, date_to, description, quantity, quantity_per_user, code, reduction_amount, reduction_currency, active, date_add, date_upd) values(1,1,1,0,0,0,%s, now(), now() + interval 1 year, 'Rabat za punkty lojalnościowe', 1, 1, %s, %s, 1, 1, now(), now())", [r['id_customer'], code, r['c']], "new", commit=True)

        id = query_db("SELECT LAST_INSERT_ID() i", [], "new", one=True)

        query_db("INSERT INTO ps_cart_rule_lang(id_cart_rule,id_lang,name) values(%s,1,'Rabat dla stałych klientów (z punktów lojalnościowych)')", [id['i']], "new", commit=True)

def clients_import():
    for t in ['cart', 'cart_cart_rule', 'cart_product', 'cart_rule', 'cart_rule_carrier', 'cart_rule_combination', 'cart_rule_country', 'cart_rule_group', 'cart_rule_lang', 'cart_rule_product_rule', 'cart_rule_product_rule_group', 'cart_rule_product_rule_value', 'cart_rule_shop', 'order_carrier', 'order_cart_rule', 'order_detail', 'order_detail_tax', 'order_history', 'order_invoice', 'order_invoice_payment', 'order_invoice_tax', 'order_message', 'order_message_lang', 'order_payment', 'order_return', 'order_return_detail', 'order_return_state', 'order_return_state_lang', 'order_slip', 'order_slip_detail', 'order_slip_detail_tax', 'order_state', 'order_state_lang', 'customer', 'customer_group', 'address', 'address_format', 'group', 'group_lang', 'group_reduction', 'group_shop']:
        copy1t1("psNew2_"+t, "ps_"+t, [], clear_new_table=True)

    for r in query_db("SELECT phone,phone_mobile,id_address FROM ps_address", [], "new"):
        p, pm, ida = r['phone'], r['phone_mobile'], r['id_address']
        if p == '' and pm != '':
            query_db("UPDATE ps_address SET phone=%s WHERE id_address=%s", [pm, ida], "new", commit=True)
        elif pm == '' and p != '':
            query_db("UPDATE ps_address SET phone_mobile=%s WHERE id_address=%s", [p, ida], "new", commit=True)

    query_db("DELETE FROM ps_cart_rule WHERE description like 'Rabat za punkty%%'", [], "new", commit=True)
    for r in  query_db("SELECT 0.2*sum(l.points) c,l.id_customer,email FROM psNew2_loyalty l JOIN psNew2_customer ON psNew2_customer.id_customer=l.id_customer where l.id_loyalty_state NOT in (4,1) GROUP BY l.id_customer ORDER BY id_customer", [], "old", commit=False):
        a(r)
    for cr in query_db("SELECT id_cart_rule FROM ps_cart_rule", [], "new"):
        if query_db("select count(*) c FROM  ps_cart_rule_group WHERE id_cart_rule=%s AND id_group=3", [cr['id_cart_rule']], "new", one=True)['c'] == 0:
            query_db("INSERT INTO ps_cart_rule_group(id_cart_rule,id_group) values(%s,3)", [cr['id_cart_rule']], "new", commit=True)

    copy1t1orders("psNew2_orders", "ps_orders", clear_new_table=True)

def catalog_import():
    query_db("DELETE FROM ps_pack", [], "new", commit=True)

    for t in ['pack', 'image', 'image_lang', 'image_shop', 'image_type', 'manufacturer', 'manufacturer_lang', 'manufacturer_shop', 'attribute', 'attribute_group', 'attribute_group_lang', 'attribute_group_shop', 'attribute_impact', 'attribute_lang', 'attribute_shop', 'feature', 'feature_shop', 'feature_lang', 'feature_value', 'feature_value_lang', 'product', 'product_attachment', 'product_attribute', 'product_attribute_combination', 'product_attribute_image', 'product_attribute_shop', 'product_carrier', 'product_lang', 'product_sale', 'product_shop', 'product_supplier', 'product_tag', 'product_country_tax', 'supplier', 'supplier_lang', 'supplier_shop', 'supply_order', 'supply_order_detail', 'category', 'category_group', 'category_lang', 'category_product', 'category_shop', 'stock_available', "stock_mvt_reason", "stock_mvt_reason_lang", 'product', 'product_sale', 'product_comment', 'product_comment_criterion', 'product_comment_criterion_category', 'product_comment_criterion_lang', 'product_comment_criterion_product', 'product_comment_grade', 'product_comment_report', 'product_comment_usefulness']:
        copy1t1("psNew2_"+t, "ps_"+t, [], clear_new_table=True)

#clients_import()
#catalog_import()
# copy /img/{c,p}
