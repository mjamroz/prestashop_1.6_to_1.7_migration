import MySQLdb as ms

host = ""
user = ""
pwd = ""

db_old = ""
db_new = ""


db_old = ms.connect(host, user, pwd, db_old, charset='utf8')
db_new = ms.connect(host, user, pwd, db_new, charset='utf8')

def query_db(q, args, db, commit=False, one=False, dict=True):
    if db == "old":
        curr_db = db_old
    elif db == "new":
        curr_db = db_new
    else:
        print("choose db!")
        return False

    if dict:
        cursor = curr_db.cursor(ms.cursors.DictCursor)
    else:
        cursor = curr_db.cursor()
    cursor.execute(q, args)

    if commit:
        curr_db.commit()
        return cursor.lastrowid
    else:
        rv = cursor.fetchall()
        return ((rv[0] if rv else None) if one else rv)

def close():
    db_old.close()
    db_new.close()

def copy1t1(old_table_name, new_table_name, columns=[], clear_new_table=False):
    print("copy %s to %s" %( old_table_name, new_table_name))
    if len(columns) == 0:
        columns_new = []
        for col in query_db("show columns from "+new_table_name, [], "new"):
            columns_new.append(col['Field'])
        for col in query_db("show columns from "+old_table_name, [], "old"):
            if col['Field'] in columns_new:
                columns.append(col['Field'])
    # TODO prefix hardcoded
    if old_table_name == "psNew2_product" or old_table_name == "psNew2_product_shop":
        columns.remove("condition")

    if clear_new_table:
        query_db("DELETE FROM " + new_table_name, [], "new", commit=True)

    old_query = "SELECT " + ",".join(columns) + " FROM " + old_table_name
    s = "%s,"*len(columns)
    new_query = "INSERT INTO "+new_table_name + "(" + ",".join(columns) + ") VALUES("+s[:-1]+")"

    for row in query_db(old_query, [], "old", dict=False):
        query_db(new_query, row, "new", commit=True)


def copy1t1orders(old_table_name, new_table_name, columns=[], clear_new_table=False):
    if len(columns) == 0:
        columns_new = []
        for col in query_db("show columns from "+new_table_name, [], "new"):
            columns_new.append(col['Field'])
        for col in query_db("show columns from "+old_table_name, [], "old"):
            if col['Field'] in columns_new:
                columns.append(col['Field'])
    for e in ["delivery_date", "invoice_date"]:
        columns.remove(e)
        columns.append(e)

    if clear_new_table:
        query_db("DELETE FROM " + new_table_name, [], "new", commit=True)

    old_query = "SELECT " + ",".join(columns) + " FROM " + old_table_name
    s = "%s,"*(len(columns))
    new_query = "INSERT INTO "+new_table_name + "(" + ",".join(columns) + ") VALUES("+s[:-1]+")"

    for row in query_db(old_query, [], "old", dict=False):
        ll = list(row)
        ll[-1] = "now()"
        ll[-2] = "now()"
        query_db(new_query, ll, "new", commit=True)
