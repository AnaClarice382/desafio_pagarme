def __init__():
    a = db()
    a.create_tables()
    a.load_csv()
    a.pre_insert_companies()

__init__()