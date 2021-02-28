from sqlalchemy import (Table, MetaData, Column, String, Integer, ForeignKey, create_engine, insert)
from datetime import datetime
import dotenv
import os
import pymysql
import pandas as pd

class db:

    def __init__(self):
        self.engine = self.open_db()
        #carrega valores do arquivo .env
        dotenv.load_dotenv(dotenv.find_dotenv())


    def import_query(self, path):
        with open(path, "r") as open_file:
            query = open_file.read()
        return query

    def open_db(self):
        host = os.getenv("HOST")
        user = os.getenv("USER")
        pwd = os.getenv("PWD")
        dbname = os.getenv("DBNAME")
        return create_engine( f"mysql+pymysql://{user}:{pwd}@{host}/{dbname}" )

    def execute_query(self, engine, query, **kwargs):
        query = query.format(**kwargs)
        engine.execute(query)
        return True

    def create_tables(self):
        metadata = self.get_metadata(self.engine)

        Table('product', metadata,
                Column('id', Integer, autoincrement=True),
                Column('product_id', String(16), primary_key=True),
                Column('product_name', String(45)))

        Table('financial_operation', metadata,
                Column('id', Integer, autoincrement=True),
                Column('financial_operation_id', String(16), primary_key=True),
                Column('financial_operation_name', String(45)))

        Table('company', metadata,
                Column('id', Integer, autoincrement=True),
                Column('company_id', String(16), primary_key=True),
                Column('company_mcc', String(45)),
                Column('company_type', String(45)))

        Table('installments', metadata,
                Column('id', Integer, autoincrement=True),
                Column('installments_id', String(16), primary_key=True),
                Column('installments_name', String(45)))
                
        Table('transaction', metadata,
                Column('id', Integer, autoincrement=True),
                Column('transaction_id', Integer, primary_key=True),
                Column('acquirer_name', String(45)),
                Column('created_at', String(16)),
                Column('date', String(60)),
                Column('company_id', String(16), ForeignKey("company.company_id")),
                Column('product_id', String(16), ForeignKey("product.product_id")),
                Column('installments_id', String(16), ForeignKey("installments.installments_id")))

        metadata.create_all()
        return metadata

    def insert_product_table(self, product_id, product_name):
        metadata = self.get_metadata(self.engine)
        
        product_table = metadata.tables['product']

        teste = (
            insert(product_table).
            values(product_id=product_id, product_name=product_name)
        )

        self.engine.execute(teste)

    def insert_financial_operation_table(self, metadata):
        metadata = self.get_metadata(self.engine)

        financial_operation_table = metadata.tables['financial_operation']

        teste = (
            insert(financial_operation_table).
            values(financial_operation_id='a', financial_operation_name='aaaaaaa aaaaaaa')
        )

        self.engine.execute(teste)

    def insert_company_table(self, company_id, company_mcc, company_type):
        metadata = self.get_metadata(self.engine)

        company_table = metadata.tables['company']

        teste = (
            insert(company_table).
            values(company_id=company_id, company_type=company_type,company_mcc=company_mcc)
        )

        self.engine.execute(teste)

    def insert_installments_table(self, metadata):
        metadata = self.get_metadata(self.engine)

        installments_table = metadata.tables['installments']

        teste = (
            insert(installments_table).
            values(installments_id='a', company_name='aaaaaaa aaaaaaa')
        )

        self.engine.execute(teste)

    def insert_transaction_table(self, metadata):
        metadata = self.get_metadata(self.engine)

        transaction_table = metadata.tables['transaction']

        teste = (
            insert(transaction_table).
            values(transaction_id='a', company_name='aaaaaaa aaaaaaa')
        )

        self.engine.execute(teste)

    def get_metadata(self, engine):
        metadata = MetaData(bind=engine)
        metadata.reflect(bind=engine)
        return metadata

    def pre_insert_companies(self):
        df = pd.read_csv('data\\Companies.csv')

        for id, mcc, company_type in zip(df['company_id'], df['company_mcc'], df['company_type']):
            self.insert_company_table(id, str(mcc), company_type)
        return True

    def pre_insert_acquirer(self):
        df = pd.read_csv('data\\Transactions.csv')

        acquirers = df['acquirer_name'].unique()
        
        return True

#a = db()

#a.create_tables()

#a.get_metadata(a.engine)