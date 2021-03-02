from sqlalchemy import (Table, MetaData, Column, String, Integer,
                        ForeignKey, create_engine, insert, Date,TIMESTAMP,
                        Float)
import dotenv
import os
import pymysql
import pandas as pd

class db:

    def load_csv(self):
        self.companies = pd.read_csv('data\\Companies.csv')
        self.payables = pd.read_csv('data\\Payables.csv')
        self.transactions_op = pd.read_csv('data\\TransactionOperations.csv')
        self.transactions = pd.read_csv('data\\TransactionOperations.csv')
    
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

        Table('company', metadata,
                Column('id', Integer, autoincrement=True),
                Column('company_id', String(16), primary_key=True),
                Column('company_mcc', String(45)),
                Column('company_type', String(45)))

        Table('product', metadata,
                Column('id', Integer, autoincrement=True),
                Column('product_id', String(16), primary_key=True),
                Column('product_name', String(45)))

        Table('installments', metadata,
                Column('id', Integer, autoincrement=True),
                Column('installments_id', String(16), primary_key=True),
                Column('installments_name', String(45)))
                
        Table('transaction', metadata,
                Column('id', Integer, autoincrement=True),
                Column('transaction_id', Integer, primary_key=True),
                Column('financial_operation_type', String(16), primary_key=True),
                Column('payment_method', String(16)),
                Column('acquirer_name', String(45)),
                Column('mdr_fee', Float),
                Column('gateway_fee', Float),
                Column('amout', Float),
                Column('date', Date),
                Column('created_at', TIMESTAMP),
                Column('month_name', String(16)),
                Column('day_of_week', Integer),
                Column('company_id', String(16), ForeignKey("company.company_id")),
                Column('product_id', String(16), ForeignKey("product.product_id")),
                Column('installments_id', String(16), ForeignKey("installments.installments_id")))

        metadata.create_all(checkfirst=True)
        return metadata

    def insert_product_table(self, product_id, product_name):
        metadata = self.get_metadata(self.engine)
        
        product_table = metadata.tables['product']

        teste = (
            insert(product_table).
            values(product_id=product_id, product_name=product_name)
        )

        self.engine.execute(teste)

    def insert_financial_operation_table(self, financial_op_name):
        metadata = self.get_metadata(self.engine)

        financial_operation_table = metadata.tables['financial_operation']

        teste = (
            insert(financial_operation_table).
            values(financial_operation_name=financial_op_name)
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
        for id, mcc, company_type in zip(self.companies['company_id'], self.companies['company_mcc'], self.companies['company_type']):
            self.insert_company_table(id, str(mcc), company_type)
        return True

    def pre_insert_transaction(self):
        month_name = {'Janeiro':1,'Fevereiro':2,'Março':3,'Abril':4,
                    'Maio':5,'Junho':6,'Julho':7,'Agosto':8,'Setembro':9, 
                    'Outubro':10, 'Novembro':11,'Dezembro':12}

        day_of_week = {'Segunda':1,'Terça':2,'Quarta':3,'Quinta':4,
            'Sexta':5,'Sábado':6,'Domingo':7}


        ###ORGANIZANDO DADOS DA "TRANSACOES"
        self.transactions = self.transactions[self.transactions['payment_method'] != 'boleto']
        self.transactions.loc[self.transactions["acquirer_name"] == 'pagarme', "product_name"] = 'PSP'
        self.transactions.loc[self.transactions["acquirer_name"] != 'pagarme', "product_name"] = 'gateway'

        self.transactions['mdr_fee'] = 0.0
        self.transactions['type_transaction'] = ''   
        self.transactions['installments_range'] = ''   

        ###FIM -- ORGANIZANDO DADOS DA "TRANSACOES"

        ###PAGARME COMO PSP
        self.payables = self.payables[self.payables['payment_method'] != 'boleto'] #Só credito e debito

        #pegando valores da taxa mdr
        for i in self.payables['transaction_id'].unique(): 
            self.transactions.loc[self.transactions["transaction_id"] == i, "mdr_fee"] = self.payables[self.payables['transaction_id'] == i]['amount'].sum()

        ###FIM -- PAGARME COMO PSP

        bool_refund_capture = (self.transactions_op['type'] == 'refund') | (self.transactions_op['type'] == 'capture')
        self.transactions_op = self.transactions_op.loc[bool_refund_capture]
        print(self.transactions.shape)
        return True
