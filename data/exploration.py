# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd

# %%
Transactions = pd.read_csv('Transactions.csv')
print(Transactions.shape)

Transactions = Transactions[Transactions['payment_method'] != 'boleto']
Transactions.loc[Transactions["acquirer_name"] == 'pagarme', "product_name"] = 'PSP'
Transactions.loc[Transactions["acquirer_name"] != 'pagarme', "product_name"] = 'gateway'
Transactions[Transactions['transaction_id']=='ff5e9dbc52']
Transactions['mdr_fee'] = 0.0
Transactions['type_transaction'] = ''


# %%
Payables = pd.read_csv('data\\Payables.csv') #PAGAR ME COMO PSP
Payables = Payables[Payables['payment_method'] != 'boleto'] 

cols = ['transaction_id','type','payment_method','fee']
Payables[cols]

print(Payables.shape) #all except boleto = 859 rows

Payables['type'].unique()
Payables[Payables['transaction_id'] == '9009ab2a35']['amount'].sum() #HUUUM ENTENDI
Payables[Payables['transaction_id'] == 'ff5e9dbc52']['amount'].sum() #HUUUM ENTENDI

#pegando valores da taxa mdr
for i in Payables['transaction_id'].unique(): 
    Transactions.loc[Transactions["transaction_id"] == i, "mdr_fee"] = Payables[Payables['transaction_id'] == i]['amount'].sum()

Transactions


# %%
TransactionOperations = pd.read_csv('data\\TransactionOperations.csv')
print(TransactionOperations.shape) #all

refund = (TransactionOperations['type']=='refund') | (TransactionOperations['type']=='capture') 
TransactionOperations = TransactionOperations[refund]
TransactionOperations = TransactionOperations.loc[TransactionOperations['status'] == 'success']
TransactionOperations
print(TransactionOperations.shape) #all
TransactionOperations[TransactionOperations['transaction_id'] == '9009ab2a35'] #HUUUM ENTENDI


#pegando valores da taxa mdr
for i in TransactionOperations['transaction_id'].unique(): 
    Transactions.loc[Transactions["transaction_id"] == i, "type_transaction"] = TransactionOperations[TransactionOperations['transaction_id'] == i]['type']
    #print(TransactionOperations[TransactionOperations['transaction_id'] == i]['type'])


Transactions


# %%



