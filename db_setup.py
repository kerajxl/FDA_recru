import pandas as pd
import random
from faker import Faker
from datetime import timedelta
import sqlite3
# Inicjalizacja Faker z ustawieniami dla polskich danych
fake = Faker('pl_PL')

def create_energy_usage_df(num_records=100000, customers=20000):
    # Przygotowanie listy identyfikatorów punktów poboru
    supply_point_ids = [f'SP{100000 + i}' for i in range(2 * num_records)]
    
    energy_usage_data = {
        'CustomerID': [],
        'Date': [],
        'Type': [],
        'Usage': [],
        'Price_per_unit': [],
        'POD': []
    }
    
    for _ in range(num_records):
        customer_id = f'CU{1000 + random.randint(1, customers)}'
        pod = random.choice(supply_point_ids)
        start_date = fake.date_between(start_date='-2y', end_date='today')
        days_in_month = pd.Period(start_date, freq='M').days_in_month
        usage_type = random.choice(['Power', 'Gas'])
        # Zapewniamy, że liczba rekordów będzie co najmniej 1
        num_usage_entries = random.randint(1, max(1, days_in_month // 5))
        usage_dates = [start_date + timedelta(days=random.randint(0, days_in_month - 1))
                       for _ in range(num_usage_entries)]
        
        for date in usage_dates:
            energy_usage_data['CustomerID'].append(customer_id)
            energy_usage_data['Date'].append(date)
            energy_usage_data['Type'].append(usage_type)
            energy_usage_data['POD'].append(pod)
            if usage_type == 'Power':
                usage = random.randint(100, 3000)
                price = round(random.uniform(600, 1800), 2)
            else:
                usage = random.randint(20, 800)
                price = round(random.uniform(200, 500), 2)
            energy_usage_data['Usage'].append(usage)
            energy_usage_data['Price_per_unit'].append(price)
    
    df = pd.DataFrame(energy_usage_data)
    df['Date'] = pd.to_datetime(df['Date'])
    return df

def create_invoices_df(energy_usage_df):
    monthly_usage = energy_usage_df.groupby(['CustomerID', 'POD', pd.Grouper(key='Date', freq='M')]).agg({
        'Usage': 'sum',
        'Price_per_unit': 'mean'
    }).reset_index()
    
    invoices_data = {
        'InvoiceID': [f'INV{100000 + i}' for i in range(monthly_usage.shape[0])],
        'CustomerID': monthly_usage['CustomerID'].tolist(),
        'POD': [],
        'Usage': [],
        'Amount': [round(u * p, 2) for u, p in zip(monthly_usage['Usage'], monthly_usage['Price_per_unit'])],
        'PaymentStatus': [random.choice(['Paid', 'Pending', 'Overdue']) for _ in range(monthly_usage.shape[0])],
        'StartDate': monthly_usage['Date'].dt.to_period('M').dt.start_time,
        'EndDate': monthly_usage['Date'].dt.to_period('M').dt.end_time,
        'PostingDate': [],
        'DueDate': [],
        'PaymentDate': [],
        'Description': []
    }
    
    for i in range(monthly_usage.shape[0]):
        start_date = invoices_data['StartDate'][i]
        end_date = invoices_data['EndDate'][i]
        posting_date = end_date + timedelta(days=random.randint(7, 21))
        due_date = posting_date + timedelta(days=7)
        payment_status = invoices_data['PaymentStatus'][i]
        
        if payment_status == 'Paid':
            payment_date = due_date - timedelta(days=random.randint(1, 5))
        elif payment_status == 'Pending':
            payment_date = None
        elif payment_status == 'Overdue':
            payment_date = due_date + timedelta(days=random.randint(1, 10))
        
        usage = monthly_usage.at[i, 'Usage']
        price = monthly_usage.at[i, 'Price_per_unit']
        supply_point_id = monthly_usage.at[i, 'POD']
        description = (
            f"Supply from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}, "
            f"SPID: {supply_point_id}, Usage: {usage} kWh/m³, Avg. Price: {price:.2f}/unit"
        )
        invoices_data['POD'].append(supply_point_id)
        invoices_data['Usage'].append(usage)
        invoices_data['PostingDate'].append(posting_date)
        invoices_data['DueDate'].append(due_date)
        invoices_data['PaymentDate'].append(payment_date)
        invoices_data['Description'].append(description)
        
    df = pd.DataFrame(invoices_data)
    return df

def create_customers_df(customers=20000):
    customers_data = {
        'CustomerID': [f'CU{1000 + i}' for i in range(customers)],
        'Name': [fake.company() for _ in range(customers)],
        'Address': [fake.address() for _ in range(customers)],
        'Phone': [fake.phone_number() for _ in range(customers)],
        'Email': [fake.email() for _ in range(customers)]
    }
    df = pd.DataFrame(customers_data)
    return df

def setup_database():
    """Generuje wszystkie DataFrame'y (faktury, zużycie energii, klienci)."""
    energy_usage_df = create_energy_usage_df()
    invoices_df = create_invoices_df(energy_usage_df)
    customers_df = create_customers_df()
    return invoices_df, energy_usage_df, customers_df

def modify_random_usage(energy_usage_df):
    """
    Modyfikuje losowy rekord w tabeli zużycia energii.
    Zwraca: indeks rekordu, identyfikator klienta, starą wartość, nową wartość oraz zmodyfikowany rekord.
    """
    random_customer_id = random.choice(energy_usage_df['CustomerID'].unique())
    customer_usage_df = energy_usage_df[energy_usage_df['CustomerID'] == random_customer_id]
    random_index = random.choice(customer_usage_df.index)
    old_usage = energy_usage_df.loc[random_index, 'Usage']
    new_usage = random.randint(100, 3000)
    energy_usage_df.loc[random_index, 'Usage'] = new_usage
    return random_index, random_customer_id, old_usage, new_usage, energy_usage_df.loc[random_index]


# query = """ 
# with calculations as (
# select 
# i.InvoiceID, i.POD, i.CustomerID,
# i.Usage,  date(StartDate) StartDate,  date(EndDate) EndDate, sum(u.Usage) as usage_rel 
# from invoices i 
# left join usage u on u.POD = i.POD and date(u.Date) between date(StartDate)  and  date(EndDate) and i.CustomerID = u.CustomerID
# group by i.InvoiceID, i.POD, i.CustomerID,
# i.Usage,  date(StartDate) ,  date(EndDate) ) 

# select c.*, cs.*
#  from 
#  calculations c
#  left join customers cs on cs.CustomerID = c.CustomerID 
#  where usage_rel != Usage

# """
# sqlquery(query)