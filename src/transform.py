import pandas as pd
import numpy as np
import logging
from .validators import DataValidator

logger = logging.getLogger(__name__)

class DataTransformer:
    
    @staticmethod
    def transform_all(dataframes):
        logger.info("Iniciando transformação dos dados...")
        
        transformed = {}
        
        for name, df in dataframes.items():
            
            DataValidator.validate_not_empty(df, f"{name} (raw)")

            if name == 'CreditCardApproval':
                transformed[name] = DataTransformer._transform_credit_card_approval(df)
            elif name == 'GitHubLoanPredictionDataset':
                transformed[name] = DataTransformer._transform_github_loan(df)
            elif name == 'GermanCredit':
                transformed[name] = DataTransformer._transform_german_credit(df)
            elif name == 'UCICreditCard':
                transformed[name] = DataTransformer._transform_uci_credit(df)
            else:
                transformed[name] = df
        
        return transformed
    
    @staticmethod
    def _transform_credit_card_approval(df):
        logger.info("Transformando CreditCardApproval...")
        
        df = df.drop(columns=["ID"])
        df['Job title'] = df['Job title'].fillna(df['Job title'].mode()[0])
        df = df.dropna().drop_duplicates()
        
        df = df.rename(columns={
            'Has a car': 'HasACar',
            'Has a property': 'HasAProperty',
            'Children count': 'ChildrenCount',
            'Employment status': 'EmploymentStatus',
            'Education level': 'EducationLevel',
            'Marital status': 'MaritalStatus',
            'Employment length': 'EmploymentLength',
            'Has a mobile phone': 'HasAMobilePhone',
            'Has a work phone': 'HasAWorkPhone',
            'Has a phone': 'HasAPhone',
            'Has an email': 'HasAEmail',
            'Job title': 'JobTitle',
            'Family member count': 'FamilyMemberCount',
            'Account age': 'AccountAge',
            'Is high risk': 'IsHighRisk'
        })
        
        df['Age'] = (df['Age'].abs() / 365).astype(int)
        df['EmploymentLength'] = (df['EmploymentLength'].abs() / 365).round(1)
        
        return df
    
    @staticmethod
    def _transform_github_loan(df):
        logger.info("Transformando GitHubLoanPredictionDataset...")
        
        df = df.drop(columns=["Loan_ID"])
        
        df['Dependents'] = pd.to_numeric(df['Dependents'], errors='coerce')
        df['Married'] = df['Married'].fillna('No').astype(bool)
        df['Self_Employed'] = df['Self_Employed'].fillna('No').astype(bool)
        
        media_dependents = np.floor(df['Dependents'].mean())
        df['Dependents'] = df['Dependents'].fillna(media_dependents)
        df['LoanAmount'] = df['LoanAmount'].fillna(df['LoanAmount'].mean())
        df['Loan_Amount_Term'] = df['Loan_Amount_Term'].fillna(df['Loan_Amount_Term'].mean())
        
        media_credit_history = np.floor(df['Credit_History'].mean())
        df['Credit_History'] = df['Credit_History'].fillna(media_credit_history)
        df = df.dropna().drop_duplicates()
        
        df.columns = df.columns.str.replace('_', '')
        
        df['TotalIncome'] = df['ApplicantIncome'] + df['CoapplicantIncome']
        df.insert(7, 'TotalIncome', df.pop('TotalIncome'))
        
        return df
    
    @staticmethod
    def _transform_german_credit(df):
        logger.info("Transformando GermanCredit...")
        
        df = df.drop(columns=["ContaCorrente", "Telefone"])
        df.columns = df.columns.str.replace('_', '')
        
        return df
    
    @staticmethod
    def _transform_uci_credit(df):
        logger.info("Transformando UCICreditCard...")
        
        df = df.drop(columns=["ID"])
        df = df.rename(columns={'default.payment.next.month': 'DefaultPaymentNextMonth'})
        
        return df
