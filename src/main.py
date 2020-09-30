import pandas as pd
import os
import numpy as np

main_path = 'C:\\OneDrive\\EDP\\O365_P&C Data Lake - General' #'C:\\Users\\e347646\\EDP\\O365_P&C Data Lake - Documents\\General'
main_out_path = 'C:\\OneDrive\\EDP\\O365_Adaptive - General\\F00\\BU21'
df_out_file = 'F00_FC20.csv'
df_out_file_BS = 'F00_FC20_BS.csv'
df_out_file_PL = 'F00_FC20_PL.csv'
df_out_file_error = 'F00_FC20_error.csv'
df_out_fullpath_error = os.path.join(main_out_path,df_out_file_error)
df_out_fullpath = os.path.join(main_out_path,df_out_file)
df_out_fullpath_BS = os.path.join(main_out_path,df_out_file_BS)
df_out_fullpath_PL = os.path.join(main_out_path,df_out_file_PL)

df_monthly_fullpath = os.path.join(main_path,'DataSources\\Actuals\\Output\\monthly_pl&bs_2020.csv')
 
df_fx_fullpath = os.path.join(main_path,'MetaDataSources\\FX\\FX.xlsx')

df_company_fullpath_08=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\08\\Scope 8M20.csv')
df_company_fullpath_07=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\07\\Scope 7M20.csv')
df_company_fullpath_06=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\06\\Scope 6M20.csv')
df_company_fullpath_05=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\05\\Scope 5M20.csv')
df_company_fullpath_04=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\04\\Scope 4M20.csv')
df_company_fullpath_03=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\03\\Scope 3M20.csv')
df_company_fullpath_02=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\02\\Scope 2M20.csv')
df_company_fullpath_01=os.path.join(main_path,'MetaDataSources\\Company\\Source\\2020\\01\\Scope 1M20.csv')

df_monthly = pd.read_csv(df_monthly_fullpath)

df_fx = pd.read_excel(df_fx_fullpath)

df_company_01= pd.read_csv(df_company_fullpath_01,  encoding="utf-16")
df_company_02= pd.read_csv(df_company_fullpath_02,  encoding="utf-16")
df_company_03= pd.read_csv(df_company_fullpath_03,  encoding="utf-16")
df_company_04= pd.read_csv(df_company_fullpath_04,  encoding="utf-16")
df_company_05= pd.read_csv(df_company_fullpath_05,  encoding="utf-16")
df_company_06= pd.read_csv(df_company_fullpath_06,  encoding="utf-16")
df_company_07= pd.read_csv(df_company_fullpath_07,  encoding="utf-16")
df_company_08= pd.read_csv(df_company_fullpath_08,  encoding="utf-16")

df_company_01['PERIOD'] = "2020-01-01"

df_company_02['PERIOD'] = "2020-02-01"

df_company_03['PERIOD'] = "2020-03-01"

df_company_04['PERIOD'] = "2020-04-01"

df_company_05['PERIOD'] = "2020-05-01"

df_company_06['PERIOD'] = "2020-06-01"

df_company_07['PERIOD'] = "2020-07-01"

df_company_08['PERIOD'] = "2020-08-01"

df_company= pd.concat([df_company_01, df_company_02, df_company_03, df_company_04, df_company_05, df_company_06, df_company_07, df_company_08], ignore_index=True)

df_company = df_company.rename({'PERIOD': 'D_PE'}, axis='columns')
df_company = df_company.rename({'Reporting unit (code)': 'D_RU'}, axis='columns')

df_fx = df_fx.rename({'PERIOD': 'D_PE'}, axis='columns')
df_fx = df_fx.rename({'CURRENCY': 'D_CU'}, axis='columns')

df_fx=df_fx[df_fx['D_PE']=='2020-08-01']

df_fx=df_fx[df_fx['Scenario']=='Actuals']

df_fx.drop(['D_PE'], axis=1, inplace= True)

df_merged_RU_fx = pd.merge(df_company,df_fx,  how='left', on='D_CU')

df_merged_RU_fx=df_merged_RU_fx[['D_RU','D_CU', 'FX_RATE_FINAL','FX_RATE_AVG', 'D_PE', 'Scenario']]

df_monthly['D_RU']= df_monthly['D_RU'].astype(str)
df_merged_RU_fx['D_RU']= df_merged_RU_fx['D_RU'].astype(str)

df_monthly=df_monthly[df_monthly['D_RU']!='6009']

df_merged_RU_fx=df_merged_RU_fx[df_merged_RU_fx['D_RU']!='6009']

df_merged=pd.merge(df_monthly, df_merged_RU_fx, how='left', on=['D_RU', 'D_PE'])

df_merged_PL=df_merged[df_merged['D_AC'].str.startswith('R')]

df_merged_PL['LC_Amount']=df_merged_PL['EUR_Amount'] * df_merged_PL['FX_RATE_AVG']

df_BS=df_merged[~df_merged['D_AC'].str.startswith('R')]

df_BS['LC_Amount']=df_BS['EUR_Amount'] * df_BS['FX_RATE_FINAL']

df_monthly_lc= pd.concat([df_BS, df_merged_PL], ignore_index=True)

df_monthly_lc.to_csv(df_out_fullpath, index=False)

df_monthly_lc_error = df_monthly_lc[df_monthly_lc['D_CU']=="NaN"]

df_monthly_lc_error.to_csv(df_out_fullpath_error)

df_merged_PL = df_merged_PL[df_merged_PL['LC_Amount'] != 0]

df_merged_PL['AccountName'] = df_merged_PL['D_AC']
df_merged_PL['AccountCode'] = df_merged_PL['D_AC']
df_merged_PL.rename({'D_AC': 'codeAcc', 'T1' : 'Partner', 'D_FL' : 'FlowAccount', 'D_PE': 'dataPeriod'}, axis=1, inplace=True)
df_merged_PL['CostCentre'] = "uncategorized"
df_merged_PL['intercoAccount'] = "C"
df_merged_PL['Partner'] = df_merged_PL['Partner'].apply(lambda partner : partner if partner != "#" and  partner != "S9999" else "Partner_CH" )

df_merged_PL.drop(['EUR_Amount', 'D_LE', 'D_NU', 'D_SP', 'D_CO', 'D_SC', 'D_CU', 'FX_RATE_FINAL', 'FX_RATE_AVG', 'Scenario'], axis=1, inplace=True)

df_merged_PL.to_csv(df_out_fullpath_PL, index=False)

df_BS.to_csv(df_out_fullpath_BS, index=False)