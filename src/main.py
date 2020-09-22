import pandas as pd
import os
import numpy as nu
main_path = 'C:\\Users\\e347646\\EDP\\O365_P&C Data Lake - Documents\\General'
df_out_file = 'Output\F00_FC20.csv'
df_out_file_error = 'Output\F00_FC20_error.csv'
df_out_fullpath_error = os.path.join(main_path,df_out_file_error)
df_monthly_fullpath = os.path.join(main_path,'DataSources\\Actuals\\Output\\monthly_pl&bs_2020.csv')
 
df_fx_fullpath = os.path.join(main_path,'MetaDataSources\\FX\\FX.xlsx')
main_path = 'C:\\Users\\e347646\\EDP\\O365_P&C Data Lake - Documents\\General'
df_out_file = 'Output\F00_FC20.csv'
df_out_file_error = 'Output\F00_FC20_error.csv'
df_out_fullpath_error = os.path.join(main_path,df_out_file_error)
df_monthly_fullpath = os.path.join(main_path,'DataSources\\Actuals\\Output\\monthly_pl&bs_2020.csv')
 
df_fx_fullpath = os.path.join(main_path,'MetaDataSources\\FX\\FX.xlsx')
df_company_fullpath_08=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\08\\Scope 8M20.csv')
df_company_fullpath_07=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\07\\Scope 7M20.csv')
df_company_fullpath_06=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\06\\Scope 6M20.csv')
df_company_fullpath_05=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\05\\Scope 5M20.csv')
df_company_fullpath_04=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\04\\Scope 4M20.csv')
df_company_fullpath_03=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\03\\Scope 3M20.csv')
df_company_fullpath_02=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\02\\Scope 2M20.csv')
df_company_fullpath_01=os.path.join(main_path,'MetaDataSources\\\Company\\Source\\2020\\01\\Scope 1M20.csv')

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

df_company= pd.concat([df_company_01, df_company_02, df_company_03, df_company_04, df_company_05, df_company_06, df_company_07, df_company_08], ignore_index=True)

df_fx=df_fx[df_fx['PERIOD']=='2020-08-01']

df_fx=df_fx[df_fx['Scenario']=='Actuals']

df_fx = df_fx.rename({'CURRENCY': 'D_CU'}, axis='columns')

df_merged_RU_fx = pd.merge(df_company,df_fx,  how='left', on='D_CU')

df_merged_RU_fx = df_merged_RU_fx.rename({'Reporting unit (code)': 'D_RU'}, axis='columns')

df_merged_RU_fx=df_merged_RU_fx[['D_RU','D_CU', 'FX_RATE_FINAL','FX_RATE_AVG', 'PERIOD', 'Scenario']]

df_merged_RU_fx_filtro= df_merged_RU_fx[df_merged_RU_fx['D_RU']=="6009"]

df_monthly_filtro=df_monthly[df_monthly['D_RU']=="6009"]

df_monthly['D_RU']= df_monthly['D_RU'].astype(str)
df_merged_RU_fx['D_RU']= df_merged_RU_fx['D_RU'].astype(str)

df_merged=pd.merge(df_monthly, df_merged_RU_fx, how='left', on=['D_RU'])

df_merged_filtro=df_merged[df_merged['D_RU']=="6009"]

df_merged_PL=df_merged[df_merged['D_AC'].str[0]=='R']

df_merged_PL['LC_Amount']=df_merged_PL.apply(lambda row : row['EUR_Amount'] * row['FX_RATE_AVG'], axis=1)

df_merged_A=df_merged[df_merged['D_AC'].str[0]=='A']
df_merged_P=df_merged[df_merged['D_AC'].str[0]=='P']

df_merged_BS= pd.merge(df_merged_A,df_merged_P,  how='outer')

df_merged_BS['LC_Amount']=df_merged_BS.apply(lambda row : row['EUR_Amount'] * row['FX_RATE_FINAL'], axis=1)

df_monthly_lc = pd.merge(df_merged_BS,df_merged_PL,  how='outer')

df_monthly_lc.to_csv(df_out_fullpath)

df_monthly_lc_error = df_monthly_lc[df_monthly_lc['D_CU']=="NaN"]

df_monthly_lc_error.to_csv(df_out_fullpath_error)