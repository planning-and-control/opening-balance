
import os
import numpy as np
import datetime
import pandas as pd

def generate_OB():
    #variables
    main_path = 'C:\\OneDrive\\EDP\\O365_P&C Data Lake - General' #'C:\\Users\\e347646\\EDP\\O365_P&C Data Lake - Documents\\General'
    main_out_path = 'C:\\OneDrive\\EDP\\O365_Adaptive - General\\F00\\BU21' #'C:\\Users\\e347646\\EDP\\O365_Adaptive - General\\F00\\BU21'
    df_out_file = 'F00_FC20.csv'
    df_out_file_BS = 'F00_FC20_BS.csv'
    df_out_file_PL = 'F00_FC20_PL.csv'
    df_out_file_error = 'F00_FC20_error.csv'
    df_out_fullpath_error = os.path.join(main_out_path,df_out_file_error)
    df_out_fullpath = os.path.join(main_out_path,df_out_file)
    df_out_fullpath_BS = os.path.join(main_out_path,df_out_file_BS)
    df_out_fullpath_PL = os.path.join(main_out_path,df_out_file_PL)
    OB_date = datetime.datetime(2020, 8, 1)

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

    df_monthly = pd.read_csv(df_monthly_fullpath, parse_dates=["D_PE"])
    df_monthly = df_monthly[df_monthly["D_PE"]<=OB_date]

    df_fx = pd.read_excel(df_fx_fullpath, parse_dates=["PERIOD"])
    df_fx=df_fx[df_fx['PERIOD']==datetime.datetime(2020, 8, 1)]
    df_fx=df_fx[df_fx['Scenario']=='Actuals']

    df_company_01= pd.read_csv(df_company_fullpath_01,  encoding="utf-16")
    df_company_02= pd.read_csv(df_company_fullpath_02,  encoding="utf-16")
    df_company_03= pd.read_csv(df_company_fullpath_03,  encoding="utf-16")
    df_company_04= pd.read_csv(df_company_fullpath_04,  encoding="utf-16")
    df_company_05= pd.read_csv(df_company_fullpath_05,  encoding="utf-16")
    df_company_06= pd.read_csv(df_company_fullpath_06,  encoding="utf-16")
    df_company_07= pd.read_csv(df_company_fullpath_07,  encoding="utf-16")
    df_company_08= pd.read_csv(df_company_fullpath_08,  encoding="utf-16")

    df_company_01['D_PE'] = datetime.datetime(2020, 1, 1)

    df_company_02['D_PE'] = datetime.datetime(2020, 2, 1)

    df_company_03['D_PE'] = datetime.datetime(2020, 3, 1)

    df_company_04['D_PE'] = datetime.datetime(2020, 4, 1)

    df_company_05['D_PE'] = datetime.datetime(2020, 5, 1)

    df_company_06['D_PE'] = datetime.datetime(2020, 6, 1)

    df_company_07['D_PE'] = datetime.datetime(2020, 7, 1)

    df_company_08['D_PE'] = datetime.datetime(2020, 8, 1)

    df_company= pd.concat([df_company_01, df_company_02, df_company_03, df_company_04, df_company_05, df_company_06, df_company_07, df_company_08], ignore_index=True)

    df_company = df_company.rename({'Reporting unit (code)': 'D_RU'}, axis='columns')

    df_fx = df_fx.rename({'PERIOD': 'D_PE'}, axis='columns')
    df_fx = df_fx.rename({'CURRENCY': 'D_CU'}, axis='columns')

    df_fx.drop(['D_PE'], axis=1, inplace= True)

    df_merged_RU_fx = pd.merge(df_company,df_fx,  how='left', on='D_CU')

    df_merged_RU_fx=df_merged_RU_fx[['D_RU','D_CU', 'FX_RATE_FINAL','FX_RATE_AVG', 'D_PE', 'Scenario']]

    df_monthly['D_RU']= df_monthly['D_RU'].astype(str)
    df_merged_RU_fx['D_RU']= df_merged_RU_fx['D_RU'].astype(str)

    df_monthly=df_monthly[df_monthly['D_RU']!='6009']

    df_monthly['D_RU']=df_monthly['D_RU'].str.replace("EM","",regex=False)
    df_monthly['D_RU']=df_monthly['D_RU'].str.replace("MEP","",regex=False)
    df_monthly['D_RU']=df_monthly['D_RU'].str.replace("OLD","",regex=False)

    df_merged_RU_fx=df_merged_RU_fx[df_merged_RU_fx['D_RU']!='6009']

    df_merged=pd.merge(df_monthly, df_merged_RU_fx, how='left', on=['D_RU', 'D_PE'])

    df_merged['AccountName'] = df_merged['D_AC']
    df_merged['AccountCode'] = df_merged['D_AC']
    df_merged.rename({'D_AC': 'codeAcc', 'T1' : 'Partner', 'D_FL' : 'FlowAccount', 'D_PE': 'dataPeriod'}, axis=1, inplace=True)
    df_merged['CostCentre'] = "uncategorized"
    df_merged['intercoAccount'] = "C"
    df_merged['Partner'] = df_merged['Partner'].apply(lambda partner : partner if partner != "#" and  partner != "S9999" else "Partner_CH" )
    df_merged.drop(['D_LE', 'D_NU', 'D_SP', 'D_CO', 'D_SC', 'Scenario'], axis=1, inplace=True)

    df_merged_PL=df_merged[df_merged['codeAcc'].str.startswith('R')]

    df_merged_PL['LC_Amount']=df_merged_PL['EUR_Amount'] * df_merged_PL['FX_RATE_AVG']

    df_BS=df_merged[~df_merged['codeAcc'].str.startswith('R')]

    df_BS['LC_Amount']=df_BS['EUR_Amount'] * df_BS['FX_RATE_FINAL']

    df_monthly_lc= pd.concat([df_BS, df_merged_PL], ignore_index=True)

    df_monthly_lc_error = df_monthly_lc[df_monthly_lc['D_CU']=="NaN"]

    df_monthly_lc_error.to_csv(df_out_fullpath_error)

    df_monthly_lc.drop(['EUR_Amount', 'D_CU', 'FX_RATE_FINAL', 'FX_RATE_AVG'], axis=1, inplace=True)

    df_monthly_lc.to_csv(df_out_fullpath, index=False)

    df_merged_PL = df_merged_PL[df_merged_PL['LC_Amount'] != 0]

    df_merged_PL.drop(['EUR_Amount', 'D_CU', 'FX_RATE_FINAL', 'FX_RATE_AVG'], axis=1, inplace=True)

    df_merged_PL.to_csv(df_out_fullpath_PL, index=False)

    df_BS.drop(['EUR_Amount', 'D_CU', 'FX_RATE_FINAL', 'FX_RATE_AVG'], axis=1, inplace=True)

    df_BS.loc[:,"dataPeriod"] = datetime.datetime(2020, 1, 1)
    df_BS.loc[:,"FlowAccount"] = "F00"

    #GroupbyColumns
    columns_df_BS = list(df_BS.columns)
    grouping_elements = ["LC_Amount"]
    for element in grouping_elements:
        if element in list(df_BS.columns):
            columns_df_BS.remove(element)

    #Groupby
    df_BS = df_BS.groupby(columns_df_BS, as_index=False, dropna=False, observed=True).sum()

    #Remove Zeros
    df_BS = df_BS[df_BS.LC_Amount != 0].reset_index(drop=True)
    df_BS = df_BS[df_BS.LC_Amount.abs() > 0.01].reset_index(drop=True)

    #2021
    df_BS_y1 = df_BS.copy(deep=True) 
    #df_BS.dataPeriod = df_BS.dataPeriod + pd.offsets.DateOffset(years=1)
    df_BS_y1.dataPeriod = df_BS_y1.dataPeriod + pd.offsets.YearBegin(1)
    #2022
    df_BS_y2 = df_BS_y1.copy(deep=True)
    df_BS_y2.dataPeriod = df_BS_y2.dataPeriod + pd.offsets.DateOffset(years=1)
    #2023
    df_BS_y3 = df_BS_y2.copy(deep=True)
    df_BS_y3.dataPeriod = df_BS_y3.dataPeriod + pd.offsets.DateOffset(years=1)
    #2024
    df_BS_y4 = df_BS_y3.copy(deep=True)
    df_BS_y4.dataPeriod = df_BS_y4.dataPeriod + pd.offsets.DateOffset(years=1)
    #2025
    df_BS_y5 = df_BS_y4.copy(deep=True)
    df_BS_y5.dataPeriod = df_BS_y5.dataPeriod + pd.offsets.DateOffset(years=1)

    df_BS_final = pd.concat([df_BS, df_BS_y1, df_BS_y2, df_BS_y3, df_BS_y4, df_BS_y5])

    df_BS_final.to_csv(df_out_fullpath_BS, index=False)

if __name__ == "__main__":
    generate_OB()