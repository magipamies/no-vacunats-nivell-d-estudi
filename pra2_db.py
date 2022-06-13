# -*- coding: utf-8 -*-
"""
Created on Mon Jun  6 11:38:33 2022

@author: magi
"""

import pandas as pd
import geopandas as gpd

from os import path as pth

import numpy as np

work_f = r'C:\Users\mpamies\Dropbox\Server\Documents\Estudis\Geografia\Master\Base_Dades\VisuaDades\Practiques\PRA2\dades'

map_muni_f = pth.join(work_f, r'LimitsadministratiusmunicipalsdeCatalunya.geojson')

f_vac = pth.join(work_f, r'Vacunaci__per_al_COVID-19__dosis_administrades_per_municipi.csv')
f_pob = pth.join(work_f, r'Poblaci__de_Catalunya_per_municipi__rang_d_edat_i_sexe.csv')
f_pob21 = pth.join(work_f, r'Poblacio_t1181mun202100.csv')

vacunes_total_out = pth.join(work_f, r'vacunes_total_out.csv')
vacunes_total_out_sum = pth.join(work_f, r'vacunes_total_out_sum.csv')

vacunes_total_out_s = pth.join(work_f, r'vacunes_total_out_s.csv')
vacunes_total_out_s_sum = pth.join(work_f, r'vacunes_total_out_s_sum.csv')

vacunes_pob_p_out = pth.join(work_f, 'vacunes_poblacio_per_out.csv')
vacunes_pob_p_m_out = pth.join(work_f, 'vacunes_poblacio_per_m_out.csv')

f_pob21_out = pth.join(work_f, 'f_pob21_out.csv')

f_est_15 = pth.join(work_f, 'Poblacio_15anys_nivellestudis.csv')
f_est_2564_p = pth.join(work_f, 'Poblacio_2564_estudismin.csv')
f_est_2564_n = pth.join(work_f, 'Poblacio_2564_nivellformacio.csv')

vacunes_pob_est_p_15_out = pth.join(work_f, 'vacunes_poblacio_estudis_15_out.csv')
vacunes_pob_est_p_nv_15_out = pth.join(work_f, 'vacunes_pob_est_p_nv_15_out.csv')
vacunes_pob_est_p_2464_out = pth.join(work_f, 'vacunes_poblacio_estudis_2464_out.csv')

vacunes_pob_est_p_15_dosis_out = pth.join(work_f, 'vacunes_poblacio_estudis_15_dosis_out.csv')
vacunes_pob_est_p_15_estudis_out = pth.join(work_f, 'vacunes_poblacio_estudis_15_estudis_out.csv')



db_v = pd.read_csv(f_vac)
db_v['DATA'] = pd.to_datetime(db_v['DATA'], format="%d/%m/%Y")
db_v['NO_VACUNAT'] = db_v['NO_VACUNAT'].fillna('vacunat')


db_v.columns
db_v.dtypes


# Creem un df amb només els registres que tenen municipi (eliminem les files que no tenen cap municipi)
db_v1 = db_v.copy()
db_v1.dropna(subset=["MUNICIPI_CODI"], inplace=True)
db_v1['MUNICIPI_CODI'] = db_v1['MUNICIPI_CODI'].astype(int).astype(str) #

#db_v1 = db_v.drop(columns=['SEXE', 'PROVINCIA', 'COMARCA', 'MUNICIPI', 'DISTRICTE'])
#db_v1 = db_v.drop(columns=['SEXE_CODI', 'PROVINCIA_CODI', 'COMARCA_CODI', 'MUNICIPI_CODI', 'DISTRICTE_CODI'])
#db_v.drop(columns=['SEXE', 'PROVINCIA', 'COMARCA', 'MUNICIPI', 'DISTRICTE'], inplace=True)




# Obrim l'arxiu csv amb la població
db_p = pd.read_csv(f_pob)
db_p['Codi'] = db_p['Codi'].astype(str)
db_p20 = db_p[db_p['Any'] == 2020]
db_p20['Any'].unique()

db_p20.drop(columns=['Any'], inplace=True)


db_p21 = pd.read_csv(f_pob21, header =5, sep=';', index_col=False)
db_p21.drop(index=db_p21.index[-1], axis=0, inplace=True)
db_p21['Codi'] = db_p21['Codi'].astype(int).astype(str)
db_p21['Codi'] = db_p21['Codi'].apply(lambda x: x[:-1])
db_p21.set_index('Codi', inplace=True)


# Fiquem els totals (Tots junts, homes, dones)
db_p21['Total. Total'] = db_p21[['Total. De 0 a 14 anys', 'Total. De 15 a 64 anys', 'Total. 65 anys o més']].sum(axis=1)
db_p21['Homes. Total'] = db_p21[['Homes. De 0 a 14 anys', 'Homes. De 15 a 64 anys', 'Homes. 65 anys o més']].sum(axis=1)
db_p21['Dones. Total'] = db_p21[['Dones. De 0 a 14 anys', 'Dones. De 15 a 64 anys', 'Dones. 65 anys o més']].sum(axis=1)

# Fem el precentatge que representa cada grup.
db_p21[['P Total. De 0 a 14 anys', 'P Total. De 15 a 64 anys', 'P Total. 65 anys o més']] = db_p21[['Total. De 0 a 14 anys', 'Total. De 15 a 64 anys', 'Total. 65 anys o més']].apply(lambda x: x/x.sum(), axis=1)
db_p21[['P Homes. De 0 a 14 anys', 'P Homes. De 15 a 64 anys', 'P Homes. 65 anys o més']] = db_p21[['Homes. De 0 a 14 anys', 'Homes. De 15 a 64 anys', 'Homes. 65 anys o més']].apply(lambda x: x/x.sum(), axis=1)
db_p21[['P Dones. De 0 a 14 anys', 'P Dones. De 15 a 64 anys', 'P Dones. 65 anys o més']] = db_p21[['Dones. De 0 a 14 anys', 'Dones. De 15 a 64 anys', 'Dones. 65 anys o més']].apply(lambda x: x/x.sum(), axis=1)

# db_p21[db_p21.index.isin(db_v1['MUNICIPI_CODI'].unique())]
db_p21.to_csv(f_pob21_out)


#db_p21[db_p21['Codi'] == '430311']['Literal']
db_borges = db_v1[db_v1['MUNICIPI_CODI'] == '43031']
# db_p21[db_p21['Codi'] == '43031']['Literal']
db_p21[db_p21.index == '43031']['Literal']





len(db_v1['MUNICIPI_CODI'].unique())
# len(db_p21['Codi'].unique())
len(db_p21.index.unique())



db_borges_nv = db_borges[db_borges['NO_VACUNAT'] == 'No vacunat']
db_borges_nv_d = db_borges[db_borges['NO_VACUNAT'] == 'No vacunat'].groupby(by=['SEXE', 'DOSI']).sum()[['RECOMPTE']].T


db_borges_v = db_borges[['NO_VACUNAT', 'DOSI', 'RECOMPTE']].groupby(by=['NO_VACUNAT', 'DOSI']).sum()

#db_borges_nv_d = db_borges_nv.groupby(by=['DOSI']).count()['RECOMPTE']

# NO vacunats per sexe
#db_nv_s = db_v[db_v['NO_VACUNAT'] == 'No vacunat'][['SEXE', 'DOSI','RECOMPTE']].groupby(by=['SEXE', 'DOSI'], axis=0).sum()
db_nv_s = db_v[db_v['NO_VACUNAT'] == 'No vacunat'][['SEXE', 'DOSI','RECOMPTE']].groupby(by=['SEXE', 'DOSI'], axis=0).sum().T

db_v_vac_s = db_v[['DATA', 'SEXE', 'NO_VACUNAT', 'DOSI', 'RECOMPTE']].groupby(by=['DATA', 'SEXE', 'NO_VACUNAT', 'DOSI'], axis=0).sum()

db_v_vac = db_v[['DATA', 'NO_VACUNAT', 'DOSI', 'RECOMPTE']].groupby(by=['DATA', 'NO_VACUNAT', 'DOSI'], axis=0).sum()


# Agrupo tots els vacunats i no vacunats per dosis
nv1 = db_v_vac.xs(('No vacunat', 1), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'NV_1'})
nv2 = db_v_vac.xs(('No vacunat', 2), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'NV_2'})
nv3 = db_v_vac.xs(('No vacunat', 3), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'NV_3'})
nv4 = db_v_vac.xs(('No vacunat', 4), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'NV_4'})

v1 = db_v_vac.xs(('vacunat', 1), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'V_1'})
v2 = db_v_vac.xs(('vacunat', 2), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'V_2'})
v3 = db_v_vac.xs(('vacunat', 3), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'V_3'})
v4 = db_v_vac.xs(('vacunat', 4), level=(1,2), drop_level=True).rename(columns = {'RECOMPTE':'V_4'})

vacuna_total = pd.concat([v1,v2,v3,v4,nv1,nv2,nv3,nv4], axis=1).fillna(0)

vacuna_total.to_csv(vacunes_total_out)

vacuna_total_sum = vacuna_total.fillna(0).cumsum()

vacuna_total_sum.to_csv(vacunes_total_out_sum)





# Agrupo entre vacunats i no vacunats, per sexe i dosis.

nvh1 = db_v_vac_s.xs(('Home', 'No vacunat', 1), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVH_1'})
nvh2 = db_v_vac_s.xs(('Home', 'No vacunat', 2), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVH_2'})
nvh3 = db_v_vac_s.xs(('Home', 'No vacunat', 3), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVH_3'})
nvh4 = db_v_vac_s.xs(('Home', 'No vacunat', 4), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVH_4'})
nvd1 = db_v_vac_s.xs(('Dona', 'No vacunat', 1), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVD_1'})
nvd2 = db_v_vac_s.xs(('Dona', 'No vacunat', 2), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVD_2'})
nvd3 = db_v_vac_s.xs(('Dona', 'No vacunat', 3), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVD_3'})
nvd4 = db_v_vac_s.xs(('Dona', 'No vacunat', 4), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'NVD_4'})

vh1 = db_v_vac_s.xs(('Home', 'vacunat', 1), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VH_1'})
vh2 = db_v_vac_s.xs(('Home', 'vacunat', 2), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VH_2'})
vh3 = db_v_vac_s.xs(('Home', 'vacunat', 3), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VH_3'})
vh4 = db_v_vac_s.xs(('Home', 'vacunat', 4), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VH_4'})
vd1 = db_v_vac_s.xs(('Dona', 'vacunat', 1), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VD_1'})
vd2 = db_v_vac_s.xs(('Dona', 'vacunat', 2), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VD_2'})
vd3 = db_v_vac_s.xs(('Dona', 'vacunat', 3), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VD_3'})
vd4 = db_v_vac_s.xs(('Dona', 'vacunat', 4), level=(1,2,3), drop_level=True).rename(columns = {'RECOMPTE':'VD_4'})



vacuna_total_s = pd.concat([vh1,vh2,vh3,vh4,vd1,vd2,vd3,vd4,nvh1,nvh2,nvh3,nvh4,nvd1,nvd2,nvd3,nvd4], axis=1).fillna(0)

vacuna_total_s.to_csv(vacunes_total_out_s)

vacuna_total_s_sum = vacuna_total_s.fillna(0).cumsum()

vacuna_total_s_sum.to_csv(vacunes_total_out_s_sum)


# df amb només els vacunats / no vacunats
vac_total = db_v_vac.xs('vacunat', level=1, drop_level=True)
no_vac_total = db_v_vac.xs('No vacunat', level=1, drop_level=True)




# Agrupar per municipi

db_v1_m = db_v1[['MUNICIPI_CODI', 'SEXE', 'NO_VACUNAT', 'DOSI', 'RECOMPTE']].groupby(by=['MUNICIPI_CODI', 'NO_VACUNAT', 'DOSI', 'SEXE'], axis=0).sum()
# https://stackoverflow.com/questions/26255671/pandas-column-values-to-columns
# aa.reset_index()


db_mun = db_v1_m.reset_index().pivot(index='MUNICIPI_CODI', columns=['NO_VACUNAT', 'DOSI', 'SEXE'], values='RECOMPTE')

db_p21['Total. Total'] = db_p21[['Total. De 0 a 14 anys', 'Total. De 15 a 64 anys', 'Total. 65 anys o més']].sum(axis=1)

db_mun.loc[:, ('No vacunat',)]

db_mun.loc[:, (slice(None), 1)]

db_mun['No vacunat', 1, "Dona"]

db_mun2 = db_mun.copy()

db_mun2['Total', 1, "Home"] = db_mun.loc[:, (slice(None), 1, "Home")].sum(axis=1)
db_mun2['Total', 2, "Home"] = db_mun.loc[:, (slice(None), 2, "Home")].sum(axis=1)
db_mun2['Total', 3, "Home"] = db_mun.loc[:, (slice(None), 3, "Home")].sum(axis=1)

db_mun2['Total', 1, "Dona"] = db_mun.loc[:, (slice(None), 1, "Dona")].sum(axis=1)
db_mun2['Total', 2, "Dona"] = db_mun.loc[:, (slice(None), 2, "Dona")].sum(axis=1)
db_mun2['Total', 3, "Dona"] = db_mun.loc[:, (slice(None), 3, "Dona")].sum(axis=1)


db_mun2['No vacunat', 1, "Total"] = db_mun.loc[:, ('No vacunat', 1)].sum(axis=1)
db_mun2['No vacunat', 2, "Total"] = db_mun.loc[:, ('No vacunat', 2)].sum(axis=1)
db_mun2['No vacunat', 3, "Total"] = db_mun.loc[:, ('No vacunat', 3)].sum(axis=1)

db_mun2['vacunat', 1, "Total"] = db_mun.loc[:, ('vacunat', 1)].sum(axis=1)
db_mun2['vacunat', 2, "Total"] = db_mun.loc[:, ('vacunat', 2)].sum(axis=1)
db_mun2['vacunat', 3, "Total"] = db_mun.loc[:, ('vacunat', 3)].sum(axis=1)

db_mun2['Total', 1, "Total"] = db_mun2.loc[:, ('Total', 1)].sum(axis=1)
db_mun2['Total', 2, "Total"] = db_mun2.loc[:, ('Total', 2)].sum(axis=1)
db_mun2['Total', 3, "Total"] = db_mun2.loc[:, ('Total', 3)].sum(axis=1)

db_mun_p = pd.DataFrame(index=db_mun2.index, columns=db_mun2.columns)
db_mun_p.drop(columns='Total', level=0, inplace=True)

# db_mun_p = pd.DataFrame(index=db_mun2.index)



db_mun_p[[('No vacunat', 1, "Total"), ('vacunat', 1, "Total")]] = db_mun2[[('No vacunat', 1, "Total"), ('vacunat', 1, "Total")]].apply(lambda x: x/x.sum(), axis=1)
db_mun_p[[('No vacunat', 2, "Total"), ('vacunat', 2, "Total")]] = db_mun2[[('No vacunat', 2, "Total"), ('vacunat', 2, "Total")]].apply(lambda x: x/x.sum(), axis=1)
db_mun_p[[('No vacunat', 3, "Total"), ('vacunat', 3, "Total")]] = db_mun2[[('No vacunat', 2, "Total"), ('vacunat', 3, "Total")]].apply(lambda x: x/x.sum(), axis=1)

db_mun_p[[('No vacunat', 1, "Home"), ('vacunat', 1, "Home")]] = db_mun2[[('No vacunat', 1, "Home"), ('vacunat', 1, "Home")]].apply(lambda x: x/x.sum(), axis=1)
db_mun_p[[('No vacunat', 2, "Home"), ('vacunat', 2, "Home")]] = db_mun2[[('No vacunat', 2, "Home"), ('vacunat', 2, "Home")]].apply(lambda x: x/x.sum(), axis=1)
db_mun_p[[('No vacunat', 3, "Home"), ('vacunat', 3, "Home")]] = db_mun2[[('No vacunat', 3, "Home"), ('vacunat', 3, "Home")]].apply(lambda x: x/x.sum(), axis=1)

db_mun_p[[('No vacunat', 1, "Dona"), ('vacunat', 1, "Dona")]] = db_mun2[[('No vacunat', 1, "Dona"), ('vacunat', 1, "Home")]].apply(lambda x: x/x.sum(), axis=1)
db_mun_p[[('No vacunat', 2, "Dona"), ('vacunat', 2, "Dona")]] = db_mun2[[('No vacunat', 2, "Dona"), ('vacunat', 2, "Dona")]].apply(lambda x: x/x.sum(), axis=1)
db_mun_p[[('No vacunat', 3, "Dona"), ('vacunat', 3, "Dona")]] = db_mun2[[('No vacunat', 3, "Dona"), ('vacunat', 3, "Dona")]].apply(lambda x: x/x.sum(), axis=1)


db_mun_p.to_csv(vacunes_pob_p_out)
db_mun_p_m = db_mun_p.join(db_p21)
db_mun_p_m.to_csv(vacunes_pob_p_m_out)

# df amb només els no vacunats totals
db_mun_p_nv_t = db_mun_p.loc[:, ('No vacunat', slice(None), 'Total')]
db_mun_p_nv_t.columns = db_mun_p_nv_t.columns.droplevel(level=[0,2])

# db_mun2.loc[:, (slice(None), 1, "Home")]



# db_mun2.columns
#

# Percentatge de gent amb com a molt aquest nivell d'estudis (+15 anys)
db_est_15 = pd.read_csv(f_est_15, header =8, sep=',', index_col=False)
db_est_15.drop(index=db_est_15.index[-1], axis=0, inplace=True)
db_est_15['Codi'] = db_est_15['Codi'].astype(int).astype(str)
db_est_15['Codi'] = db_est_15['Codi'].apply(lambda x: x[:-1])
db_est_15.set_index('Codi', inplace=True)

db_mun_p_est_15 = db_mun_p.join(db_est_15)
db_mun_p_est_15 = db_mun_p_est_15.join(db_p21['Total. Total'])
db_mun_p_est_15.to_csv(vacunes_pob_est_p_15_out)

db_mun_p_nv_est_15 = db_mun_p_nv_t.join(db_est_15)
db_mun_p_nv_est_15 = db_mun_p_nv_est_15.join(db_p21['Total. Total'])
db_mun_p_nv_est_15.to_csv(vacunes_pob_est_p_nv_15_out)

db_mun_p_nv_d = db_mun_p.loc[:, ('No vacunat', slice(None), 'Dona')]
db_mun_p_nv_d.columns = db_mun_p_nv_d.columns.droplevel(level=[0,2])
db_mun_p_nv_d.rename(columns={1:'Dones 1d', 2:'Dones 2d', 3:'Dones 3d'}, inplace=True)

db_mun_p_nv_h = db_mun_p.loc[:, ('No vacunat', slice(None), 'Home')]
db_mun_p_nv_h.columns = db_mun_p_nv_h.columns.droplevel(level=[0,2])
db_mun_p_nv_h.rename(columns={1:'Homes 1d', 2:'Homes 2d', 3:'Homes 3d'}, inplace=True)

db_mun_p_nv_est_15 = db_mun_p_nv_est_15.join(db_mun_p_nv_d)
db_mun_p_nv_est_15 = db_mun_p_nv_est_15.join(db_mun_p_nv_h)

db_mun_p_nv_est_15.reset_index(inplace=True)

# PER DOSIS
db_mun_p_nv_est_15_1 = db_mun_p_nv_est_15.drop(columns=[2,3])
db_mun_p_nv_est_15_1["Dosis"] = '1 Dosi'
db_mun_p_nv_est_15_1.rename(columns={1:'Recompte'}, inplace=True)
db_mun_p_nv_est_15_2 = db_mun_p_nv_est_15.drop(columns=[1,3])
db_mun_p_nv_est_15_2["Dosis"] = '2 Dosi'
db_mun_p_nv_est_15_2.rename(columns={2:'Recompte'}, inplace=True)
db_mun_p_nv_est_15_3 = db_mun_p_nv_est_15.drop(columns=[1,2])
db_mun_p_nv_est_15_3["Dosis"] = '3 Dosi'
db_mun_p_nv_est_15_3.rename(columns={3:'Recompte'}, inplace=True)

db_mun_p_nv_est_15_dosis = pd.concat([db_mun_p_nv_est_15_1, db_mun_p_nv_est_15_2, db_mun_p_nv_est_15_3])
db_mun_p_nv_est_15_dosis.to_csv(vacunes_pob_est_p_15_dosis_out)



# PER NIVELL D'ESTUDIS

db_mun_p_nv_est_15_e1 = db_mun_p_nv_est_15.copy()
# db_mun_p_nv_est_15_e1 = db_mun_p_nv_est_15.drop(columns=['Primera etapa d’educació secundària i similar',
#                                                          'Segona etapa d’educació secundària i similar',
#                                                          'Educació superior'])
db_mun_p_nv_est_15_e1["Educacio"] = 'Educació primària o inferior'
# db_mun_p_nv_est_15_e1.rename(columns={'Educació primària o inferior':'Percentatge_poblacio'}, inplace=True)
db_mun_p_nv_est_15_e1['Percentatge_poblacio'] = db_mun_p_nv_est_15_e1['Educació primària o inferior']

db_mun_p_nv_est_15_e2 = db_mun_p_nv_est_15.copy()
# db_mun_p_nv_est_15_e2 = db_mun_p_nv_est_15.drop(columns=['Educació primària o inferior',
#                                                          'Educació superior'])
db_mun_p_nv_est_15_e2["Educacio"] = 'Educació secundaria'
db_mun_p_nv_est_15_e2['Percentatge_poblacio'] = db_mun_p_nv_est_15_e2['Primera etapa d’educació secundària i similar'] + db_mun_p_nv_est_15_e2['Segona etapa d’educació secundària i similar'].astype(float)
# db_mun_p_nv_est_15_e2 = db_mun_p_nv_est_15_e2.drop(columns=['Primera etapa d’educació secundària i similar',
#                                                          'Segona etapa d’educació secundària i similar'])

db_mun_p_nv_est_15_e3 = db_mun_p_nv_est_15.copy()
# db_mun_p_nv_est_15_e3 = db_mun_p_nv_est_15.drop(columns=['Primera etapa d’educació secundària i similar',
#                                                          'Segona etapa d’educació secundària i similar',
#                                                          'Educació primària o inferior'])
db_mun_p_nv_est_15_e3["Educacio"] = 'Educació superior'
db_mun_p_nv_est_15_e3['Percentatge_poblacio'] = db_mun_p_nv_est_15_e3['Educació superior']
# db_mun_p_nv_est_15_e3.rename(columns={'Educació superior':'Percentatge_poblacio'}, inplace=True)

db_mun_p_nv_est_15_estudis = pd.concat([db_mun_p_nv_est_15_e1, db_mun_p_nv_est_15_e2, db_mun_p_nv_est_15_e3])
db_mun_p_nv_est_15_estudis.fillna(0.0, inplace=True)
db_mun_p_nv_est_15_estudis[[1, 2, 3, 'Dones 1d', 'Dones 2d', 'Dones 3d', 'Homes 1d', 'Homes 2d', 'Homes 3d']] = db_mun_p_nv_est_15_estudis[[1,2,3, 'Dones 1d', 'Dones 2d', 'Dones 3d', 'Homes 1d', 'Homes 2d', 'Homes 3d']]*100

db_mun_p_nv_est_15_estudis.to_csv(vacunes_pob_est_p_15_estudis_out, float_format='%.2f')


# db_mun_p_nv_est_15_estudis['Dones 1d'] =


# Percentatge de gent amb com a molt aquest nivell d'estudis (24-65 anys)
db_est_2564_p = pd.read_csv(f_est_2564_p, header =10, sep=',', index_col=False)
db_est_2564_p.drop(index=db_est_2564_p.index[-1], axis=0, inplace=True)
db_est_2564_p['Codi'] = db_est_2564_p['Codi'].astype(int).astype(str)
db_est_2564_p['Codi'] = db_est_2564_p['Codi'].apply(lambda x: x[:-1])
db_est_2564_p.set_index('Codi', inplace=True)

db_mun_p_est_2564 = db_mun_p.join(db_est_2564_p)
db_mun_p_est_2564.to_csv(vacunes_pob_est_p_2464_out)


# Num total de gent amb com a molt aquest nivell d'estudis (24-65 anys)
db_est_2564_n = pd.read_csv(f_est_2564_n, header =9, sep=',', index_col=False)
db_est_2564_n.drop(index=db_est_2564_n.index[-1], axis=0, inplace=True)
db_est_2564_n['Codi'] = db_est_2564_n['Codi'].astype(int).astype(str)
db_est_2564_n['Codi'] = db_est_2564_n['Codi'].apply(lambda x: x[:-1])
db_est_2564_n.set_index('Codi', inplace=True)

