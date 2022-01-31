__author__ = "Yongchao Huang"

import pandas as pd
pd.set_option('display.max_columns', 50)
pd.set_option('mode.chained_assignment', None)
import numpy as np
from functools import reduce

# 1. read data into pandas
wd = '/your_file_path/'
df_INPUT = pd.read_csv(wd + 'input.csv', na_values=['-']) # recognize -  as NaN values
colnames = df_INPUT.columns.values

linked_id_vec = ['source.@id', 'impactAssessment.source.@id', 'cycle.defaultSource.@id', 'site.defaultSource.@id']
# linked_id_vec1 = ['cycle.@id', 'impactAssessment.cycle.@id']
# linked_id_vec2 = ['site.@id', 'cycle.site.@id']
# linked_id_vec3 = ['source.@id', 'impactAssessment.source.@id', 'cycle.defaultSource.@id', 'site.defaultSource.@id']

def merge(df_INPUT, linked_id_vec):
    df_input = df_INPUT.copy(deep=True)
    df = df_input[~df_input[linked_id_vec[0]].isna()]
    for i in range(1, len(linked_id_vec)):
        df_index = df.index.to_numpy()
        df = df.reset_index(drop=True)

        df_join = df_input[~df_input[linked_id_vec[i]].isna()]
        df_join_index = df_join.index.to_numpy()
        df_join = df_join.reset_index(drop=True)

        drop_index = np.concatenate((df_index, df_join_index))

        unique_ids_values = df[linked_id_vec[0]].unique()
        df_temp = pd.DataFrame()
        for id_value in unique_ids_values:
            df1 = df[df[linked_id_vec[0]]==id_value].reset_index(drop=True)
            if id_value in df_join[linked_id_vec[i]].unique():
                 df2 = df_join[df_join[linked_id_vec[i]]==id_value].reset_index(drop=True)
                 if df1.shape[0]==1:
                     df1= pd.concat([df1] * df2.shape[0], ignore_index=True)
                 df_temp = df_temp.append(df1.combine_first(df2))
            else:
                df_temp = df_temp.append(df1)
        df_temp = df_temp.reset_index(drop=True)
        df = df_temp.copy(deep=True) # dynamically update df, as the ID moves right

        df_duplicate = df.copy(deep=True)
        # df_duplicate.rename([xx for xx in range(df_INPUT.shape[0], df_INPUT.shape[0]+df_duplicate.shape[0])], axis='index')
        df_duplicate.index = [xx for xx in range(df_INPUT.shape[0]+100*df_duplicate.shape[0], df_INPUT.shape[0]+101*df_duplicate.shape[0])]
        for drop_ind in drop_index:
            if drop_ind in df_input.index.to_numpy():
                df_input = df_input.drop(drop_ind)
        df_input = df_input.append(df_duplicate)
    return  df, df_input

df_output, df_input= merge(df_INPUT=df_INPUT, linked_id_vec=linked_id_vec)
# df_output1, df_input1= merge(df_INPUT=df_INPUT, linked_id_vec=linked_id_vec1)
# df_output2, df_input2= merge(df_INPUT=df_output1, linked_id_vec=linked_id_vec2)
# df_output3, df_input3= merge(df_INPUT=df_output2, linked_id_vec=linked_id_vec3)

# unit testing
expected_output = pd.read_csv(wd + 'output.csv', na_values=['-'])
# for col in expected_output.columns:
#     if expected_output[col].dtype == 'int64':
#         expected_output[col] = expected_output[col].astype(np.float64)
#     if expected_output[col].dtype == 'bool':
#         expected_output[col] = expected_output[col].astype(object)

pd.testing.assert_frame_equal(expected_output, df_output, check_dtype=False)
