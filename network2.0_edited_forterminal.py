#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import numpy as np
import networkx as nx
import redivis
import pandas as pd
import community as com
from pandarallel import pandarallel
from functools import reduce
import datetime


# In[2]:


get_ipython().run_line_magic('env', 'REDIVIS_API_TOKEN = AAABPF1ssF9wTqKxnz/EUx+LT5psJUbF')
# set token to get data ###DO NOT PUT THIS PUBLIC###
user = redivis.user("seyeonk")
project = user.project("gharchive_seyeon:931c")
table = project.table("dep_time_irrelevant_network_output:9t8g")


# In[3]:


time_names = ["bug_strict_current_win_network_output:bjc7",
              "bug_strict_within_3_wins_network_output:x7hs",
              "bug_strict_up_to_now_network_output:xa18",
              "bug_loose_current_win_network_output:rzf8",
              "bug_loose_within_3_wins_network_output:tdmn",
              "bug_loose_up_to_now_network_output:bhnv",
              "dep_current_win_network_output:m2ks",
              "dep_within_3_wins_network_output:47tn",
              "dep_up_to_now_network_output:hfyg",
              "fea_current_win_network_output:tfwc",
              "fea_within_3_wins_network_output:4feb",
              "fea_up_to_now_network_output:py94"]

time_names_bug_strict = ["bug_strict_current_win_network_output:bjc7",
                          "bug_strict_within_3_wins_network_output:x7hs",
                          "bug_strict_up_to_now_network_output:xa18"]

time_names_bug_loose = ["bug_loose_current_win_network_output:rzf8",
                        "bug_loose_within_3_wins_network_output:tdmn",
                        "bug_loose_up_to_now_network_output:bhnv"]

time_names_dep = ["dep_current_win_network_output:m2ks",
                  "dep_within_3_wins_network_output:47tn",
                  "dep_up_to_now_network_output:hfyg"]

time_names_fea = ["fea_current_win_network_output:tfwc",
                  "fea_within_3_wins_network_output:4feb",
                  "fea_up_to_now_network_output:py94"]

no_time_names = ["bug_strict_time_irrelevant_network_output:txh8",
                 "bug_loose_time_irrelevant_repo_edge_output:p14e",
                 "dep_time_irrelevant_network_output:9t8g",
                 "fea_time_irrelevant_network_output:s34q"]

name_abb = {"bug_strict_current_win_network_output:bjc7": "_bug_str_cur",
            "bug_strict_within_3_wins_network_output:x7hs": "_bug_str_3win",
            "bug_strict_up_to_now_network_output:xa18": "_bug_str_tonow",
            "bug_loose_current_win_network_output:rzf8": "_bug_loo_cur",
            "bug_loose_within_3_wins_network_output:tdmn": "_bug_loo_3win",
            "bug_loose_up_to_now_network_output:bhnv": "_bug_loo_tonow",
            "dep_current_win_network_output:m2ks": "_dep_cur",
            "dep_within_3_wins_network_output:47tn": "_dep_3win",
            "dep_up_to_now_network_output:hfyg": "_dep_tonow",
            "fea_current_win_network_output:tfwc": "_fea_cur",
            "fea_within_3_wins_network_output:4feb": "_fea_3win",
            "fea_up_to_now_network_output:py94": "_fea_tonow",
            "bug_strict_time_irrelevant_network_output:txh8": "_bug_str_all",
            "bug_loose_time_irrelevant_repo_edge_output:p14e": "_bug_loo_all",
            "dep_time_irrelevant_network_output:9t8g": "_dep_all",
            "fea_time_irrelevant_network_output:s34q": "_fea_all"}


# In[4]:


def compute_net (repo_data):
    G=nx.from_pandas_edgelist(repo_data, 'node_A', 'node_B')
    partition = com.best_partition(G)
    vk = dict(G.degree())
    vk = list(vk.values())
    
    return pd.Series({'density': nx.density(G), 
                      'num_nodes': nx.number_of_nodes(G),
                      'num_edges': nx.number_of_edges(G),
                      'modularity': com.modularity(partition, G),
                      'deg_ave': np.mean(vk),
                      'ave_cluster': nx.average_clustering(G),
                      # 'ave_min_path': nx.average_shortest_path_length(G),
                      # 'global_effi':nx.global_efficiency(G)
                     })


# In[ ]:


#pandarallel.initialize(nb_workers=48, progress_bar=True)
#no_time_output = []
#for name in no_time_names:
#    df = project.table(name).to_dataframe()
#    no_time_net = df.groupby(['repoId_fix']).parallel_apply(compute_net).reset_index()
#    keep_same = {'repoId_fix'}
#    no_time_net.columns = ['{}{}'.format(c, '' if c in keep_same else name_abb[name])
#                   for c in no_time_net.columns]
#
#    no_time_output.append(no_time_net)
#
#no_time_merged = reduce(lambda  left,right: pd.merge(left,right,on=['repoId_fix'],
#                                            how='outer'), no_time_output)
#
#date = datetime.date.today().strftime("%Y_%m_%d")
#pd.DataFrame.to_csv(no_time_merged, 'no_time_merged_' + date + '.csv', sep=',', na_rep='NA', index=False)


# In[ ]:


# pandarallel.initialize(nb_workers=48)
# time_output = []
# for name in time_names:
#     df = project.table(name).to_dataframe()
#     time_net = df.groupby(['repoId_fix','win']).parallel_apply(compute_net).reset_index()
#     keep_same = {'repoId_fix','win'}
#     time_net.columns = ['{}{}'.format(c, '' if c in keep_same else name_abb[name])
#                    for c in time_net.columns]
    
#     time_output.append(time_net)
    
# time_merged = reduce(lambda  left,right: pd.merge(left,right,on=['repoId_fix','win'],
#                                             how='outer'), time_output)

# date = datetime.date.today().strftime("%Y_%m_%d")
# pd.DataFrame.to_csv(time_merged, 'time_merged_' + date + '.csv', sep=',', na_rep='NA', index=False)


# In[ ]:


#pandarallel.initialize(nb_workers=48)
#time_output_bug_strict = []
#for name in time_names_bug_strict:
#    df = project.table(name).to_dataframe()
#    time_net = df.groupby(['repoId_fix','win']).parallel_apply(compute_net).reset_index()
#    keep_same = {'repoId_fix','win'}
#    time_net.columns = ['{}{}'.format(c, '' if c in keep_same else name_abb[name])
#                   for c in time_net.columns]
#
#    time_output_bug_strict.append(time_net)
#
#time_merged_bug_loose = reduce(lambda  left,right: pd.merge(left,right,on=['repoId_fix','win'],
#                                            how='outer'), time_output_bug_strict)
#
#date = datetime.date.today().strftime("%Y_%m_%d")
#pd.DataFrame.to_csv(time_merged_bug_loose, 'time_merged_bug_strict_' + date + '.csv', sep=',', na_rep='NA', index=False)


# In[ ]:


pandarallel.initialize(nb_workers=48)
time_output_bug_loose = []
for name in time_names_bug_loose:
    df = project.table(name).to_dataframe()
    time_net = df.groupby(['repoId_fix','win'], sort=False).parallel_apply(compute_net).reset_index()
    keep_same = {'repoId_fix','win'}
    time_net.columns = ['{}{}'.format(c, '' if c in keep_same else name_abb[name])
                   for c in time_net.columns]
    
    time_output_bug_loose.append(time_net)
    
time_merged_bug_loose = reduce(lambda  left,right: pd.merge(left,right,on=['repoId_fix','win'],
                                            how='outer'), time_output_bug_loose)

date = datetime.date.today().strftime("%Y_%m_%d")
#pd.DataFrame.to_csv(time_merged_bug_loose, 'time_merged_bug_loose_' + date + '.csv', sep=',', na_rep='NA', index=False)

pd.DataFrame.to_csv(time_output_bug_loose[2], 'time_merged_bug_loose_tonow_' + date + '.csv', sep=',', na_rep='NA', index=False)
pd.DataFrame.to_csv(time_output_bug_loose[1], 'time_merged_bug_loose_3win_' + date + '.csv', sep=',', na_rep='NA', index=False)
pd.DataFrame.to_csv(time_output_bug_loose[0], 'time_merged_bug_loose_cur_' + date + '.csv', sep=',', na_rep='NA', index=False)


# In[ ]:


pandarallel.initialize(nb_workers=48)
time_output_dep = []
for name in time_names_dep:
    df = project.table(name).to_dataframe()
    time_net = df.groupby(['repoId_fix','win'], sort=False).parallel_apply(compute_net).reset_index()
    keep_same = {'repoId_fix','win'}
    time_net.columns = ['{}{}'.format(c, '' if c in keep_same else name_abb[name])
                   for c in time_net.columns]
    
    time_output_dep.append(time_net)
    
time_merged_dep = reduce(lambda  left,right: pd.merge(left,right,on=['repoId_fix','win'],
                                            how='outer'), time_output_dep)

date = datetime.date.today().strftime("%Y_%m_%d")
#pd.DataFrame.to_csv(time_merged_dep, 'time_merged_dep_' + date + '.csv', sep=',', na_rep='NA', index=False)

pd.DataFrame.to_csv(time_output_dep[2], 'time_merged_dep_tonow_' + date + '.csv', sep=',', na_rep='NA', index=False)
pd.DataFrame.to_csv(time_output_dep[1], 'time_merged_dep_3win_' + date + '.csv', sep=',', na_rep='NA', index=False)
pd.DataFrame.to_csv(time_output_dep[0], 'time_merged_dep_cur_' + date + '.csv', sep=',', na_rep='NA', index=False)


# In[ ]:


pandarallel.initialize(nb_workers=48)
time_output_fea = []
for name in time_names_fea:
    df = project.table(name).to_dataframe()
    time_net = df.groupby(['repoId_fix','win'], sort=False).parallel_apply(compute_net).reset_index()
    keep_same = {'repoId_fix','win'}
    time_net.columns = ['{}{}'.format(c, '' if c in keep_same else name_abb[name])
                   for c in time_net.columns]
    
    time_output_fea.append(time_net)
    
time_merged_fea = reduce(lambda  left,right: pd.merge(left,right,on=['repoId_fix','win'],
                                            how='outer'), time_output_fea)

date = datetime.date.today().strftime("%Y_%m_%d")
#pd.DataFrame.to_csv(time_merged_fea, 'time_merged_fea_' + date + '.csv', sep=',', na_rep='NA', index=False)

pd.DataFrame.to_csv(time_output_fea[2], 'time_merged_fea_tonow_' + date + '.csv', sep=',', na_rep='NA', index=False)
pd.DataFrame.to_csv(time_output_fea[1], 'time_merged_fea_3win_' + date + '.csv', sep=',', na_rep='NA', index=False)
pd.DataFrame.to_csv(time_output_fea[0], 'time_merged_fea_cur_' + date + '.csv', sep=',', na_rep='NA', index=False)

