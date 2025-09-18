from Data_preparation import data_for_clust
from Data_preparation import quantity_executors
from sklearn.cluster import KMeans

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.cluster.hierarchy import dendrogram, linkage, fcluster
from scipy.spatial.distance import pdist

data = data_for_clust('Здания_Финальная_версия_ред.xlsx', 'Финальная ред')
clust_data = data[['ИНН исполнителя', 'Средняя цена контракта', 'Количество контрактов']]

clust_data = clust_data.copy()

clust_data = clust_data.loc[data['ИНН исполнителя'] != '']

clust_data['Средняя цена контракта_log'] = np.log1p(clust_data['Средняя цена контракта'])
clust_data_new = clust_data.drop(['Средняя цена контракта', 'ИНН исполнителя'], axis=1)

distance_matrix = pdist(clust_data_new, metric='euclidean')

Z = linkage(distance_matrix, method='ward')

def scree_method():
    last_merges = Z[-10:, 2]
    last_merges_reversed = last_merges[::-1]  # переворачиваем

    plt.figure(figsize=(10, 6))
    plt.plot(range(1, len(last_merges_reversed) + 1), last_merges_reversed, 'ro-')
    plt.xlabel('Количество кластеров')
    plt.ylabel('Расстояние слияния')
    plt.title('Метод каменистой осыпи (Иерархическая кластеризация)')
    plt.grid(True)
    plt.xticks(range(1, len(last_merges_reversed) + 1))
    plt.show()

def dendrogramm():
    plt.figure(figsize=(15, 8))
    plt.title('Дендрограмма иерархической кластеризации\n(Метод Варда)')
    plt.xlabel('Индекс образца')
    plt.ylabel('Расстояние')

    dendrogram(Z,
                truncate_mode='lastp',
                p=40,
                show_leaf_counts=True,
                leaf_rotation=90,
                leaf_font_size=8,
                show_contracted=True)

    plt.legend()
    plt.tight_layout()
    plt.show()

# scree_method()
# dendrogramm()

max_distance = Z[:, 2].max()
print(max_distance)

height_for_3_clusters = Z[-3, 2]  # высота для получения 3 кластеров
print(f"Высота для 3 кластеров: {height_for_3_clusters}")

num_clusters = 4
clusters = fcluster(Z, t=4, criterion='maxclust')
print(clusters)

# Добавляем кластеры к данным
# clust_data_prepared['cluster'] = clusters
clust_data['cluster'] = clusters
# print(clusters)
# print(clust_data)

def kmeans_clustering(df, n_clusters=3):

    # Подготовка данных
    # X_scaled, scaler, numeric_columns = prepare_for_kmeans(df)

    # Выполнение K-Means
    kmeans = KMeans(n_clusters, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(df)

    # Добавляем кластеры к исходным данным
    df_clustered = clust_data.copy()
    df_clustered['cluster'] = clusters

    # Добавляем scaled features для визуализации
    # df_clustered['scaled_quantity'] = df[:, 0]
    # df_clustered['scaled_price_log'] = df[:, 1]

    return df_clustered

df_clustered = kmeans_clustering(clust_data_new, n_clusters=3)
print(df_clustered['cluster'].values)