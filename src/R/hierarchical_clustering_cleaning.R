options(repos=c(CRAN="http://cran.rstudio.com"))

#  Пример применения кластерного анализа

#  Шаг 1.  Чтение данных

Abonent.01 = read.table("Здания для кластеризации.csv", header=T, sep=";", dec=",")

summary(Abonent.01)

## Подготовка данных
#   Шаг 2.  Удаление пропущенных значений

Abonent.02=na.omit(Abonent.01)
summary(Abonent.02)

Abonent.03 = Abonent.02

Abonent.03$Средняя.цена.контракта <- log(Abonent.03$Средняя.цена.контракта)
Abonent.03$Количество.контрактов <- log(Abonent.03$Количество.контрактов)

# Шаг 3.  Стандартизация переменных.

# значения в интервале от 0 до 1
Ab.max=apply(Abonent.03[,-1],2,max)
Ab.min=apply(Abonent.03[,-1],2,min)
Abonent.03=scale(Abonent.03[,-1], center = Ab.min, scale = Ab.max-Ab.min)


summary(Abonent.03)

## Анализ
#  Шаг 4.  процедура кластерного анализа

d=dist(Abonent.03, method = "euclidean")
clust.Abonent = hclust(d, "ward.D2")
clust.Abonent

#  Шаг 5.  Построение графика: дендрограмма
#  Возможны следующие версии команды

plot(clust.Abonent)

plot(clust.Abonent, hang = -1)
plot(clust.Abonent, hang = -1, labels=Abonent.02[ ,1])

#  Шаг 6.  Определение числа кластеров

# Выделим кластеры на дендрограмме
rect.hclust(clust.Abonent, k=4, border="red")
rect.hclust(clust.Abonent, k=5, border="green")
rect.hclust(clust.Abonent, k=6, border="blue")

cluster_assignments <- cutree(clust.Abonent, k = 6)
final_results <- cbind(Abonent.02, Cluster = cluster_assignments)

install.packages("writexl")
library(writexl)

write_xlsx(final_results, "результаты_кластеризации.xlsx")

#  Обзор результатов процедуры кластерного анализа
#===================================================
names(clust.Abonent)

# история объединения кластеров
clust.Abonent$merge


#  расстояния между кластерами в момент объединения
clust.Abonent$height

#  порядок следования объектов на дендрограмме
clust.Abonent$order

# метки классифицируемых объектов
clust.Abonent$labels

#  метод вычисления расстояний между кластерами
clust.Abonent$method

#  текст выполняемой команды
clust.Abonent$call

#  метод вычисления расстояний между объектами
clust.Abonent$dist.method

#================================================

# Интерпретация результатов

#  Автоматическое разбиение данных на 4 кластера
groups = cutree(clust.Abonent, k=4)

#groups

colMeans(Abonent.02[groups==1,])
colMeans(Abonent.02[groups==2,])
colMeans(Abonent.02[groups==3,])
colMeans(Abonent.02[groups_==4,])


g4.1=round(colMeans(Abonent.02[groups==1, -1]),2)
g4.2=round(colMeans(Abonent.02[groups==2, -1]),2)
g4.3=round(colMeans(Abonent.02[groups==3, -1]),2)
g4.4=round(colMeans(Abonent.02[groups==4, -1]),2)

cluster.means=data.frame(g4.1,g4.2,g4.3,g4.4)

#  Автоматическое разбиение данных на 5 кластеров
groups5 = cutree(clust.Abonent, k=5)
g5.1=round(colMeans(Abonent.02[groups5==1, -1]),2)
g5.2=round(colMeans(Abonent.02[groups5==2, -1]),2)
g5.3=round(colMeans(Abonent.02[groups5==3, -1]),2)
g5.4=round(colMeans(Abonent.02[groups5==4, -1]),2)
g5.5=round(colMeans(Abonent.02[groups5==5, -1]),2)

cluster.means.5=data.frame(g5.1,g5.2,g5.3,g5.4,g5.5)


# Определение количества кластеров с помощью графика каменистая осыпь

plot((length(Abonent.02[ ,1])-1):1,clust.Abonent$height,type='o')

plot(20:1,clust.Abonent$height[369:388],type='o')

  groups6 = cutree(clust.Abonent, k=6)
g5.1=round(colMeans(Abonent.02[groups6==1, -1]),2)
g5.2=round(colMeans(Abonent.02[groups6==2, -1]),2)
g5.3=round(colMeans(Abonent.02[groups6==3, -1]),2)
g5.4=round(colMeans(Abonent.02[groups6==4, -1]),2)
g5.5=round(colMeans(Abonent.02[groups6==5, -1]),2)
g5.6=round(colMeans(Abonent.02[groups6==6, -1]),2)

cluster.means.6=data.frame(g5.1,g5.2,g5.3,g5.4,g5.5, g5.6)

Abonent.02$Cluster=groups6


d=dist(Abonent.03,"manhattan")
clust.Abonent1 = hclust(d, "complete")
plot(clust.Abonent1, hang = -1)
rect.hclust(clust.Abonent1, k=5, border="red")
rect.hclust(clust.Abonent1, k=6, border="green")
groupsM = cutree(clust.Abonent1, k=5)
gМ5.1=round(colMeans(Abonent.02[groupsM==1, -1]),2)
gМ5.2=round(colMeans(Abonent.02[groupsM==2, -1]),2)
gM5.3=round(colMeans(Abonent.02[groupsM==3, -1]),2)
gM5.4=round(colMeans(Abonent.02[groupsM==4, -1]),2)
gM5.5=round(colMeans(Abonent.02[groupsM==5, -1]),2)

cluster.means.M=data.frame(gМ5.1,gМ5.2,gM5.3,gM5.4,gM5.5)