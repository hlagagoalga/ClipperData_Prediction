import sys
sys.path.insert(0, '../')
from utilities import *
start_time=time.time()
#### The following is where I train the model and make a prediction for Monday.
#################################################################################

con_dev=connectDB('prod')
sql_extract_data="""
with t_a as
(
/*sql_all_floating=*/
SELECT
	date_run::date as date_run,
	sum(bbls/1000) as total_amount
from
	archive_crude
where
	region_arrive = 'USG'
	and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
    and date_run<=date_arrive
	and grade not ilike '%us%'
    and poi_arrive=0
group by 1
order by 1
)
,
t_b as
(
/*sql_stored_zone */
SELECT
	date_run::date as date_run,
	sum(bbls/1000) as total_amount
from
	archive_crude a
join
	asvt_storage b on a.vessel = b.vessel and b.storage = 1 and a.date_run::date between b.date_arrive + interval '1 days' and b.date_depart
where
	a.region_arrive = 'USG'
	and a.poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
	and a.grade not ilike '%us%'
    and poi_arrive=0
group by 1
order by 1
),
t_c as
(
/* sql_not_zero*/
SELECT
	sum(bbls/1000) as total_amount,
	date_arrive as date_arrive
from
	archive_crude
where
	region_arrive = 'USG'
	and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
	and grade not ilike '%us%'
    and poi_arrive!=0
    and date_arrive>='2016-05-28'
    and date_run::date='2016-08-15'
group by date_arrive
order by 2
)
select t_aa.date_run as date_run , t_aa.total_amount as all_floating,
t_bb.total_amount as storage_zone, t_cc.total_amount as imported_value
from t_a t_aa
inner join t_b t_bb
on t_aa.date_run=t_bb.date_run
inner join t_c t_cc
on t_bb.date_run=t_cc.date_arrive

"""

sql_refinery_runs="""
select date, value from ei.ei_flat where seriesid = 'PET.W_NA_YUP_R30_PER.W' and date>='2016-05-28' order by date
"""

sql_storage_level="""
select date, value from ei.ei_flat where seriesid = 'PET.WCESTP31.W' and date>= '2016-05-28' order by date
"""



df_overall=read_sql(sql_extract_data,con_dev)
df_refinery=read_sql(sql_refinery_runs,con_dev)
df_storage_level=read_sql(sql_storage_level,con_dev)


### THe following is the code where I calculate the valitality of the import value :

DEL_VO=2
VOL_1=[]
VOL_2=[]
for i in range (0, len(df_overall['imported_value'].tolist())):
	if i==0:
		VOL_1.append(0.0)
	else:
		VOL_1.append((df_overall['imported_value'].tolist()[i]-df_overall['imported_value'].tolist()[i-1])/float(df_overall['imported_value'].tolist()[i]))

for i in range (0,len(df_overall['imported_value'].tolist())):
	if i ==0:
		VOL_2.append(0.0)
	elif i==1:
		VOL_2.append((df_overall['imported_value'].tolist()[i]-df_overall['imported_value'].tolist()[i-1])/float(df_overall['imported_value'].tolist()[i]))
	else:
		VOL_2.append((df_overall['imported_value'].tolist()[i]-df_overall['imported_value'].tolist()[i-2])/float(df_overall['imported_value'].tolist()[i]))

### The following is the code where I include the price of the futures in the model
#xl_file= pd.ExcelFile('future_price.xlsx')
#df_price=xl_file.parse('Sheet1')

####

df_X=df_overall[['date_run','all_floating','storage_zone']]
df_Y=df_overall[['date_run','imported_value']]
df_X['VOL_1']=VOL_1
df_X['VOL_2']=VOL_2


################################################################################

print df_X


#print "The following is the y, "
#print df_Y



X_train, y_train=df_X, df_Y


REG_VAR=[]
REG_IND=[]
REG_IMPORT=[]
DELTA=7

# print "The training data length is ", len(X_train)
# print "The testing data length is ", len(X_test)


for i in range(len(X_train)-DELTA+1):
  if i+DELTA<len(X_train):
    that_friday=DateToWeek(X_train['date_run'].tolist()[i+DELTA])
    if not (df_storage_level.loc[df_storage_level['date']==that_friday]['value'].empty):
	  REG_VAR.append(X_train['all_floating'].tolist()[i:i+DELTA]+X_train['storage_zone'].tolist()[i:i+DELTA]+[df_storage_level.loc[df_storage_level['date']==that_friday]['value'].tolist()[0],df_refinery.loc[df_refinery['date']==that_friday]['value'].tolist()[0],X_train['VOL_1'].tolist()[i+DELTA],X_train['VOL_2'].tolist()[i+DELTA]])
	  REG_IND.append(y_train['imported_value'].tolist()[i+DELTA])
    elif not (df_storage_level.loc[df_storage_level['date']==(that_friday-dt.timedelta(days=7))]['value'].empty):
      REG_VAR.append(X_train['all_floating'].tolist()[i:i+DELTA]+X_train['storage_zone'].tolist()[i:i+DELTA]+[df_storage_level.loc[df_storage_level['date']==(that_friday-dt.timedelta(days=7))]['value'].tolist()[0],df_refinery.loc[df_refinery['date']==(that_friday-dt.timedelta(days=7))]['value'].tolist()[0],X_train['VOL_1'].tolist()[i+DELTA],X_train['VOL_2'].tolist()[i+DELTA]])
      REG_IND.append(y_train['imported_value'].tolist()[i+DELTA])
    else:
      REG_VAR.append(X_train['all_floating'].tolist()[i:i+DELTA]+X_train['storage_zone'].tolist()[i:i+DELTA]+[df_storage_level.loc[df_storage_level['date']==(that_friday-dt.timedelta(days=14))]['value'].tolist()[0],df_refinery.loc[df_refinery['date']==(that_friday-dt.timedelta(days=14))]['value'].tolist()[0],X_train['VOL_1'].tolist()[i+DELTA],X_train['VOL_2'].tolist()[i+DELTA]])
      REG_IND.append(y_train['imported_value'].tolist()[i+DELTA])
  else:
	  break


### Here, we do the same thing for the testing data.

VAR=np.matrix(np.asarray(REG_VAR))
IND=np.asarray(REG_IND)
IND=np.matrix(np.asarray(REG_IND)).T
regr = linear_model.LinearRegression()
regr_1 = DecisionTreeRegressor(max_depth=5)
regr_2=RandomForestRegressor()

adaregr=ensemble.AdaBoostRegressor(RandomForestRegressor(),n_estimators=80)
# actually fitting the model
adaregr.fit(VAR,IND)
print "The model is trained already"

print VAR.shape

### The following is the step where I take the features and make the actual prediction.

PRE_VAR=[]
now = dt.datetime.now()
pre_friday=DateToWeek(now.date())

print "pre friday is :"
print pre_friday

sql_sundays="""
SELECT
	sum(bbls/1000) as total_amount,
	date_arrive as date_arrive
from
	archive_crude
where
	region_arrive = 'USG'
	and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
	and grade not ilike '%us%'
    and poi_arrive!=0
    and date_arrive>='2016-05-28'
    and date_run::date='2016-08-15'
group by date_arrive
order by 2
"""
df_sundays=read_sql(sql_sundays,con_dev)
pre_VOL_1=(df_sundays['total_amount'].tolist()[-1]-df_sundays['total_amount'].tolist()[-2])/float(df_sundays['total_amount'].tolist()[-1])
pre_VOL_2=(df_sundays['total_amount'].tolist()[-1]-df_sundays['total_amount'].tolist()[-3])/float(df_sundays['total_amount'].tolist()[-1])


PRE_VAR.append(X_train['all_floating'].tolist()[-7:]+X_train['storage_zone'].tolist()[-7:]+[df_storage_level.loc[df_storage_level['date']==(pre_friday-dt.timedelta(days=14))]['value'].tolist()[0],df_refinery.loc[df_refinery['date']==(pre_friday-dt.timedelta(days=14))]['value'].tolist()[0],pre_VOL_1,pre_VOL_2])
#PRE_VAR.shape

print "predicted result for monday is as below"
print adaregr.predict(PRE_VAR)[0]
