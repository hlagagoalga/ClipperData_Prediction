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

print "just to check , sunday is :"
print df_sundays['total_amount'].tolist()[-1]
print "saturday is "
print df_sundays['total_amount'].tolist()[-2]

print "Individual models are being trained for individual days, please wait >>>>>>>"











################################################################################
# The folllowing is the part where I predict the Tuesday, Wednesday and Thursday.


sql_all_tuesday="""
with t_a as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '1 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1, 3
order by 1
),
t_b as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '2 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_c as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '3 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_d as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '4 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_s as (
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
order by 1),
t_i as (
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
select t_aa.estimated_date as estimated_monday, t_aa.date_arrive as date_arrive, t_aa.tuesday_estimated_import as tuesday_estimated_import,
t_bb.tuesday_estimated_import as wednesday_estimated_import,
t_cc.tuesday_estimated_import as thursday_estimated_import,t_dd.tuesday_estimated_import as friday_estimated_import,
 t_ss.total_amount as storage_zone, t_ii.total_amount as imported_value from
t_a t_aa
inner join t_b t_bb
on t_aa.estimated_date=t_bb.estimated_date
inner join t_c t_cc
on t_bb.estimated_date=t_cc.estimated_date
inner join t_s t_ss
on t_cc.estimated_date=t_ss.date_run
left join t_d t_dd
on t_ss.date_run= t_dd.estimated_date
inner join t_i t_ii
on t_aa.date_arrive=t_ii.date_arrive
"""

sql_all_wednesday="""
with t_a as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '1 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1, 3
order by 1
),
t_b as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '2 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_c as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '3 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_d as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '4 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_s as (
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
order by 1),
t_i as (
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
select t_aa.estimated_date as estimated_monday, t_bb.date_arrive as date_arrive,t_aa.tuesday_estimated_import as tuesday_estimated_import,
t_bb.tuesday_estimated_import as wednesday_estimated_import,
t_cc.tuesday_estimated_import as thursday_estimated_import,t_dd.tuesday_estimated_import as friday_estimated_import,
 t_ss.total_amount as storage_zone, t_ii.total_amount as imported_value from
t_a t_aa
inner join t_b t_bb
on t_aa.estimated_date=t_bb.estimated_date
inner join t_c t_cc
on t_bb.estimated_date=t_cc.estimated_date
inner join t_s t_ss
on t_cc.estimated_date=t_ss.date_run
left join t_d t_dd
on t_ss.date_run= t_dd.estimated_date
inner join t_i t_ii
on t_bb.date_arrive=t_ii.date_arrive
"""

sql_all_thursday="""
with t_a as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '1 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1, 3
order by 1
),
t_b as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '2 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_c as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '3 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_d as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '4 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_s as (
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
order by 1),
t_i as (
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
select t_aa.estimated_date as estimated_monday,t_cc.date_arrive as date_arrive, t_aa.tuesday_estimated_import as tuesday_estimated_import,
t_bb.tuesday_estimated_import as wednesday_estimated_import,
t_cc.tuesday_estimated_import as thursday_estimated_import,t_dd.tuesday_estimated_import as friday_estimated_import,
 t_ss.total_amount as storage_zone, t_ii.total_amount as imported_value from
t_a t_aa
inner join t_b t_bb
on t_aa.estimated_date=t_bb.estimated_date
inner join t_c t_cc
on t_bb.estimated_date=t_cc.estimated_date
inner join t_s t_ss
on t_cc.estimated_date=t_ss.date_run
left join t_d t_dd
on t_ss.date_run= t_dd.estimated_date
inner join t_i t_ii
on t_cc.date_arrive=t_ii.date_arrive
"""


sql_all_friday="""
with t_a as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '1 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1, 3
order by 1
),
t_b as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '2 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_c as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '3 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_d as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '4 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_s as (
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
order by 1),
t_i as (
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
select t_aa.estimated_date as estimated_monday,t_dd.date_arrive as date_arrive, t_aa.tuesday_estimated_import as tuesday_estimated_import,
t_bb.tuesday_estimated_import as wednesday_estimated_import,
t_cc.tuesday_estimated_import as thursday_estimated_import,t_dd.tuesday_estimated_import as friday_estimated_import,
 t_ss.total_amount as storage_zone, t_ii.total_amount as imported_value from
t_a t_aa
inner join t_b t_bb
on t_aa.estimated_date=t_bb.estimated_date
inner join t_c t_cc
on t_bb.estimated_date=t_cc.estimated_date
inner join t_s t_ss
on t_cc.estimated_date=t_ss.date_run
left join t_d t_dd
on t_ss.date_run= t_dd.estimated_date
inner join t_i t_ii
on t_cc.date_arrive+interval'1 days'=t_ii.date_arrive
"""
df_overall_tuesday=read_sql(sql_all_tuesday,con_dev)
df_overall_wednesday=read_sql(sql_all_wednesday,con_dev)
df_overall_thursday=read_sql(sql_all_thursday,con_dev)
df_overall_friday=read_sql(sql_all_friday,con_dev)

df_overall_tuesday['friday_estimated_import'].fillna(0, inplace=True)
df_overall_wednesday['friday_estimated_import'].fillna(0, inplace=True)
df_overall_thursday['friday_estimated_import'].fillna(0, inplace=True)
df_overall_friday['friday_estimated_import'].fillna(0, inplace=True)

### Until now, the data is prepared to be used.
### The following is where I need to train the model for tuesday.
### The features that need to be included are: the volatality and the refinaty runs and stock levesl.


### The following is about the volatality, here we have
TUES_VOL_1=[]
TUES_VOL_2=[]
print df_X['date_run']
for i,item in enumerate(df_overall_tuesday['date_arrive']):
	TUES_VOL_1.append(df_X.loc[df_X['date_run']==item]['VOL_1'].tolist()[0])
	TUES_VOL_2.append(df_X.loc[df_X['date_run']==item]['VOL_2'].tolist()[0])

#print len(TUES_VOL_2)==len(df_overall_tuesday)

df_overall_tuesday['VOL_1']=TUES_VOL_1
df_overall_tuesday['VOL_2']=TUES_VOL_2


#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
TUES_VAR=np.matrix(np.asarray([df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()])).T
TUES_IND=np.matrix(np.asarray(df_overall_tuesday['imported_value'].tolist())).T

print TUES_IND.shape
print TUES_VAR.shape
#tues_regr=linear_model.LinearRegression()
#tues_regr.fit(TUES_VAR,TUES_IND)

tues_ranf=ensemble.AdaBoostRegressor(RandomForestRegressor(),n_estimators=80)
tues_ranf.fit(TUES_VAR,TUES_IND)


### So far, the model is already trained.




### The following is the model training process for the wednesday.
WED_VOL_1=[]
WED_VOL_2=[]
print df_X['date_run']
for i,item in enumerate(df_overall_wednesday['date_arrive']):
	WED_VOL_1.append(df_X.loc[df_X['date_run']==item]['VOL_1'].tolist()[0])
	WED_VOL_2.append(df_X.loc[df_X['date_run']==item]['VOL_2'].tolist()[0])

#print len(TUES_VOL_2)==len(df_overall_tuesday)

df_overall_wednesday['VOL_1']=WED_VOL_1
df_overall_wednesday['VOL_2']=WED_VOL_2

# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
WED_VAR=np.matrix(np.asarray([df_overall_wednesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()])).T
WED_IND=np.matrix(np.asarray(df_overall_wednesday['imported_value'].tolist())).T

#wed_regr=linear_model.LinearRegression()
#wed_regr.fit(WED_VAR,WED_IND)

wed_ranf=ensemble.AdaBoostRegressor(RandomForestRegressor(),n_estimators=80)
wed_ranf.fit(WED_VAR,WED_IND)

print "Wednesdays model is taken care of."




### The following is the model training process for the thursday.
THUR_VOL_1=[]
THUR_VOL_2=[]
print df_X['date_run']
for i,item in enumerate(df_overall_thursday['date_arrive']):
	THUR_VOL_1.append(df_X.loc[df_X['date_run']==item]['VOL_1'].tolist()[0])
	THUR_VOL_2.append(df_X.loc[df_X['date_run']==item]['VOL_2'].tolist()[0])

#print len(TUES_VOL_2)==len(df_overall_tuesday)

df_overall_thursday['VOL_1']=THUR_VOL_1
df_overall_thursday['VOL_2']=THUR_VOL_2

# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
THUR_VAR=np.matrix(np.asarray([df_overall_thursday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()])).T
THUR_IND=np.matrix(np.asarray(df_overall_thursday['imported_value'].tolist())).T

#thur_regr=linear_model.LinearRegression()
#thur_regr.fit(THUR_VAR,THUR_IND)

thur_ranf=ensemble.AdaBoostRegressor(RandomForestRegressor(),n_estimators=80)
thur_ranf.fit(THUR_VAR,THUR_IND)

print "Thursday model is taken care of."








### The following is the model for Friday.
FRI_VOL_1=[]
FRI_VOL_2=[]
print df_X['date_run']
for i,item in enumerate(df_overall_thursday['date_arrive']):
	FRI_VOL_1.append(df_X.loc[df_X['date_run']==item]['VOL_1'].tolist()[0])
	FRI_VOL_2.append(df_X.loc[df_X['date_run']==item]['VOL_2'].tolist()[0])

#print len(TUES_VOL_2)==len(df_overall_tuesday)

df_overall_friday['VOL_1']=FRI_VOL_1
df_overall_friday['VOL_2']=FRI_VOL_2

# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
FRI_VAR=np.matrix(np.asarray([df_overall_friday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()])).T
FRI_IND=np.matrix(np.asarray(df_overall_friday['imported_value'].tolist())).T

fri_ranf=ensemble.AdaBoostRegressor(RandomForestRegressor(),n_estimators=80)
fri_ranf.fit(FRI_VAR,FRI_IND)



print "Friday model is taken care of."



### so far, the model is already trained, now we need to estimate this weeks data.
### The following is making the prediction about this week.
### Lets this week's tuesday:

sql_predict_tuesday="""
with table_tuesday as (
with t_a as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '1 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1, 3
order by 1
),
t_b as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import, date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '2 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_c as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '3 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_d as (
select date_run::date as estimated_date, sum(bbls/1000) as tuesday_estimated_import,date_arrive from archive_crude where TRIM(to_char(date_run::date, 'day'))='monday' and date_arrive=date_run::date+ interval '4 days'
and region_arrive = 'USG'
and poi_depart not in ( select distinct(poi) from as_poi where lo_country_code='US')
and grade not ilike '%us%'
and poi_arrive=0
group by 1,3
order by 1
),
t_s as (
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
order by 1),
t_i as (
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
select t_aa.estimated_date as estimated_monday, t_aa.date_arrive as date_arrive,t_aa.tuesday_estimated_import as tuesday_estimated_import,
t_bb.tuesday_estimated_import as wednesday_estimated_import,
t_cc.tuesday_estimated_import as thursday_estimated_import,t_dd.tuesday_estimated_import as friday_estimated_import,
 t_ss.total_amount as storage_zone, t_ii.total_amount as imported_value from
t_a t_aa
inner join t_b t_bb
on t_aa.estimated_date=t_bb.estimated_date
inner join t_c t_cc
on t_bb.estimated_date=t_cc.estimated_date
inner join t_s t_ss
on t_cc.estimated_date=t_ss.date_run
left join t_d t_dd
on t_ss.date_run= t_dd.estimated_date
left join t_i t_ii
on t_bb.date_arrive=t_ii.date_arrive)
select * from table_tuesday where estimated_monday='2016-08-15'
"""





TEST_TUES_VOL_1=[]
TEST_TUES_VOL_2=[]
monday_predicted=adaregr.predict(PRE_VAR)[0]
sunday_import=df_sundays['total_amount'].tolist()[-1]
saturday_import=df_sundays['total_amount'].tolist()[-2]

### This is where I get the sunday and saturday import values.

df_test_tuesday=read_sql(sql_predict_tuesday,con_dev)
df_test_tuesday['friday_estimated_import'].fillna(0, inplace=True)
for i,item in enumerate(df_test_tuesday['date_arrive']):
	TEST_TUES_VOL_1.append((monday_predicted-sunday_import)/float(monday_predicted))
	TEST_TUES_VOL_2.append((monday_predicted-saturday_import)/float(monday_predicted))
#print len(TUES_VOL_2)==len(df_overall_tuesday)
df_test_tuesday['VOL_1']=TEST_TUES_VOL_1
df_test_tuesday['VOL_2']=TEST_TUES_VOL_2
# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
TEST_TUES_VAR=np.matrix(np.asarray([df_test_tuesday['tuesday_estimated_import'].tolist(),df_test_tuesday['wednesday_estimated_import'].tolist(),df_test_tuesday['thursday_estimated_import'].tolist(),df_test_tuesday['friday_estimated_import'].tolist(),df_test_tuesday['storage_zone'].tolist(),df_test_tuesday['VOL_1'].tolist(),df_test_tuesday['VOL_2'].tolist()])).T
TEST_TUES_PREDICTED=tues_ranf.predict(TEST_TUES_VAR)
print "The predicted result for tuesday is as below: "
print TEST_TUES_PREDICTED[0]


### The following si the actual prediction for wednesday.
df_test_wednesday=read_sql(sql_predict_tuesday,con_dev)
TEST_WED_VOL_1=[]
TEST_WED_VOL_2=[]
for i,item in enumerate(df_test_tuesday['date_arrive']):
	TEST_WED_VOL_1.append((monday_predicted-sunday_import)/float(monday_predicted))
	TEST_WED_VOL_2.append((monday_predicted-saturday_import)/float(monday_predicted))
#print len(TUES_VOL_2)==len(df_overall_tuesday)
df_test_wednesday['VOL_1']=TEST_TUES_VOL_1
df_test_wednesday['VOL_2']=TEST_TUES_VOL_2
# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
TEST_WED_VAR=np.matrix(np.asarray([df_test_tuesday['tuesday_estimated_import'].tolist(),df_test_tuesday['wednesday_estimated_import'].tolist(),df_test_tuesday['thursday_estimated_import'].tolist(),df_test_tuesday['friday_estimated_import'].tolist(),df_test_tuesday['storage_zone'].tolist(),df_test_tuesday['VOL_1'].tolist(),df_test_tuesday['VOL_2'].tolist()])).T
TEST_WED_PREDICTED=wed_ranf.predict(TEST_WED_VAR)
print "The predicted result for wednesday is as below: "
print TEST_WED_PREDICTED[0]


### The following si the actual prediction for thursday.
df_test_thursday=read_sql(sql_predict_tuesday,con_dev)
TEST_THUR_VOL_1=[]
TEST_THUR_VOL_2=[]
for i,item in enumerate(df_test_tuesday['date_arrive']):
	TEST_THUR_VOL_1.append((monday_predicted-sunday_import)/float(monday_predicted))
	TEST_THUR_VOL_2.append((monday_predicted-saturday_import)/float(monday_predicted))
#print len(TUES_VOL_2)==len(df_overall_tuesday)
df_test_thursday['VOL_1']=TEST_TUES_VOL_1
df_test_thursday['VOL_2']=TEST_TUES_VOL_2
# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
TEST_THUR_VAR=np.matrix(np.asarray([df_test_tuesday['tuesday_estimated_import'].tolist(),df_test_tuesday['wednesday_estimated_import'].tolist(),df_test_tuesday['thursday_estimated_import'].tolist(),df_test_tuesday['friday_estimated_import'].tolist(),df_test_tuesday['storage_zone'].tolist(),df_test_tuesday['VOL_1'].tolist(),df_test_tuesday['VOL_2'].tolist()])).T
TEST_THUR_PREDICTED=thur_ranf.predict(TEST_THUR_VAR)
print "The predicted result for thursday is as below: "
print TEST_THUR_PREDICTED[0]

### The following si the actual prediction for friday
df_test_friday=read_sql(sql_predict_tuesday,con_dev)
TEST_FRI_VOL_1=[]
TEST_FRI_VOL_2=[]
for i,item in enumerate(df_test_tuesday['date_arrive']):
	TEST_FRI_VOL_1.append((monday_predicted-sunday_import)/float(monday_predicted))
	TEST_FRI_VOL_2.append((monday_predicted-saturday_import)/float(monday_predicted))
#print len(TUES_VOL_2)==len(df_overall_tuesday)
df_test_friday['VOL_1']=TEST_TUES_VOL_1
df_test_friday['VOL_2']=TEST_TUES_VOL_2
# print df_overall_tuesday
#M=[df_overall_tuesday['tuesday_estimated_import'].tolist(),df_overall_tuesday['wednesday_estimated_import'].tolist(),df_overall_tuesday['thursday_estimated_import'].tolist(),df_overall_tuesday['friday_estimated_import'].tolist(),df_overall_tuesday['storage_zone'].tolist(),df_overall_tuesday['VOL_1'].tolist(),df_overall_tuesday['VOL_2'].tolist()]
TEST_FRI_VAR=np.matrix(np.asarray([df_test_tuesday['tuesday_estimated_import'].tolist(),df_test_tuesday['wednesday_estimated_import'].tolist(),df_test_tuesday['thursday_estimated_import'].tolist(),df_test_tuesday['friday_estimated_import'].tolist(),df_test_tuesday['storage_zone'].tolist(),df_test_tuesday['VOL_1'].tolist(),df_test_tuesday['VOL_2'].tolist()])).T
TEST_FRI_PREDICTED=fri_ranf.predict(TEST_FRI_VAR)
print "The predicted result for friday is as below: "
print TEST_FRI_PREDICTED[0]
print "Overall sum for this is expected to be:"
print sum([monday_predicted, sunday_import, saturday_import, TEST_WED_PREDICTED[0],TEST_TUES_PREDICTED[0], TEST_THUR_PREDICTED[0],TEST_FRI_PREDICTED[0]])
print "Daily Average for this week is expected to be:"
print sum([monday_predicted, sunday_import, saturday_import, TEST_WED_PREDICTED[0],TEST_TUES_PREDICTED[0], TEST_THUR_PREDICTED[0],TEST_FRI_PREDICTED[0]])/7
print "My program took", (time.time() - start_time)/60, "minutes to run"
