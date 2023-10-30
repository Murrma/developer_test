'''
You'll need to install the following packages:
  fastapi
  pydantic
  uvicorn
  psycopg2
Once those are installed, you can run the script using something like this:
  $ uvicorn develop:app
'''

#imports
from fastapi import FastAPI, Query
import pydantic as pd
import sqlalchemy as sql
from datetime import date, timedelta
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import text
from pydantic import BaseModel

#config
DB_TYPE = "postgresql"
DB_TECH = 'psycopg2'
DB_PORT = '5432'
DB_HOST = 'localhost'
DB_NAME = 'postgres_database'
DB_USER = 'postgres'
DB_PASSWORD = 'postgres'

#schema for database
class rolls(declarative_base()):
	__tablename__ = 'coils'
	ID = sql.Column('id', sql.Integer, primary_key=True)
	leng = sql.Column('length', sql.Float, nullable=False)
	weig = sql.Column('weight', sql.Float, nullable=False)
	addd = sql.Column('add_date', sql.Date, default=date.today())
	deld = sql.Column('del_date', sql.Date, default=date(9999,12,31))

#schemas for fastapi
class roll(BaseModel):
	id: int
	length: float
	weight:float
	add_date:date
	del_date:date

class diap(BaseModel):
	id: list = [0,10000000000]
	length: list = [-1,100000000]
	weight:list = [-1,100000000]
	add_date:list = [date(1,1,1), date.today()]
	del_date:list = [date(1,1,1), date.today()]

#table and connection creation in postgresql
metadata = sql.MetaData()
coil = sql.Table(
	'coils',
	metadata,
	sql.Column('id', sql.Integer, primary_key=True),
	sql.Column('length', sql.Float, nullable=False),
	sql.Column('weight', sql.Float, nullable=False),
	sql.Column('add_date', sql.Date, default=date.today()),
	sql.Column('del_date', sql.Date, default=date(9999,12,31)))

url=f'{DB_TYPE}+{DB_TECH}://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
engine = sql.create_engine(url)
factory = sessionmaker(bind=engine)
session = factory()
metadata.create_all(engine)


#api
app = FastAPI(title = 'metal_coil')

@app.get('/coil')
def get_coil_list(id_diap:tuple=Query([-1,10]), 
				length_diap:tuple=Query([-1,1000000000]),
				weight_diap:tuple=Query([-1,1000000000]),
				add_date_diap:tuple=Query([date(1,1,1), date.today()]),
				del_date_diap:tuple=Query([date(1,1,1), date.today()])
				):

	where_const = ''

	start, stop = id_diap
	id_l=session.execute(sql.select(rolls).where(text(f'{start} <= coils.id')).where(text(f'{stop} >= coils.id'))).all()

	
	start, stop = length_diap
	l_l=session.execute(sql.select(rolls).where(text(f'{start} <= coils.length')).where(text(f'{stop} >= coils.length'))).all()
	

	start, stop = weight_diap
	w_l=session.execute(sql.select(rolls).where(text(f'{start} <= coils.weight')).where(text(f'{stop} >= coils.weight'))).all()


	start, stop = add_date_diap
	a_l=session.execute(sql.select(rolls).where(text("'" + f"{start}" + "' <= coils.add_date")).where(text("'" + f"{stop}" + "' >= coils.add_date"))).all()


	start, stop = del_date_diap
	d_l=session.execute(sql.select(rolls).where(text("'" + f"{start}" + "' <= coils.del_date")).where(text("'" + f"{stop}" + "' >= coils.del_date"))).all()


	
	pre_answer = id_l + l_l + w_l + a_l + d_l
	answer = []
	pre_answer = list(set(pre_answer))
	for item in pre_answer:
		if item in id_l and item in l_l and item in w_l and item in a_l and item in d_l:
			answer.append(item)
	
	if not answer:
		return 100
	return pre_answer

	

@app.get('/coil/stats')
def get_coil_stats(start=date(1,1,1), stop=date.today()):
	data = session.execute(sql.select(rolls).where(text("'" + f"{stop}" + "' >= coils.add_date"))).all()

	yy,mm,dd = map(int, start.split('-'))
	y,m,d = map(int, stop.split('-'))
	start, stop = date(yy,mm,dd), date(y,m,d)
	added = 0
	deleted = 0
	mid_length = 0
	mid_weight = 0
	summary_weight = 0
	max_time = timedelta(days=1)
	min_time = timedelta(days=999999999)
	dates = {}

	for num, info in enumerate(data):
		if info.rolls.deld == date(9999,12,31) or info.rolls.deld >= start:

			mid_length += info.rolls.leng
			summary_weight += info.rolls.weig

			if info.rolls.addd >= start and info.rolls.addd <= stop:
				added += 1
			if info.rolls.deld >= start and info.rolls.deld <= stop:
				deleted += 1

			if max_time < info.rolls.deld - info.rolls.addd:
				max_time = info.rolls.deld - info.rolls.addd
			if min_time > info.rolls.deld - info.rolls.addd:
				min_time = info.rolls.deld - info.rolls.addd

		reset = start
		while start < stop:
			yy,mm,dd = start.year, start.month, start.day

			if not f'{yy}-{mm}-{dd}' in dates:
				dates[f'{yy}-{mm}-{dd}'] = [0,0,0]
			if info.rolls.addd <= start and info.rolls.deld >= start and dates[f'{yy}-{mm}-{dd}'] == [0,0,0]:
				dates[f'{yy}-{mm}-{dd}'] = [info.rolls.leng, info.rolls.weig, 1]

			elif info.rolls.addd <= start and info.rolls.deld >= start:
				dates[f'{yy}-{mm}-{dd}'][0] += info.rolls.leng
				dates[f'{yy}-{mm}-{dd}'][1] += info.rolls.weig
				dates[f'{yy}-{mm}-{dd}'][2] += 1

			start += timedelta(days=1)
		start = reset

	max_c = 0
	min_c = 1000000000000000000000000000000000000000000000000000000000000000000000

	max_weig = 0
	min_weig = 1000000000000000000000000000000000000000000000000000000000000000000000
	
	max_leng = 0
	min_leng = 1000000000000000000000000000000000000000000000000000000000000000000000
	
	dat_min = 0
	dat_max = 1

	min_w = 1000000000000000000000000000000000000000000000000000000000000000000000
	for key, val in dates.items():
		print(key, val)
		if max_leng < val[0]:
			max_leng = val[0]
		if min_leng > val[0]:
			min_leng = val[0]

		if max_weig < val[1]:
			max_weig = val[1]
			dat_max = key
		if min_weig > val[1]:
			dat_min = key
			min_weig = val[1]

		if max_c < val[2]:
			c = key
			max_c = val[2]
		if min_c > val[2]:
			cc = key
			min_c = val[2]

	mid_length /= (num + 1)
	mid_weight = summary_weight / (num + 1)

	return {
	'количество добавленных рулонов': added,
	'количество удалённых рулонов': deleted,
	'средняя длина': mid_length,
	'средний вес': mid_weight,
	'максимальная длина': max_leng,
	'минимальная длина': min_leng,
	'максимальный вес': max_weig,
	'минимальный вес': min_weig,
	'суммарный вес': summary_weight,
	'максимальный промежуток между добавлением и удалением рулона ': max_time,
	'минимальный промежуток между добавлением и удалением рулона': min_time,
	'День, когда на складе находилось максимальное количество рулонов': c,
	'День, когда на складе находилось минимальное количество рулонов': cc,
	'День, когда суммарный вес рулонов на складе был максимальным': dat_max,
	'День, когда суммарный вес рулонов на складе был минимальным': dat_min}

@app.post('/coil')
def add_coil(roll:roll):
	if roll.add_date:
		new_rec = rolls(leng=roll.length, weig=roll.weight, addd=roll.add_date)		
		session.add(new_rec)
	else: 
		new_rec = rolls(leng=roll['length'], weig=roll['weight'])		
		session.add(new_rec)
	session.commit()
	return roll.id


@app.delete('/coil')
def del_coil(ID=None):
	if not ID:
		return 100 
	updated = session.query(rolls).filter(rolls.ID == ID).first()
	updated.deld = date.today()
	session.commit()
	return 200 
