# -*- coding: utf-8 -*-
import subprocess
import os
from datetime import datetime
import sqlite3
import pandas as pd
from flask import Flask 
import json
from parse_move import get_data

app = Flask(__name__)

# запуск процесса очистки данных
def clear_data(path):
	subprocess.call(['spark-submit', 'clear_move.py', path], shell=True)

# работа с таблицами в созданной базе данных
class DB:

	def __init__(self):
		self.conn = sqlite3.connect('movedatabase.db')
		self.cursor = self.conn.cursor()

	def createFlatTable(self):
		self.cursor.execute('''
			create table if not exists flat(
				id integer primary key autoincrement,
				flat_id integer,
				rooms integer,
				m2 integer,
				price int,
				price_m int,
				city varchar(128),
				floor varchar(128),
				m2_room varchar(128),
				region varchar(128),
				highway varchar(128),
				mkad_km int,
				city_type varchar(128),
				update_date varchar(128),
				start_dttm datetime default current_timestamp,
				end_dttm datetime default (datetime('2999-12-31 23:59:59'))
			);
			''')

		self.cursor.execute('''
			create view if not exists v_flat as
				select
				id,
				flat_id,
				rooms,
				m2,
				price,
				price_m,
				city,
				floor,
				m2_room,
				region,
				highway,
				mkad_km,
				city_type,
				update_date
			from flat
			where current_timestamp between start_dttm and end_dttm
			;
			''')


	def createTableNewRows(self):
		#create flat_01 добавление новых объявлений
		self.cursor.execute('''
			create table flat_01 as
				select 
					t1.*
				from flat_00 t1
				left join v_flat t2
				on t1.flat_id = t2.flat_id
				where t2.flat_id is null; 
			''')

	def createTableUpdateRows(self):
		#create flat_02 обновление изменившихся объявлений
		self.cursor.execute('''
			create table flat_02 as
				select
					t1.*
				from flat_00 t1
				inner join v_flat t2
				on t1.flat_id = t2.flat_id
				and ( t1.rooms <> t2.rooms 
				or t1.m2 <> t2.m2 
				or t1.price <> t2.price 
				or t1.price_m <> t2.price_m
				or t1.city <> t2.city
				or t1.floor <> t2.floor
				or t1.m2_room <> t2.m2_room
				or t1.region <> t2.region
				or t1.highway <> t2.highway
				or t1.mkad_km <> t2.mkad_km
				or t1.city_type <> t2.city_type
				or t1.update_date <> t2.update_date
				)
			''')

	def createTablePriceUp(self):
		#create flat_price_up объявления с выросшей ценой
		self.cursor.execute('''
			create view if not exists city_price_up as
				select distinct
					t1.city
				from flat_00 t1
				inner join v_flat t2
				on t1.flat_id = t2.flat_id
				and (t1.price > t2.price 
				)
			''')

	def createTablePriceDown(self):
		#create flat_price_down объявления со сниженной ценой
		self.cursor.execute('''
			create view if not exists flat_price_down as
				select
					t1.*
					,t2.*
				from flat_00 t1
				inner join v_flat t2
				on t1.flat_id = t2.flat_id
				and (t1.price < t2.price 
				)
			''')

	def createTableDeleteRows(self):
		#create flat_03 удаление неактуальных объявлений
		self.cursor.execute('''
			create table flat_03 as
				select 
				t1.flat_id
				from v_flat t1
				left join flat_00 t2
				on t1.flat_id = t2.flat_id
				where t2.flat_id is null;
		''')

	def createTableNewFlat(self):
		#create flat_new новые объявления
		self.cursor.execute('''
			create table flat_new as
				select 
					t1.*
				from flat_00 t1
				left join v_flat t2
				on t1.flat_id = t2.flat_id
				where t2.flat_id is null; 
			''')

	def updateFlatTable(self):
		
		self.cursor.execute('''
			update flat
			set end_dttm = current_timestamp
			where flat_id in (select flat_id from flat_03)
			and end_dttm = datetime('2999-12-31 23:59:59');
		''')

		self.cursor.execute('''
			update flat
			set end_dttm = current_timestamp
			where flat_id in (select flat_id from flat_02)
			and end_dttm = datetime('2999-12-31 23:59:59');

		''')

		self.cursor.execute('''
			insert into flat (flat_id, rooms, m2, price, price_m, city, 
			floor, m2_room, region, highway, mkad_km, city_type, update_date) 
			select flat_id, rooms, m2, price, price_m, city, 
			floor, m2_room, region, highway, mkad_km, city_type, update_date from flat_02;
		''')

		self.cursor.execute('''
			insert into flat (flat_id, rooms, m2, price, price_m, city, 
			floor, m2_room, region, highway, mkad_km, city_type, update_date) 
			select flat_id, rooms, m2, price, price_m, city, 
			floor, m2_room, region, highway, mkad_km, city_type, update_date from flat_01;
		''')

		self.conn.commit()

	def deleteTmpTables(self):
		self.cursor.execute('''
			drop table if exists flat_00;
		''')
		self.cursor.execute('''
			drop table if exists flat_01;
		''')
		self.cursor.execute('''
			drop table if exists flat_02;
		''')
		self.cursor.execute('''
			drop table if exists flat_03;
		''')
		self.cursor.execute('''
			drop table if exists flat_new;
		''')
		self.cursor.execute('''
			drop view if exists flat_price_down;
		''')
		self.cursor.execute('''
			drop view if exists city_price_up;
		''')



@app.route('/api/get_data_by_city/<city>')	

# консолидация всех процессов приложения
def ParseDataMove(city):
	try:
		dirname = r'results/' + datetime.now().strftime("%Y_%m_%d__%H_%M_%S")
		os.mkdir(dirname)
		get_data(dirname+'/result_01.csv', city)
		clear_data(dirname)
		db = DB()
		db.createFlatTable()
		db.createTableNewFlat()
		db.createTableNewRows()
		db.createTablePriceDown()
		db.createTablePriceUp()
		db.createTableUpdateRows()
		db.createTableDeleteRows()
		db.updateFlatTable()
		
	
# сохранение информации в JSON
		def readTable(tableName):
			sql= f'select * from {tableName}'
			db.cursor.execute(sql)
			return db.cursor.fetchall()

		def sqlToJson(tableName, path):
			with open(path, 'w', encoding='utf-8') as file:
				for row in readTable(tableName):
					row_dict = {
							"id" : row[0],
							"flat_id" : row[1],
							"rooms" : row[5],
							"m2" : row[6],
							"price" : row[7],
							"price_m" : row[8],
							"city" : row[4],
							"floor" : row[10],
							"m2_room" : row[9],
							"region" : row[11],
							"highway" : row[12],
							"mkad_km" : row[13],
							"city_type" : row[3],
							"update_date" : row[2],
							}
					db_json = json.dumps(row_dict, ensure_ascii=False)
					file.write(db_json + '\n')

		def priceUpsqlToJson(tableName, path):
			with open(path, 'w', encoding='utf-8') as file:
				for row in readTable(tableName):
					row_dict = {
							"city" : row[0]
							}
					db_json = json.dumps(row_dict, ensure_ascii=False)
					file.write(db_json + '\n')

		priceUpsqlToJson('city_price_up', 'city_price_up.json')
		sqlToJson('flat_price_down', 'flat_price_down.json')
		sqlToJson('flat_new', 'flat_new.json')

		db.deleteTmpTables()
		
		return json.dumps({"status":'ok'})
	except Exception as e:
		return json.dumps({"status":'err', 'error_text': str(e)})

# запуск API
if __name__ == '__main__':
	app.debug = True
	app.run()