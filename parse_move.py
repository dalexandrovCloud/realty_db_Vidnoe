import requests
from bs4 import BeautifulSoup
import csv
import subprocess
import pandas as pd
import re


# парсинг страницы по селекторам
def parse_move(page_num, url, city):
	def try_expt(func):
		try:
			resultrow.append(func())
		except Exception:
			resultrow.append('-')
			pass
			
	while True:	
		result = requests.get(url.format(city, page_num))
		soup = BeautifulSoup(result.content, 'html.parser')
		flats_list = soup.select('.search-item.move-object')
		last_page = soup.find_all(href='?page=2&similar=1&limit=100')
		last_page2 = soup.select('.no-search')
		result_flatlist = []
		print('+'*20, page_num)

		# определение последней страницы
		if (len(last_page) != 0) or (len(last_page2) != 0) or (page_num == 1000):
			print('пустая страница - выход')
			return -1

		for flat in flats_list:
			resultrow = []
			try_expt(lambda :flat.select_one('.search-item__header a').get('href').split('/')[-2].split('_')[-1])
			try_expt(lambda :''.join(flat.select_one('.search-item__price-values').text.split(' ')[-8:]))			
			try_expt(lambda :' '.join(re.findall('\S+', flat.select_one('.search-item__item-property.search-item__location').text.strip().replace('.', '/'))))
			try_expt(lambda :flat.select_one('.search-item__item-property.search-item__road').text.strip())
			try_expt(lambda :flat.select_one('.search-item__item-property.search-item__car-time').text.strip())
			try_expt(lambda :' '.join(re.findall('\S+', flat.select_one('.search-item__properties').text.strip().split(':', 1)[1])))
			try_expt(lambda :flat.select_one('.search-item__item-property.search-item__last-update').text.strip())
			result_flatlist.append(resultrow)
		page_num+=1
		return result_flatlist

# построчная запись результата парсинга в файл
def append_row(path, row):
	with open(path, 'a', encoding='utf8') as file:
		writer = csv.writer(file)
		writer.writerow(row)

# парсинг с пагинацией	
def get_data(path, city):
	url = 'https://move.ru/{}/kvartiry/?page={}&limit=100'
	# url = 'https://move.ru/kvartiry/?page={}&limit=100' #Moscow
	# city = 'moskovskaya_oblast'
	# city = 'chehov-reg'
	# city = 'lenisk-reg'
	# city = 'zaraysk-reg'
	# city = 'vidnoe'	
	page_num = 1
	while True:
		result = parse_move(page_num, url, city)
		if result == -1:
			break
		else: 
			for row in result:
				append_row(path, row)
		page_num += 1

		

