import os
import re
import logging
import time
import threading
import html
import random
from typing_extensions import TypeGuard
from urllib.parse import urlparse
import cherrypy
import sqlite3
from bs4 import BeautifulSoup
import requests

logging.basicConfig(level=logging.DEBUG)
db_name = "./index.db"
queue = []
start_url_list = [
	"https://www.1337core.de/",
	"https://de.wikipedia.org/",
	"https://stackoverflow.com/questions",
	"https://www.tagesschau.de/",
	"https://www.heise.de/"
]


def main():
	init_db()
	load_saved_queue()
	threading.Thread(target=loop_queue_worker).start()
	cherrypy.quickstart(WebServer())


class WebServer(object):
	@cherrypy.expose
	def index(self, query=None):
		if query:
			return f"""
			{get_html_header(query)}
			{get_query_result(query)}
			{get_html_footer()}
			"""
		else:
			return f"""
			{get_html_header()}
			{get_html_footer()}
			"""

def init_db():
	db = sqlite3.connect(db_name)
	result_index = db.execute(
		"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='index'"
	).fetchone()
	if result_index[0] == 0:
		logging.debug("No index found. Create one.")
		db.execute(
			"""CREATE TABLE 'index'
			(ID				INTEGER	PRIMARY KEY	AUTOINCREMENT,
			url				TEXT	NOT NULL,
			title			TEXT	NOT NULL,
			description		TEXT	NOT NULL,
			quality			INTEGER	NOT NULL);
			"""
		)
		for url in start_url_list:
			queue.append(url)

	result_queue = db.execute(
		"SELECT count(name) FROM sqlite_master WHERE type='table' AND name='queue'"
	).fetchone()
	if result_queue[0] == 0:
		logging.debug("No queue found. Create one.")
		db.execute(
			"""CREATE TABLE 'queue'
			(ID				INTEGER	PRIMARY KEY	AUTOINCREMENT,
			url				TEXT	NOT NULL);
			"""
		)
	db.commit()
	db.close()


def load_saved_queue():
	db = sqlite3.connect(db_name)
	result_queue = db.execute("SELECT * FROM 'queue'").fetchall()
	db.commit()
	db.close()
	logging.debug(f"Load queue: {len(result_queue)}")
	for result in result_queue:
		queue.append(result[1])


def get_query_result(query):
	db = sqlite3.connect(db_name)
	query_parts = query.split(" ")
	results = []
	# full title hit
	db = sqlite3.connect(db_name)
	result_query_titles = db.execute("	SELECT * FROM 'index' WHERE TITLE LIKE (?)", [query]).fetchmany(1000)
	db.close()
	if result_query_titles:
		return get_result_list(result_query_titles)
	return "NOT FOUND"




def insert_in_db(url, title, description, quality):
	db = sqlite3.connect(db_name)
	logging.debug(f"Insert in index: {url}")
	db.execute(
		f"""INSERT INTO 'index' (url, title, description, quality)  
		VALUES (?, ?, ?, ?);
		""", (url, title, description, quality)
	)
	db.commit()
	db.close()
	return True


def save_queue_in_db():
	db = sqlite3.connect(db_name)
	logging.debug(f"Save queue: {len(queue)}")
	for url in queue:
		db.execute(
			f"INSERT INTO 'queue' (url) VALUES (?);", [url]
		)
	db.commit()
	db.close()


def loop_queue_worker():
	counter = 0
	while True == True:
		if len(queue) > 0:
			crawl_site(queue.pop(0))
			counter = counter + 1
		else:
			time.sleep(1)
		if counter % 100 == 0:
			random.shuffle(queue)
			save_queue_in_db()


def add_queue(url):
	if url not in queue and not is_in_index(url):
		queue.append(url)


def add_queue_top(url):
	if url not in queue and not is_in_index(url):
		queue.insert(0, url)


def is_in_index(url):
	db = sqlite3.connect(db_name)
	bool_result = db.execute("SELECT * from 'index' WHERE url = (?)", [url]).fetchone()
	db.commit()
	db.close()
	return bool_result


def crawl_site(url):
	# request website
	logging.debug(f"Crawl url: {url}")
	site_response = request_url(url)
	
	# filter bad responses
	if not site_response:
		return

	# parse content
	soup = BeautifulSoup(site_response.text, "html.parser")

	# parsed current url
	parsed_url = urlparse(site_response.url)
	base_tag = soup.find("base")
	# link_array = re.findall(
	# 	r"(http|https):\/\/([\w\-_]+(?:(?:\.[\w\-_]+)+))([\w\-\.,@?^=%&:/~\+#]*[\w\-\@?^=%&/~\+#])?",
	# 	site_response.text,
	# )

	# # get http links
	# for link_parts in link_array:
	# 	full_url = link_parts[0] + "://" + link_parts[1] + link_parts[2]
	# 	if parsed_url.hostname in full_url:
	# 		add_queue(full_url)

	# get html links
	a_tags = soup.find_all("a")
	for a_tag in a_tags:
		full_link = ""
		link = a_tag.attrs.get("href")
		if not link:
			continue

		if link and link.startswith("/"):
			port = ""
			if parsed_url.port:
				port = ":" + str(parsed_url.port)
			full_link = (
				parsed_url.scheme + "://" + parsed_url.hostname + port + link
			)
			add_queue(full_link)

		# check base tag urls
		if base_tag:
			base = base_tag.attrs.get("href")
			if base:
				port = ""
				if parsed_url.port:
					port = ":" + str(parsed_url.port)
				full_link = parsed_url.scheme + "://" + base + port + link
				add_queue(full_link)

		# absolut urls to same domain
		if link and link.startswith("http"):
			if parsed_url.hostname in link:
				add_queue(link)
			else:
				add_queue_top(link)

	# save urls in index
	title = ""
	if soup.find("title"):
		title = soup.find("title").get_text()
		title = title.strip()
		title = title.replace("\t", "").replace("\n", " ")
	
	# check quality
	if len(soup.findAll("p")) >= 3 and len(title) > 0:
		get_paras = []
		for par in soup.findAll("p"):
			get_paras.append(par.get_text())
		all_paras = " ".join(get_paras)
		if len(all_paras) >= 500:
			description = " ".join(get_paras)
			description = description.replace("\t", "").replace("\n", " ")
			insert_in_db(url, title, description[0:500], 0)
		else:
			logging.debug("NO INDEX: NOT ENOUGH TEXT")
	else:
		logging.debug("NO INDEX: NOT ENOUGH PARAGRAPHS OR TITLE")


def request_url(url):
	try:
		session = requests.session()
		session.headers[
			"User-Agent"
		] = "Bester Crawler der Welt!"
		header = session.head(url=url, timeout=5)

		# check status code
		if header.status_code >= 300:
			logging.debug("NO CRAWL: HTTP Error Code!")
			return False

		# check content size
		if not header.headers.get("Content-Length") == None and int(header.headers.get("Content-Length")) >= 1000000:
			logging.debug("NO CRAWL: Size too big!")
			return False

		# check content type
		one_allowed_content_type = False
		for allowed_content_type in ["html", "plain", "xml", "text", "json"]:
			if (
				not header.headers.get("content-type")
				or allowed_content_type in header.headers.get("content-type").lower()
			):
				one_allowed_content_type = True
		if not one_allowed_content_type:
			logging.debug("NO CRAWL: Wrong content-type!")
			return False

		response = session.get(url=url, timeout=3)
		session.close()
		return response
	except Exception as e:
		logging.debug(e)
		return False

def get_html_header(query=None):
	if not query:
		query = ""
	return f"""
	<html><body>
		<h1>Suchmaschine</h1>
		<form method="GET">
			<input type="text" name="query" value="{html.escape(query)}"/>
			<button>Suche</button>
		</form>
	"""

def get_html_footer():
	return "</body></html>"

def get_result_list(db_results):
	result = "<li>"
	for result in db_results:
		result = f"{result}{html.escape(result[1])}"
	result = f"{result}</li>"
	return result

def __del__():
	save_queue_in_db()


if __name__ == "__main__":
	main()