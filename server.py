import logging
import time
import threading
import random
from urllib.parse import urlparse
import cherrypy
import sqlite3
from bs4 import BeautifulSoup
import requests
import frontend

logging.basicConfig(level=logging.DEBUG)
db_name = "./index.db"
visited_urls = []
queue: list[str] = []
language_list = ["de", "en"]
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
	def index(self, query=None, language=None):
		if query:
			return f"""
			{frontend.get_html_header(query)}
			{get_query_result(query)}
			{frontend.get_html_footer()}
			"""
		else:
			return f"""
			{frontend.get_html_header()}
			{frontend.get_html_footer()}
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
			language		TEXT	NOT NULL,
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

def get_query_result(query: str):
	db = sqlite3.connect(db_name)
	# full title hit
	db = sqlite3.connect(db_name)
	result_query_titles = db.execute("SELECT * FROM 'index' WHERE title LIKE (?) OR description LIKE (?)", [f"%{query}%", f"%{query}%"]).fetchmany(1000)
	db.close()
	if result_query_titles:
		return frontend.get_result_list(result_query_titles)
	return "NOT FOUND"

def insert_in_db(url, title, description, language, quality):
	db = sqlite3.connect(db_name)
	logging.debug(f"Insert in index: {url}")
	db.execute(
		f"""INSERT INTO 'index' (url, title, description, language, quality)  
		VALUES (?, ?, ?, ?, ?);
		""", (url, title, description, language, quality)
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
	result = db.execute("SELECT * from 'index' WHERE url = (?)", [url]).fetchone()
	db.commit()
	db.close()
	if result is None:
		return False
	else:
		return True

def crawl_site(url):
	# filter already visited urls
	if url in visited_urls:
		return
	visited_urls.append(url)

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

	# check language
	language = ""
	if soup.find("html"):
		if soup.find("html").attrs.get("lang"):
			language = soup.find("html").attrs.get("lang").lower()
	if language not in language_list:
		logging.debug("NO INDEX: WRONG LANGUAGE")
		return

	# get html links
	a_tags = soup.find_all("a")
	for a_tag in a_tags:
		full_link = ""
		link = a_tag.attrs.get("href")
		if not link:
			continue
		link = link.lower()

		if link.startswith("java"):
			continue

		if link.startswith("/"):
			port = ""
			if parsed_url.port:
				port = ":" + str(parsed_url.port)
			full_link = (
				parsed_url.scheme + "://" + parsed_url.hostname + port + link
			)
			add_queue(full_link)
			continue

		# absolut urls to same domain
		if link.startswith("https"):
			if parsed_url.hostname in link:
				add_queue(link)
			else:
				add_queue_top(link)
			continue

		# check base tag urls
		base_tag = soup.find("base")
		if base_tag:
			base = base_tag.attrs.get("href")
			add_queue(base + link)
			continue

	# save urls in index
	title = ""
	if soup.find("title"):
		title = soup.find("title").get_text()
		title = title.strip()
		title = title.replace("\t", "").replace("\n", " ")
	
	# check quality
	if len(soup.findAll("p")) < 3 and len(title) == 0:
		logging.debug("NO INDEX: NOT ENOUGH PARAGRAPHS OR TITLE")
		return

	# check quality
	get_paras = []
	for par in soup.findAll("p"):
		get_paras.append(par.get_text())
	all_paras = " ".join(get_paras)
	if len(all_paras) < 500:
		logging.debug("NO INDEX: NOT ENOUGH TEXT")
		return

	description = all_paras
	description = description.replace("\t", "").replace("\n", " ")
	insert_in_db(url, title, description[0:500], language, 0)

def request_url(url):
	try:
		session = requests.session()
		session.headers[
			"User-Agent"
		] = "Bester Crawler der Welt!"
		header = session.head(url=url, timeout=3)

		# check status code
		if header.status_code >= 400:
			logging.debug(f"NO CRAWL: HTTP Error Code: {header.status_code}")
			return False

		# check content size
		if not header.headers.get("Content-Length") == None and int(header.headers.get("Content-Length")) >= 1000000:
			logging.debug("NO CRAWL: Size too big!")
			return False

		# check content type
		one_allowed_content_type = False
		for allowed_content_type in ["html", "plain", "text"]:
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

def __del__():
	save_queue_in_db()

if __name__ == "__main__":
	main()