import html

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
    output = []
    for result in db_results:
        output.append(f"<li>{html.escape(result[1])}</li>")
    return "\n".join(output)