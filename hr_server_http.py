from hr_mcp_server import server
from mcp.server.streamable_http import run_http_server

app = run_http_server(server)
