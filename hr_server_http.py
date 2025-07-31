from hr_mcp_server import server
from mcp.server.streamable_http import run_http_server

if __name__ == "__main__":
    print("🔌 Launching HR MCP Server via HTTP...")
    run_http_server(server)  
