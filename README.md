# HR MCP Server App

This is a custom MCP server for HR insights, deployable as a Databricks App using streamable HTTP.

## Tools Implemented

- `get_employee_info`
- `get_employee_benefits`
- `search_employees`
- `get_leave_requests`
- `get_performance_reviews`
- `get_hr_summary`

## Deployment (Databricks CLI)

```bash
databricks auth login --host https://<your-workspace>
databricks apps create mcp-hr-server
databricks sync . "/Users/<username>/mcp-hr-server"
databricks apps deploy mcp-hr-server --source-code-path "/Workspace/Users/<username>/mcp-hr-server"
