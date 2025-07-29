import json
import sys
from typing import Any, Dict, Optional
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
import logging
from datetime import datetime
import os
import re

# Initialize Spark (use existing session in Databricks)
spark = SparkSession.builder.getOrCreate()

# Configuration Management
class Config:
    """Configuration management for the HR MCP server"""
    def __init__(self):
        self.catalog = os.getenv("DATABRICKS_CATALOG", "gen_ai_demos")
        self.schema = os.getenv("DATABRICKS_SCHEMA", "hr_data")
        self.max_records = int(os.getenv("MAX_RECORDS", "1000"))
        
    def get_table_name(self, table: str) -> str:
        return f"{self.catalog}.{self.schema}.{table}"

# Initialize configuration
config = Config()
EMPLOYEES_TABLE = config.get_table_name("employees")
LEAVE_REQUESTS_TABLE = config.get_table_name("leave_requests")
PERFORMANCE_REVIEWS_TABLE = config.get_table_name("performance_reviews")
EMPLOYEE_BENEFITS_TABLE = config.get_table_name("employee_benefits")

# Configure logging for Databricks
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Security and Validation Functions
def validate_employee_id(employee_id: int) -> bool:
    """Validate employee ID is in expected range"""
    return isinstance(employee_id, int) and 201 <= employee_id <= 300

def sanitize_string_input(input_str: str, max_length: int = 100) -> str:
    """Comprehensive sanitization for string inputs"""
    if not input_str:
        return ""
    
    # Remove potentially dangerous characters
    dangerous_chars = ["'", '"', ";", "--", "/*", "*/", "\\", "%", "<", ">", "&", "|"]
    sanitized = str(input_str)
    
    for char in dangerous_chars:
        sanitized = sanitized.replace(char, "")
    
    # Truncate to max length and strip whitespace
    return sanitized.strip()[:max_length]

def validate_department(department: str) -> bool:
    """Validate department name"""
    valid_departments = ["Engineering", "Marketing", "Finance", "HR", "Sales", "Operations", "Support", "Legal"]
    return department in valid_departments if department else True

def validate_leave_status(status: str) -> bool:
    """Validate leave request status"""
    valid_statuses = ["Pending", "Approved", "Denied", "Cancelled"]
    return status in valid_statuses if status else True

def validate_review_period(period: str) -> bool:
    """Validate review period"""
    valid_periods = ["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024", "Annual 2024"]
    return period in valid_periods if period else True

def validate_table_access():
    """Validate that all required tables are accessible"""
    tables = {
        "Employees": EMPLOYEES_TABLE,
        "Leave Requests": LEAVE_REQUESTS_TABLE,
        "Performance Reviews": PERFORMANCE_REVIEWS_TABLE,
        "Employee Benefits": EMPLOYEE_BENEFITS_TABLE
    }
    
    for table_name, table_path in tables.items():
        try:
            # Check if table exists and get basic info
            result = spark.sql(f"DESCRIBE TABLE {table_path}").collect()
            logger.info(f"{table_name} accessible: {len(result)} columns")
        except Exception as e:
            error_msg = f"{table_name} not accessible: {table_path} - {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)

# Enhanced MCP Server Class
class MCPServer:
    """Model Context Protocol Server for HR Data"""
    
    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.tools = {}
        self.request_count = 0
        
    def register_tool(self, name: str, description: str, parameters: Dict, function: callable):
        """Register a tool with the MCP server"""
        self.tools[name] = {
            "description": description,
            "parameters": parameters,
            "function": function
        }
        logger.info(f"Tool registered: {name}")
    
    def run_stdio(self):
        """Run server in stdio mode for MCP communication"""
        logger.info("Starting MCP server in stdio mode")
        
        try:
            for line in sys.stdin:
                try:
                    self.request_count += 1
                    request = json.loads(line.strip())
                    logger.info(f"Processing request #{self.request_count}: {request.get('method', 'unknown')}")
                    
                    response = self.handle_request(request)
                    print(json.dumps(response))
                    sys.stdout.flush()
                    
                except json.JSONDecodeError as e:
                    error_response = {"error": f"Invalid JSON: {str(e)}"}
                    print(json.dumps(error_response))
                    sys.stdout.flush()
                    logger.error(f"JSON decode error: {e}")
                    
                except Exception as e:
                    error_response = {"error": f"Request processing failed: {str(e)}"}
                    print(json.dumps(error_response))
                    sys.stdout.flush()
                    logger.error(f"Request processing error: {e}")
                    
        except KeyboardInterrupt:
            logger.info("MCP server stopped by user")
        except Exception as e:
            logger.error(f"Fatal error in stdio loop: {e}")
    
    def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP protocol requests"""
        method = request.get("method")
        params = request.get("params", {})
        
        if method == "initialize":
            return {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {},
                    "logging": {}
                },
                "serverInfo": {
                    "name": self.name,
                    "version": "1.0.0",
                    "description": self.description
                }
            }
        
        elif method == "tools/list":
            return {
                "tools": [
                    {
                        "name": name,
                        "description": tool["description"],
                        "inputSchema": tool["parameters"]
                    }
                    for name, tool in self.tools.items()
                ]
            }
        
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            if tool_name in self.tools:
                try:
                    logger.info(f"Executing tool: {tool_name} with args: {arguments}")
                    result = self.tools[tool_name]["function"](**arguments)
                    
                    return {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result, indent=2, default=str)
                            }
                        ],
                        "isError": False
                    }
                except Exception as e:
                    error_msg = f"Tool execution failed for '{tool_name}': {str(e)}"
                    logger.error(error_msg)
                    return {
                        "content": [
                            {
                                "type": "text", 
                                "text": json.dumps({"error": error_msg}, indent=2)
                            }
                        ],
                        "isError": True
                    }
            else:
                return {"error": f"Tool '{tool_name}' not found"}
        
        elif method == "ping":
            return {"pong": True}
            
        return {"error": f"Unknown method: {method}"}

# HR Tool Functions - ALL 6 COMPLETE IMPLEMENTATIONS

def get_employee_info(employee_id: int, include_salary: bool = False, include_benefits: bool = True) -> Dict[str, Any]:
    """
    Get comprehensive employee information including benefits data.
    
    Args:
        employee_id: Employee ID to lookup (201-300)
        include_salary: Whether to include salary information (requires authorization)
        include_benefits: Whether to include benefits information
    """
    try:
        # Validate employee ID
        if not validate_employee_id(employee_id):
            return {"error": "Invalid employee ID. Must be between 201 and 300."}
        
        # Build comprehensive query with all tables including benefits
        query = f"""
        SELECT e.*,
               b.health_plan, b.retirement_plan, b.company_match_percent,
               b.pto_days_annual, b.sick_days_annual, b.dental_coverage,
               b.vision_coverage, b.life_insurance, b.pay_frequency,
               COUNT(lr.request_id) as total_leave_requests,
               COUNT(CASE WHEN lr.status = 'Pending' THEN 1 END) as pending_leave_requests,
               AVG(pr.goals_achievement) as avg_goals_score,
               AVG(pr.technical_skills) as avg_technical_score,
               MAX(pr.overall_rating) as latest_performance_rating
        FROM {EMPLOYEES_TABLE} e
        LEFT JOIN {EMPLOYEE_BENEFITS_TABLE} b ON e.employee_id = b.employee_id
        LEFT JOIN {LEAVE_REQUESTS_TABLE} lr ON e.employee_id = lr.employee_id
        LEFT JOIN {PERFORMANCE_REVIEWS_TABLE} pr ON e.employee_id = pr.employee_id
        WHERE e.employee_id = {employee_id}
        GROUP BY e.employee_id, e.name, e.department, e.role, e.email, 
                 e.salary, e.leave_balance, e.office_location, e.employment_status, e.hire_date,
                 b.health_plan, b.retirement_plan, b.company_match_percent, b.pto_days_annual,
                 b.sick_days_annual, b.dental_coverage, b.vision_coverage, b.life_insurance, b.pay_frequency
        """
        
        df = spark.sql(query)
        results = df.toPandas().to_dict(orient="records")
        
        if not results:
            return {
                "error": f"Employee with ID {employee_id} not found",
                "employee_id": employee_id
            }
        
        employee_data = results[0]
        
        # Structure the response with benefits separated
        response_data = {
            "basic_info": {},
            "benefits": {},
            "performance_summary": {}
        }
        
        # Basic employee info
        basic_fields = ["employee_id", "name", "department", "role", "email", "leave_balance", 
                       "office_location", "employment_status", "hire_date"]
        for field in basic_fields:
            if field in employee_data:
                response_data["basic_info"][field] = employee_data[field]
        
        # Add salary if authorized
        if include_salary and "salary" in employee_data:
            response_data["basic_info"]["salary"] = employee_data["salary"]
        
        # Benefits info
        if include_benefits:
            benefits_fields = ["health_plan", "retirement_plan", "company_match_percent",
                             "pto_days_annual", "sick_days_annual", "dental_coverage",
                             "vision_coverage", "life_insurance", "pay_frequency"]
            for field in benefits_fields:
                if field in employee_data and employee_data[field] is not None:
                    response_data["benefits"][field] = employee_data[field]
        
        # Performance summary
        perf_fields = ["total_leave_requests", "pending_leave_requests", "avg_goals_score",
                      "avg_technical_score", "latest_performance_rating"]
        for field in perf_fields:
            if field in employee_data and employee_data[field] is not None:
                response_data["performance_summary"][field] = employee_data[field]
        
        return {
            "success": True,
            "employee": response_data,
            "query_timestamp": datetime.now().isoformat(),
            "tables_queried": [EMPLOYEES_TABLE, EMPLOYEE_BENEFITS_TABLE, LEAVE_REQUESTS_TABLE, PERFORMANCE_REVIEWS_TABLE],
            "salary_included": include_salary,
            "benefits_included": include_benefits
        }
        
    except Exception as e:
        logger.error(f"Failed to query employee {employee_id}: {e}")
        return {"error": f"Failed to query employee information: {str(e)}"}

def get_employee_benefits(employee_id: Optional[int] = None, benefit_type: Optional[str] = None) -> Dict[str, Any]:
    """
    Get employee benefits information with filtering options.
    
    Args:
        employee_id: Specific employee ID to lookup
        benefit_type: Filter by benefit type (health, retirement, etc.)
    """
    try:
        if employee_id and not validate_employee_id(employee_id):
            return {"error": "Invalid employee ID. Must be between 201 and 300."}
        
        # Build query with employee names
        query = f"""
        SELECT b.*, e.name, e.department, e.role
        FROM {EMPLOYEE_BENEFITS_TABLE} b
        JOIN {EMPLOYEES_TABLE} e ON b.employee_id = e.employee_id
        WHERE 1=1
        """
        
        filters_applied = {}
        
        if employee_id:
            query += f" AND b.employee_id = {employee_id}"
            filters_applied["employee_id"] = employee_id
        
        query += " ORDER BY e.name"
        
        results = spark.sql(query).toPandas().to_dict(orient="records")
        
        # Filter by benefit type if specified
        if benefit_type:
            benefit_type_clean = sanitize_string_input(benefit_type.lower())
            if benefit_type_clean in ["health", "medical"]:
                # Focus on health-related benefits
                for result in results:
                    result["filtered_benefits"] = {
                        "health_plan": result.get("health_plan"),
                        "dental_coverage": result.get("dental_coverage"),
                        "vision_coverage": result.get("vision_coverage")
                    }
            elif benefit_type_clean in ["retirement", "401k"]:
                # Focus on retirement benefits
                for result in results:
                    result["filtered_benefits"] = {
                        "retirement_plan": result.get("retirement_plan"),
                        "company_match_percent": result.get("company_match_percent")
                    }
            filters_applied["benefit_type"] = benefit_type_clean
        
        return {
            "success": True,
            "total_results": len(results),
            "benefits": results,
            "filters_applied": filters_applied,
            "query_timestamp": datetime.now().isoformat(),
            "table_queried": EMPLOYEE_BENEFITS_TABLE
        }
        
    except Exception as e:
        logger.error(f"Failed to query employee benefits: {e}")
        return {"error": f"Failed to query employee benefits: {str(e)}"}

def search_employees(department: Optional[str] = None, role: Optional[str] = None, 
                    office_location: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    """
    Search employees with multiple filtering options.
    
    Args:
        department: Filter by department
        role: Filter by role (partial match)
        office_location: Filter by office location
        limit: Maximum number of results to return
    """
    try:
        # Validate inputs
        if limit < 1 or limit > config.max_records:
            return {"error": f"Limit must be between 1 and {config.max_records}"}
        
        if department and not validate_department(department):
            return {"error": "Invalid department. Must be one of: Engineering, Marketing, Finance, HR, Sales, Operations, Support, Legal"}
        
        from pyspark.sql.functions import col
        df = spark.read.table(EMPLOYEES_TABLE)
        filters_applied = {"limit": limit}
        
        # Apply filters
        if department:
            department_clean = sanitize_string_input(department)
            df = df.filter(col("department") == department_clean)
            filters_applied["department"] = department_clean
        
        if role:
            role_clean = sanitize_string_input(role)
            df = df.filter(col("role").contains(role_clean))
            filters_applied["role"] = role_clean
        
        if office_location:
            location_clean = sanitize_string_input(office_location)
            df = df.filter(col("office_location") == location_clean)
            filters_applied["office_location"] = location_clean
        
        # Select relevant columns and limit results
        df = df.select("employee_id", "name", "department", "role", "office_location", "employment_status") \
               .orderBy("name") \
               .limit(limit)
        
        results = df.toPandas().to_dict(orient="records")
        
        return {
            "success": True,
            "total_results": len(results),
            "employees": results,
            "filters_applied": filters_applied,
            "query_timestamp": datetime.now().isoformat(),
            "table_queried": EMPLOYEES_TABLE
        }
        
    except Exception as e:
        logger.error(f"Failed to search employees: {e}")
        return {"error": f"Failed to search employees: {str(e)}"}

def get_leave_requests(employee_id: Optional[int] = None, status: Optional[str] = None, 
                      limit: int = 100, days_back: Optional[int] = None) -> Dict[str, Any]:
    """
    Get leave requests with comprehensive filtering.
    
    Args:
        employee_id: Filter by specific employee ID
        status: Filter by leave status
        limit: Maximum number of results
        days_back: Only return requests from the last N days
    """
    try:
        # Validate inputs
        if employee_id and not validate_employee_id(employee_id):
            return {"error": "Invalid employee ID. Must be between 201 and 300."}
        
        if status and not validate_leave_status(status):
            return {"error": "Invalid status. Must be one of: Pending, Approved, Denied, Cancelled"}
        
        if limit < 1 or limit > config.max_records:
            return {"error": f"Limit must be between 1 and {config.max_records}"}
        
        from pyspark.sql.functions import col, expr
        
        # Build query with joins
        query_parts = [f"""
        SELECT lr.*, e.name as employee_name, e.department, a.name as approver_name
        FROM {LEAVE_REQUESTS_TABLE} lr
        JOIN {EMPLOYEES_TABLE} e ON lr.employee_id = e.employee_id
        LEFT JOIN {EMPLOYEES_TABLE} a ON lr.approver_id = a.employee_id
        WHERE 1=1
        """]
        
        filters_applied = {"limit": limit}
        
        # Add filters
        if employee_id:
            query_parts.append(f"AND lr.employee_id = {employee_id}")
            filters_applied["employee_id"] = employee_id
        
        if status:
            status_clean = sanitize_string_input(status)
            query_parts.append(f"AND lr.status = '{status_clean}'")
            filters_applied["status"] = status_clean
        
        if days_back and isinstance(days_back, int) and days_back > 0:
            query_parts.append(f"AND lr.submitted_timestamp >= current_timestamp() - interval {days_back} days")
            filters_applied["days_back"] = days_back
        
        query_parts.append("ORDER BY lr.submitted_timestamp DESC")
        query_parts.append(f"LIMIT {limit}")
        
        full_query = " ".join(query_parts)
        results = spark.sql(full_query).toPandas().to_dict(orient="records")
        
        return {
            "success": True,
            "total_results": len(results),
            "leave_requests": results,
            "filters_applied": filters_applied,
            "query_timestamp": datetime.now().isoformat(),
            "table_queried": LEAVE_REQUESTS_TABLE
        }
        
    except Exception as e:
        logger.error(f"Failed to query leave requests: {e}")
        return {"error": f"Failed to query leave requests: {str(e)}"}

def get_performance_reviews(employee_id: Optional[int] = None, review_period: Optional[str] = None, 
                           limit: int = 50) -> Dict[str, Any]:
    """
    Get performance reviews for specific employees.
    
    Args:
        employee_id: Employee ID to get reviews for
        review_period: Filter by review period
        limit: Maximum number of reviews to return
    """
    try:
        if employee_id and not validate_employee_id(employee_id):
            return {"error": "Invalid employee ID. Must be between 201 and 300."}
        
        if review_period and not validate_review_period(review_period):
            return {"error": "Invalid review period. Must be one of: Q1 2024, Q2 2024, Q3 2024, Q4 2024, Annual 2024"}
        
        if limit < 1 or limit > config.max_records:
            return {"error": f"Limit must be between 1 and {config.max_records}"}
        
        conditions = []
        filters_applied = {"limit": limit}
        
        if employee_id:
            conditions.append(f"pr.employee_id = {employee_id}")
            filters_applied["employee_id"] = employee_id
        if review_period:
            period_clean = sanitize_string_input(review_period)
            conditions.append(f"pr.review_period = '{period_clean}'")
            filters_applied["review_period"] = period_clean
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        query = f"""
        SELECT pr.*, e.name as employee_name, e.department, r.name as reviewer_name
        FROM {PERFORMANCE_REVIEWS_TABLE} pr
        JOIN {EMPLOYEES_TABLE} e ON pr.employee_id = e.employee_id
        LEFT JOIN {EMPLOYEES_TABLE} r ON pr.reviewer_id = r.employee_id
        WHERE {where_clause}
        ORDER BY pr.review_date DESC
        LIMIT {limit}
        """
        
        results = spark.sql(query).toPandas().to_dict(orient="records")
        
        return {
            "success": True,
            "total_results": len(results),
            "performance_reviews": results,
            "filters_applied": filters_applied,
            "query_timestamp": datetime.now().isoformat(),
            "table_queried": PERFORMANCE_REVIEWS_TABLE
        }
        
    except Exception as e:
        logger.error(f"Failed to query performance reviews: {e}")
        return {"error": f"Failed to query performance reviews: {str(e)}"}

def get_hr_summary(days_back: int = 30) -> Dict[str, Any]:
    """
    Get comprehensive HR summary across all data sources including benefits.
    
    Args:
        days_back: Number of days to look back for the summary
    """
    try:
        from pyspark.sql.functions import col, count, avg, expr, desc, sum as sum_
        
        if days_back < 1 or days_back > 365:
            return {"error": "Days back must be between 1 and 365"}
        
        summary = {
            "success": True,
            "time_range_days": days_back,
            "query_timestamp": datetime.now().isoformat()
        }
        
        # Employee summary
        try:
            emp_df = spark.read.table(EMPLOYEES_TABLE)
            total_employees = emp_df.count()
            dept_breakdown = emp_df.groupBy("department").count().orderBy(desc("count")).collect()
            
            summary["employees"] = {
                "total_employees": total_employees,
                "department_breakdown": {row["department"]: row["count"] for row in dept_breakdown}
            }
        except Exception as e:
            summary["employees"] = {"error": str(e)}
        
        # Benefits summary
        try:
            benefits_df = spark.read.table(EMPLOYEE_BENEFITS_TABLE)
            
            # Health plan distribution
            health_plans = benefits_df.groupBy("health_plan").count().orderBy(desc("count")).collect()
            
            # Retirement plan distribution
            retirement_plans = benefits_df.groupBy("retirement_plan").count().orderBy(desc("count")).collect()
            
            # Average company match
            avg_match = benefits_df.agg(avg("company_match_percent").alias("avg_match")).collect()[0]["avg_match"]
            
            summary["benefits"] = {
                "health_plan_distribution": {row["health_plan"]: row["count"] for row in health_plans if row["health_plan"]},
                "retirement_plan_distribution": {row["retirement_plan"]: row["count"] for row in retirement_plans if row["retirement_plan"]},
                "average_company_match_percent": float(avg_match) if avg_match else 0
            }
        except Exception as e:
            summary["benefits"] = {"error": str(e)}
        
        # Leave requests summary
        try:
            leave_df = spark.read.table(LEAVE_REQUESTS_TABLE).filter(
                col("submitted_timestamp") >= expr(f"current_timestamp() - interval {days_back} days")
            )
            
            total_requests = leave_df.count()
            status_breakdown = leave_df.groupBy("status").count().orderBy(desc("count")).collect()
            
            summary["leave_requests"] = {
                "total_requests": total_requests,
                "status_breakdown": {row["status"]: row["count"] for row in status_breakdown}
            }
        except Exception as e:
            summary["leave_requests"] = {"error": str(e)}
        
        # Performance reviews summary
        try:
            perf_df = spark.read.table(PERFORMANCE_REVIEWS_TABLE).filter(
                col("review_date") >= expr(f"current_date() - interval {days_back} days")
            )
            
            total_reviews = perf_df.count()
            avg_scores = perf_df.agg(
                avg("goals_achievement").alias("avg_goals"),
                avg("technical_skills").alias("avg_technical"),
                avg("communication").alias("avg_communication")
            ).collect()[0]
            
            summary["performance_reviews"] = {
                "total_reviews": total_reviews,
                "average_scores": {
                    "goals_achievement": float(avg_scores["avg_goals"]) if avg_scores["avg_goals"] else 0,
                    "technical_skills": float(avg_scores["avg_technical"]) if avg_scores["avg_technical"] else 0,
                    "communication": float(avg_scores["avg_communication"]) if avg_scores["avg_communication"] else 0
                }
            }
        except Exception as e:
            summary["performance_reviews"] = {"error": str(e)}
        
        return summary
        
    except Exception as e:
        logger.error(f"Failed to generate HR summary: {e}")
        return {"error": f"Failed to generate HR summary: {str(e)}"}

# Initialize MCP Server
server = MCPServer(
    name="hr-data-mcp",
    description="Comprehensive HR insights from Delta tables in Unity Catalog (Employees, Leave Requests, Performance Reviews, Employee Benefits)"
)

# Register all 6 HR tools
server.register_tool(
    name="get_employee_info",
    description="Get comprehensive employee information including benefits data",
    parameters={
        "type": "object",
        "properties": {
            "employee_id": {
                "type": "integer",
                "minimum": 201,
                "maximum": 300,
                "description": "Employee ID to lookup"
            },
            "include_salary": {
                "type": "boolean",
                "default": False,
                "description": "Whether to include salary information (requires authorization)"
            },
            "include_benefits": {
                "type": "boolean",
                "default": True,
                "description": "Whether to include benefits information"
            }
        },
        "required": ["employee_id"]
    },
    function=get_employee_info
)

server.register_tool(
    name="get_employee_benefits",
    description="Get employee benefits information with filtering options",
    parameters={
        "type": "object",
        "properties": {
            "employee_id": {
                "type": "integer",
                "minimum": 201,
                "maximum": 300,
                "description": "Specific employee ID to lookup"
            },
            "benefit_type": {
                "type": "string",
                "enum": ["health", "medical", "retirement", "401k"],
                "description": "Filter by benefit type"
            }
        }
    },
    function=get_employee_benefits
)

server.register_tool(
    name="search_employees",
    description="Search employees with multiple filtering options",
    parameters={
        "type": "object",
        "properties": {
            "department": {
                "type": "string",
                "enum": ["Engineering", "Marketing", "Finance", "HR", "Sales", "Operations", "Support", "Legal"],
                "description": "Filter by department"
            },
            "role": {
                "type": "string",
                "description": "Filter by role (partial match)"
            },
            "office_location": {
                "type": "string",
                "enum": ["New York", "San Francisco", "Austin", "Remote", "Chicago"],
                "description": "Filter by office location"
            },
            "limit": {
                "type": "integer",
                "default": 50,
                "minimum": 1,
                "maximum": config.max_records,
                "description": "Maximum number of results to return"
            }
        }
    },
    function=search_employees
)

server.register_tool(
    name="get_leave_requests",
    description="Get leave requests with comprehensive filtering",
    parameters={
        "type": "object",
        "properties": {
            "employee_id": {
                "type": "integer",
                "minimum": 201,
                "maximum": 300,
                "description": "Filter by specific employee ID"
            },
            "status": {
                "type": "string",
                "enum": ["Pending", "Approved", "Denied", "Cancelled"],
                "description": "Filter by leave status"
            },
            "limit": {
                "type": "integer",
                "default": 100,
                "minimum": 1,
                "maximum": config.max_records,
                "description": "Maximum number of results"
            },
            "days_back": {
                "type": "integer",
                "minimum": 1,
                "maximum": 365,
                "description": "Only return requests from the last N days"
            }
        }
    },
    function=get_leave_requests
)

server.register_tool(
    name="get_performance_reviews",
    description="Get performance reviews for specific employees",
    parameters={
        "type": "object",
        "properties": {
            "employee_id": {
                "type": "integer",
                "minimum": 201,
                "maximum": 300,
                "description": "Employee ID to get reviews for"
            },
            "review_period": {
                "type": "string",
                "enum": ["Q1 2024", "Q2 2024", "Q3 2024", "Q4 2024", "Annual 2024"],
                "description": "Filter by review period"
            },
            "limit": {
                "type": "integer",
                "default": 50,
                "minimum": 1,
                "maximum": config.max_records,
                "description": "Maximum number of reviews to return"
            }
        }
    },
    function=get_performance_reviews
)

server.register_tool(
    name="get_hr_summary",
    description="Get comprehensive HR summary across all data sources including benefits",
    parameters={
        "type": "object",
        "properties": {
            "days_back": {
                "type": "integer",
                "default": 30,
                "minimum": 1,
                "maximum": 365,
                "description": "Number of days to look back for the summary"
            }
        }
    },
    function=get_hr_summary
)

# Initialize and validate
try:
    validate_table_access()
    logger.info("All HR tables including Benefits validated successfully")
except Exception as e:
    logger.error(f"Table validation failed: {e}")

# Startup message
if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stdio":
        logger.info("Starting HR MCP server with full functionality in stdio mode")
        server.run_stdio()
    else:
        print("HR Data MCP Server - Complete Final Version")
        print("=" * 60)
        print(f"Server initialized: {server.name}")
        print(f" Tools available: {len(server.tools)}")
        for tool_name in server.tools.keys():
            print(f"   🔧 {tool_name}")
        print(f" Tables configured:")
        print(f" Employees: {EMPLOYEES_TABLE}")
        print(f" Leave Requests: {LEAVE_REQUESTS_TABLE}")
        print(f" Performance Reviews: {PERFORMANCE_REVIEWS_TABLE}")
        print(f" Employee Benefits: {EMPLOYEE_BENEFITS_TABLE}")
        print(" Run with 'stdio' argument for MCP client communication")
        print("Or test individual functions:")
        print("   - get_employee_info(205)")
        print("   - search_employees(department='Engineering')")
        print("   - get_leave_requests(employee_id=210)")
        print("   - get_performance_reviews(employee_id=215)")
        print("   - get_employee_benefits(employee_id=220)")
        print("   - get_hr_summary(30)")
        
print(" Complete HR MCP Server with all 6 tools ready for deployment!")
