#!/usr/bin/env node

// Add AbortController polyfill for older Node.js versions
import AbortController from 'abort-controller';
global.AbortController = global.AbortController || AbortController;

/**
 * Metabase MCP Server
 * Implements interaction with Metabase API, providing the following features:
 * - Get dashboard list
 * - Get question list
 * - Get database list
 * - Execute question queries
 * - Get dashboard details
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  ListResourcesRequestSchema,
  ReadResourceRequestSchema,
  CallToolRequestSchema,
  ListResourcesResult,
  ReadResourceResult,
  ResourceSchema,
  ToolSchema
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import axios, { AxiosInstance } from "axios";

// SQL Documentation Structure
interface SQLDocSection {
  title: string;
  content: string;
  subsections?: Record<string, SQLDocSection>;
}

const SQL_DOCUMENTATION: Record<string, SQLDocSection> = {
  overview: {
    title: "SQL Documentation Overview",
    content: `This documentation provides comprehensive information about the appointments table and related tables in the Berry Street data warehouse. This documentation is designed to help analyze appointment data effectively.`,
    subsections: {
      introduction: {
        title: "Introduction",
        content: `The appointments table in the Data Warehouse (berry-street.berrystreet schema) stores information about client appointments with healthcare providers. These appointments represent scheduled sessions between clients and providers. The table includes details about appointment scheduling, status, duration, and various system metadata.

Location: berry-street.berrystreet.appointments

The table contains 52 columns covering various aspects of appointments, from basic scheduling information to integration details with video conferencing platforms.`
      }
    }
  },
  table_schema: {
    title: "Table Schema",
    content: `Detailed information about the appointments table schema and related tables.`,
    subsections: {
      key_fields: {
        title: "Key Fields",
        content: `Primary Identifiers:
- id: Unique identifier for the appointment (UUID format)
- healthie_id: Numeric identifier from the Healthie system
- client_id: Reference to the client/patient who scheduled the appointment
- provider_id: Reference to the healthcare provider who conducts the appointment
- appointment_type_id: Reference to the type of appointment`
      },
      date_fields: {
        title: "Date and Time Fields",
        content: `The table includes several datetime fields that track the appointment timeline:
- created_at: When the appointment record was created
- date: The scheduled date and time for the appointment
- original_date: Original string representation of the appointment date
- updated_at: When the appointment record was last updated
- healthie_created_at: When the appointment was created in the Healthie system`
      },
      status_fields: {
        title: "Status Fields",
        content: `- status: Current status categorized as "Occurred", "Rescheduled", "Cancelled", or "No-Show"
- internal_status: Internal tracking status
- healthie_is_deleted: Boolean indicating if the appointment is deleted in Healthie
- charting_note_approved: Boolean indicating if the appointment charting note has been approved`
      }
    }
  },
  date_handling: {
    title: "Date Handling",
    content: `Important Time Zone Information for Appointment Data`,
    subsections: {
      timezone_rules: {
        title: "Timezone Rules",
        content: `All date and time fields in the appointments table are stored in UTC. When analyzing data for specific calendar days, you must convert these timestamps to Eastern Time (EST/EDT) by subtracting 5 hours (or 4 hours during daylight saving time).`
      },
      conversion_examples: {
        title: "Conversion Examples",
        content: `Example SQL conversion:
\`\`\`sql
-- Convert UTC to Eastern Time
DATE(created_at - INTERVAL '5 hours') AS eastern_date
\`\`\``
      }
    }
  },
  common_patterns: {
    title: "Common Analysis Patterns",
    content: `Common patterns for analyzing appointment data`,
    subsections: {
      volume_analysis: {
        title: "Appointment Volume Analysis",
        content: `- Count bookings (using created_at) by day, week, month
- Count scheduled/occurred appointments (using date) by day, week, month
- Track trends in appointment bookings over time
- Analyze busy periods or seasonal patterns`
      },
      provider_analysis: {
        title: "Provider Analysis",
        content: `- Calculate provider utilization rates
- Track appointment hours per provider
- Analyze provider scheduling patterns
- Compare billing units versus actual time spent`
      }
    }
  },
  sample_queries: {
    title: "Sample SQL Queries",
    content: `Example SQL queries for common analysis patterns`,
    subsections: {
      insurance_analysis: {
        title: "Insurance vs. Non-Insurance Appointment Analysis",
        content: `\`\`\`sql
-- Compare insurance vs non-insurance appointments booked in a date range
WITH date_range_appointments AS (
    SELECT 
        a.client_id,
        DATE(a.created_at - INTERVAL '5 hours') AS booking_date_est,
        CASE 
            WHEN (h.group_name LIKE '%insurance%' OR h.group_name LIKE '%Insurance%') THEN 'Insurance'
            ELSE 'Non-Insurance'
        END AS payment_type
    FROM 
        "berry-street"."berrystreet"."appointments" a
    LEFT JOIN
        "berry-street"."berrystreet"."healthie_users" h ON a.client_id = h.id
    WHERE 
        DATE(a.created_at - INTERVAL '5 hours') >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT 
    booking_date_est,
    payment_type,
    COUNT(*) AS appointment_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(PARTITION BY booking_date_est), 2) AS daily_percentage
FROM 
    date_range_appointments
GROUP BY 
    booking_date_est, payment_type
ORDER BY 
    booking_date_est DESC, payment_type
\`\`\``
      },
      conversion_analysis: {
        title: "Initial to Follow-Up Conversion Rate",
        content: `\`\`\`sql
WITH patient_appointment_types AS (
    SELECT
        h.healthie_organization_id AS organization,
        a.client_id,
        CASE WHEN MIN(a.date) = a.date THEN 'Initial' ELSE 'Follow-Up' END AS visit_type,
        COUNT(*) AS appointment_count
    FROM
        "berry-street"."berrystreet"."appointments" a
    JOIN
        "berry-street"."berrystreet"."healthie_users" h ON a.client_id = h.id
    WHERE
        a.status = 'Occurred'
        AND DATE(a.date - INTERVAL '5 hours') >= CURRENT_DATE - INTERVAL '180 days'
    GROUP BY
        h.healthie_organization_id, a.client_id, CASE WHEN MIN(a.date) = a.date THEN 'Initial' ELSE 'Follow-Up' END
)
SELECT
    organization,
    COUNT(DISTINCT client_id) AS total_patients,
    SUM(CASE WHEN visit_type = 'Initial' THEN appointment_count ELSE 0 END) AS initial_appointments,
    SUM(CASE WHEN visit_type = 'Follow-Up' THEN appointment_count ELSE 0 END) AS followup_appointments,
    ROUND(100.0 * COUNT(DISTINCT CASE WHEN visit_type = 'Follow-Up' THEN client_id END) / 
          NULLIF(COUNT(DISTINCT CASE WHEN visit_type = 'Initial' THEN client_id END), 0), 2) AS conversion_rate
FROM
    patient_appointment_types
GROUP BY
    organization
ORDER BY
    conversion_rate DESC;
\`\`\``
      }
    }
  },
  analysis_examples: {
    title: "Analysis Examples",
    content: `Detailed examples of common analysis scenarios`,
    subsections: {
      billable_analysis: {
        title: "Billable Units vs Actual Time Analysis",
        content: `\`\`\`sql
SELECT
    EXTRACT(MONTH FROM a.date - INTERVAL '5 hours') AS month,
    EXTRACT(YEAR FROM a.date - INTERVAL '5 hours') AS year,
    h.healthie_organization_id AS organization,
    COUNT(*) AS appointment_count,
    SUM(a.actual_duration) AS total_actual_minutes,
    SUM(a.actual_duration_calculated) AS total_billable_minutes,
    ROUND(SUM(a.actual_duration_calculated) / 15) AS total_billable_units,
    ROUND(100.0 * (SUM(a.actual_duration_calculated) - SUM(a.actual_duration)) / SUM(a.actual_duration), 2) AS billing_efficiency_pct
FROM
    "berry-street"."berrystreet"."appointments" a
JOIN
    "berry-street"."berrystreet"."healthie_users" h ON a.provider_id = h.id
WHERE
    a.status = 'Occurred'
    AND DATE(a.date - INTERVAL '5 hours') >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY
    EXTRACT(MONTH FROM a.date - INTERVAL '5 hours'),
    EXTRACT(YEAR FROM a.date - INTERVAL '5 hours'),
    h.healthie_organization_id
ORDER BY
    year, month, organization;
\`\`\``
      }
    }
  }
};

// Custom error codes
enum ErrorCode {
  InternalError = "internal_error",
  InvalidRequest = "invalid_request",
  InvalidParams = "invalid_params",
  MethodNotFound = "method_not_found"
}

// Custom error class
class McpError extends Error {
  code: ErrorCode;
  
  constructor(code: ErrorCode, message: string) {
    super(message);
    this.code = code;
    this.name = "McpError";
  }
}

// Get Metabase configuration from environment variables
const METABASE_URL = process.env.METABASE_URL;
const METABASE_USERNAME = process.env.METABASE_USERNAME;
const METABASE_PASSWORD = process.env.METABASE_PASSWORD;

if (!METABASE_URL || !METABASE_USERNAME || !METABASE_PASSWORD) {
  throw new Error("METABASE_URL, METABASE_USERNAME, and METABASE_PASSWORD environment variables are required");
}

// Create custom Schema objects using z.object
const ListResourceTemplatesRequestSchema = z.object({
  method: z.literal("resources/list_templates")
});

const ListToolsRequestSchema = z.object({
  method: z.literal("tools/list")
});

// Add schema validation for SQL documentation
const SQLDocumentationSchema = z.object({
  title: z.string(),
  content: z.string(),
  subsections: z.record(z.object({
    title: z.string(),
    content: z.string(),
    subsections: z.record(z.object({
      title: z.string(),
      content: z.string()
    })).optional()
  })).optional()
});

// Add output format schema
const OutputFormatSchema = z.enum(['json', 'markdown']);

class MetabaseServer {
  private server: Server;
  private axiosInstance: AxiosInstance;
  private sessionToken: string | null = null;

  constructor() {
    this.server = new Server(
      {
        name: "metabase-server",
        version: "0.1.0",
      },
      {
        capabilities: {
          resources: {},
          tools: {},
        },
      }
    );

    this.axiosInstance = axios.create({
      baseURL: METABASE_URL,
      headers: {
        "Content-Type": "application/json",
      },
    });

    this.setupResourceHandlers();
    this.setupToolHandlers();
    
    // Enhanced error handling with logging
    this.server.onerror = (error: Error) => {
      this.logError('Server Error', error);
    };

    process.on('SIGINT', async () => {
      this.logInfo('Shutting down server...');
      await this.server.close();
      process.exit(0);
    });
  }

  // Add logging utilities
  private logInfo(message: string, data?: unknown) {
    const logMessage = {
      timestamp: new Date().toISOString(),
      level: 'info',
      message,
      data
    };
    console.error(JSON.stringify(logMessage));
    // MCP SDK changed, can't directly access session
    try {
      // Use current session if available
      console.error(`INFO: ${message}`);
    } catch (e) {
      // Ignore if session not available
    }
  }

  private logError(message: string, error: unknown) {
    const errorObj = error as Error;
    const apiError = error as { response?: { data?: { message?: string } }, message?: string };
    
    const logMessage = {
      timestamp: new Date().toISOString(),
      level: 'error',
      message,
      error: errorObj.message || 'Unknown error',
      stack: errorObj.stack
    };
    console.error(JSON.stringify(logMessage));
    // MCP SDK changed, can't directly access session
    try {
      console.error(`ERROR: ${message} - ${errorObj.message || 'Unknown error'}`);
    } catch (e) {
      // Ignore if session not available
    }
  }

  /**
   * Get Metabase session token
   */
  private async getSessionToken(): Promise<string> {
    if (this.sessionToken) {
      return this.sessionToken;
    }

    this.logInfo('Authenticating with Metabase...');
    try {
      const response = await this.axiosInstance.post('/api/session', {
        username: METABASE_USERNAME,
        password: METABASE_PASSWORD,
      });

      this.sessionToken = response.data.id;
      
      // Set default request headers
      this.axiosInstance.defaults.headers.common['X-Metabase-Session'] = this.sessionToken;
      
      this.logInfo('Successfully authenticated with Metabase');
      return this.sessionToken as string;
    } catch (error) {
      this.logError('Authentication failed', error);
      throw new McpError(
        ErrorCode.InternalError,
        'Failed to authenticate with Metabase'
      );
    }
  }

  /**
   * Set up resource handlers
   */
  private setupResourceHandlers() {
    this.server.setRequestHandler(ListResourcesRequestSchema, async (request) => {
      this.logInfo('Listing resources...', { requestStructure: JSON.stringify(request) });
      await this.getSessionToken();

      try {
        // Get dashboard list
        const dashboardsResponse = await this.axiosInstance.get('/api/dashboard');
        
        // Combine dashboards with documentation resources
        return {
          resources: [
            ...dashboardsResponse.data.map((dashboard: any) => ({
              uri: `metabase://dashboard/${dashboard.id}`,
              mimeType: "application/json",
              name: dashboard.name,
              description: `Metabase dashboard: ${dashboard.name}`
            })),
            // Add SQL documentation resources
            {
              uri: "metabase://docs/sql/overview",
              mimeType: "text/markdown",
              name: "SQL Documentation Overview",
              description: "Overview of the SQL documentation"
            },
            {
              uri: "metabase://docs/sql/table_schema",
              mimeType: "text/markdown",
              name: "Table Schema Documentation",
              description: "Documentation about table schemas"
            },
            {
              uri: "metabase://docs/sql/date_handling",
              mimeType: "text/markdown",
              name: "Date Handling Documentation",
              description: "Documentation about date and timezone handling"
            },
            {
              uri: "metabase://docs/sql/common_patterns",
              mimeType: "text/markdown",
              name: "Common Analysis Patterns",
              description: "Documentation about common SQL analysis patterns"
            },
            {
              uri: "metabase://docs/sql/sample_queries",
              mimeType: "text/markdown",
              name: "Sample SQL Queries",
              description: "Example SQL queries for common analyses"
            },
            {
              uri: "metabase://docs/sql/analysis_examples",
              mimeType: "text/markdown",
              name: "Analysis Examples",
              description: "Detailed examples of SQL analyses"
            }
          ]
        };
      } catch (error) {
        this.logError('Failed to list resources', error);
        throw new McpError(
          ErrorCode.InternalError,
          'Failed to list Metabase resources'
        );
      }
    });

    // Resource templates
    this.server.setRequestHandler(ListResourceTemplatesRequestSchema, async () => {
      return {
        resourceTemplates: [
          {
            uriTemplate: 'metabase://dashboard/{id}',
            name: 'Dashboard by ID',
            mimeType: 'application/json',
            description: 'Get a Metabase dashboard by its ID',
          },
          {
            uriTemplate: 'metabase://card/{id}',
            name: 'Card by ID',
            mimeType: 'application/json',
            description: 'Get a Metabase question/card by its ID',
          },
          {
            uriTemplate: 'metabase://database/{id}',
            name: 'Database by ID',
            mimeType: 'application/json',
            description: 'Get a Metabase database by its ID',
          },
          {
            uriTemplate: 'metabase://docs/sql/{section}',
            name: 'SQL Documentation Section',
            mimeType: 'text/markdown',
            description: 'Access SQL documentation sections',
          }
        ],
      };
    });

    // Read resources
    this.server.setRequestHandler(ReadResourceRequestSchema, async (request) => {
      this.logInfo('Reading resource...', { requestStructure: JSON.stringify(request) });
      await this.getSessionToken();

      const uri = request.params?.uri;
      let match;

      try {
        // Handle SQL documentation resources
        if ((match = uri.match(/^metabase:\/\/docs\/sql\/(\w+)$/))) {
          const section = match[1];
          const docSection = SQL_DOCUMENTATION[section];
          
          if (!docSection) {
            throw new McpError(
              ErrorCode.InvalidRequest,
              `Invalid SQL documentation section: ${section}`
            );
          }

          // Validate the documentation section
          const validatedSection = SQLDocumentationSchema.parse(docSection);
          
          // Convert to markdown
          let markdown = `# ${validatedSection.title}\n\n${validatedSection.content}\n\n`;
          
          if (validatedSection.subsections) {
            for (const [key, subsection] of Object.entries(validatedSection.subsections)) {
              markdown += `## ${subsection.title}\n\n${subsection.content}\n\n`;
            }
          }
          
          return {
            contents: [{
              uri: request.params?.uri,
              mimeType: "text/markdown",
              text: markdown
            }]
          };
        }
        
        // Handle dashboard resources
        if ((match = uri.match(/^metabase:\/\/dashboard\/(\d+)$/))) {
          const dashboardId = match[1];
          const response = await this.axiosInstance.get(`/api/dashboard/${dashboardId}`);
          
          return {
            contents: [{
              uri: request.params?.uri,
              mimeType: "application/json",
              text: JSON.stringify(response.data, null, 2)
            }]
          };
        }
        
        // Handle question/card resources
        else if ((match = uri.match(/^metabase:\/\/card\/(\d+)$/))) {
          const cardId = match[1];
          const response = await this.axiosInstance.get(`/api/card/${cardId}`);
          
          return {
            contents: [{
              uri: request.params?.uri,
              mimeType: "application/json",
              text: JSON.stringify(response.data, null, 2)
            }]
          };
        }
        
        // Handle database resources
        else if ((match = uri.match(/^metabase:\/\/database\/(\d+)$/))) {
          const databaseId = match[1];
          const response = await this.axiosInstance.get(`/api/database/${databaseId}`);
          
          return {
            contents: [{
              uri: request.params?.uri,
              mimeType: "application/json",
              text: JSON.stringify(response.data, null, 2)
            }]
          };
        }
        
        else {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `Invalid URI format: ${uri}`
          );
        }
      } catch (error) {
        if (error instanceof z.ZodError) {
          throw new McpError(
            ErrorCode.InvalidRequest,
            `Invalid documentation format: ${error.message}`
          );
        }
        if (axios.isAxiosError(error)) {
          throw new McpError(
            ErrorCode.InternalError,
            `Metabase API error: ${error.response?.data?.message || error.message}`
          );
        }
        throw error;
      }
    });
  }

  /**
   * Set up tool handlers
   */
  private setupToolHandlers() {
    this.server.setRequestHandler(ListToolsRequestSchema, async () => {
      return {
        tools: [
          {
            name: "list_dashboards",
            description: "List all dashboards in Metabase",
            inputSchema: {
              type: "object",
              properties: {}
            }
          },
          {
            name: "list_cards",
            description: "List all questions/cards in Metabase",
            inputSchema: {
              type: "object",
              properties: {}
            }
          },
          {
            name: "list_databases",
            description: "List all databases in Metabase",
            inputSchema: {
              type: "object",
              properties: {}
            }
          },
          {
            name: "execute_card",
            description: "Execute a Metabase question/card and get results",
            inputSchema: {
              type: "object",
              properties: {
                card_id: {
                  type: "number",
                  description: "ID of the card/question to execute"
                },
                parameters: {
                  type: "object",
                  description: "Optional parameters for the query"
                }
              },
              required: ["card_id"]
            }
          },
          {
            name: "get_dashboard_cards",
            description: "Get all cards in a dashboard",
            inputSchema: {
              type: "object",
              properties: {
                dashboard_id: {
                  type: "number",
                  description: "ID of the dashboard"
                }
              },
              required: ["dashboard_id"]
            }
          },
          {
            name: "execute_query",
            description: "Execute a SQL query against a Metabase database",
            inputSchema: {
              type: "object",
              properties: {
                database_id: {
                  type: "number",
                  description: "ID of the database to query"
                },
                query: {
                  type: "string",
                  description: "SQL query to execute"
                },
                native_parameters: {
                  type: "array",
                  description: "Optional parameters for the query",
                  items: {
                    type: "object"
                  }
                }
              },
              required: ["database_id", "query"]
            }
          },
          {
            name: "convert_timezone",
            description: "Convert UTC timestamps to Eastern Time for appointment analysis",
            inputSchema: {
              type: "object",
              properties: {
                utc_timestamp: {
                  type: "string",
                  description: "UTC timestamp to convert"
                },
                output_format: {
                  type: "string",
                  enum: ["date", "datetime"],
                  description: "Output format (date or datetime)"
                }
              },
              required: ["utc_timestamp"]
            }
          },
          {
            name: "search_sql_docs",
            description: "Search through the SQL documentation",
            inputSchema: {
              type: "object",
              properties: {
                query: {
                  type: "string",
                  description: "Search query"
                },
                section: {
                  type: "string",
                  description: "Optional section to search in"
                },
                output_format: {
                  type: "string",
                  enum: ["json", "markdown"],
                  description: "Output format preference"
                }
              },
              required: ["query"]
            }
          }
        ]
      };
    });

    this.server.setRequestHandler(CallToolRequestSchema, async (request) => {
      this.logInfo('Calling tool...', { requestStructure: JSON.stringify(request) });
      await this.getSessionToken();

      try {
        switch (request.params?.name) {
          case "list_dashboards": {
            const response = await this.axiosInstance.get('/api/dashboard');
            return {
              content: [{
                type: "text",
                text: JSON.stringify(response.data, null, 2)
              }]
            };
          }

          case "list_cards": {
            const response = await this.axiosInstance.get('/api/card');
            return {
              content: [{
                type: "text",
                text: JSON.stringify(response.data, null, 2)
              }]
            };
          }

          case "list_databases": {
            const response = await this.axiosInstance.get('/api/database');
            return {
              content: [{
                type: "text",
                text: JSON.stringify(response.data, null, 2)
              }]
            };
          }

          case "execute_card": {
            const cardId = request.params?.arguments?.card_id;
            if (!cardId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Card ID is required"
              );
            }

            const parameters = request.params?.arguments?.parameters || {};
            const response = await this.axiosInstance.post(`/api/card/${cardId}/query`, { parameters });
            
            return {
              content: [{
                type: "text",
                text: JSON.stringify(response.data, null, 2)
              }]
            };
          }

          case "get_dashboard_cards": {
            const dashboardId = request.params?.arguments?.dashboard_id;
            if (!dashboardId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Dashboard ID is required"
              );
            }

            const response = await this.axiosInstance.get(`/api/dashboard/${dashboardId}`);
            
            return {
              content: [{
                type: "text",
                text: JSON.stringify(response.data.cards, null, 2)
              }]
            };
          }
          
          case "execute_query": {
            const databaseId = request.params?.arguments?.database_id;
            const query = request.params?.arguments?.query;
            const nativeParameters = request.params?.arguments?.native_parameters || [];
            
            if (!databaseId) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Database ID is required"
              );
            }
            
            if (!query) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "SQL query is required"
              );
            }
            
            // Build query request body
            const queryData = {
              type: "native",
              native: {
                query: query,
                template_tags: {}
              },
              parameters: nativeParameters,
              database: databaseId
            };
            
            const response = await this.axiosInstance.post('/api/dataset', queryData);
            
            return {
              content: [{
                type: "text",
                text: JSON.stringify(response.data, null, 2)
              }]
            };
          }
          
          case "convert_timezone": {
            const timestamp = request.params?.arguments?.utc_timestamp as string;
            const format = (request.params?.arguments?.output_format as string) || "datetime";
            
            if (!timestamp) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "UTC timestamp is required"
              );
            }

            try {
              const utcDate = new Date(timestamp);
              // Subtract 5 hours for EST
              const estDate = new Date(utcDate.getTime() - (5 * 60 * 60 * 1000));
              
              const result = format === "date" 
                ? estDate.toISOString().split('T')[0]
                : estDate.toISOString();

              return {
                content: [{
                  type: "text",
                  text: JSON.stringify({
                    utc: timestamp,
                    eastern: result
                  }, null, 2)
                }]
              };
            } catch (error) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Invalid timestamp format"
              );
            }
          }

          case "search_sql_docs": {
            const query = (request.params?.arguments?.query as string)?.toLowerCase();
            const section = request.params?.arguments?.section as string;
            const outputFormat = (request.params?.arguments?.output_format as string) || "json";
            
            if (!query) {
              throw new McpError(
                ErrorCode.InvalidParams,
                "Search query is required"
              );
            }

            const results: Array<{
              section: string;
              title: string;
              content: string;
              matches: string[];
            }> = [];

            const searchSection = (
              sectionName: string,
              docSection: SQLDocSection,
              parentTitle?: string
            ) => {
              const fullTitle = parentTitle 
                ? `${parentTitle} > ${docSection.title}`
                : docSection.title;

              if (docSection.content.toLowerCase().includes(query)) {
                results.push({
                  section: sectionName,
                  title: fullTitle,
                  content: docSection.content,
                  matches: [docSection.content]
                });
              }

              if (docSection.subsections) {
                for (const [key, subsection] of Object.entries(docSection.subsections)) {
                  if (subsection.content.toLowerCase().includes(query)) {
                    results.push({
                      section: `${sectionName}.${key}`,
                      title: `${fullTitle} > ${subsection.title}`,
                      content: subsection.content,
                      matches: [subsection.content]
                    });
                  }
                }
              }
            };

            if (section && section in SQL_DOCUMENTATION) {
              const docSection = SQL_DOCUMENTATION[section];
              searchSection(section, docSection);
            } else if (!section) {
              for (const [key, docSection] of Object.entries(SQL_DOCUMENTATION)) {
                searchSection(key, docSection);
              }
            } else {
              throw new McpError(
                ErrorCode.InvalidParams,
                `Invalid section: ${section}`
              );
            }

            if (outputFormat === 'markdown') {
              let markdown = `# Search Results for "${query}"\n\n`;
              results.forEach(result => {
                markdown += `## ${result.title}\n\n`;
                markdown += `${result.content}\n\n`;
              });

              return {
                content: [{
                  type: "text",
                  text: markdown
                }]
              };
            }

            return {
              content: [{
                type: "text",
                text: JSON.stringify(results, null, 2)
              }]
            };
          }
          
          default:
            return {
              content: [
                {
                  type: "text",
                  text: `Unknown tool: ${request.params?.name}`
                }
              ],
              isError: true
            };
        }
      } catch (error) {
        if (axios.isAxiosError(error)) {
          return {
            content: [{
              type: "text",
              text: `Metabase API error: ${error.response?.data?.message || error.message}`
            }],
            isError: true
          };
        }
        throw error;
      }
    });
  }

  async run() {
    try {
      this.logInfo('Starting Metabase MCP server...');
      const transport = new StdioServerTransport();
      await this.server.connect(transport);
      this.logInfo('Metabase MCP server running on stdio');
    } catch (error) {
      this.logError('Failed to start server', error);
      throw error;
    }
  }
}

// Add global error handlers
process.on('uncaughtException', (error: Error) => {
  console.error(JSON.stringify({
    timestamp: new Date().toISOString(),
    level: 'fatal',
    message: 'Uncaught Exception',
    error: error.message,
    stack: error.stack
  }));
  process.exit(1);
});

process.on('unhandledRejection', (reason: unknown, promise: Promise<unknown>) => {
  const errorMessage = reason instanceof Error ? reason.message : String(reason);
  console.error(JSON.stringify({
    timestamp: new Date().toISOString(),
    level: 'fatal',
    message: 'Unhandled Rejection',
    error: errorMessage
  }));
});

const server = new MetabaseServer();
server.run().catch(console.error);
