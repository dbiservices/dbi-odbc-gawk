// minimalist gawk interface to ODBC;
// this extension turns gawk into an effective ODBC client program; 
// vim: hi MatchParen ctermfg=208 ctermbg=bg
// cec@dbi-services, September 2021;
//
// gawk-specific stuff:
// returned successful connection and cursor (i.e. statement) handles are non-negative integer numbers; -1 means an error occurred;
// based on example code here:
//    https://docs.microsoft.com/en-us/sql/connect/odbc/cpp-code-example-app-connect-access-sql-db?view=sql-server-ver15;
//    https://github.com/Azure/unixODBC-MSSQL/blob/master/exe/isql.h;
// to debug, odbc tracing can be enabled by adding the following lines in /etc/odbcinst.ini as root:
//[ODBC]
//Trace=Yes
//TraceFile=/dev/stdout
//TraceOptions=3

#define SQL_WCHART_CONVERT

#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <wchar.h>
#include <sys/stat.h>
#include <sql.h>
#include <sqltypes.h>
#include <sqlext.h>
#include "gawkapi.h"

unsigned short int bDebug = 0;

// gawk interfacing stuff;
static const gawk_api_t *api;   /* for convenience macros to work */
static awk_ext_id_t ext_id;
static const char *ext_version = "an interface to ODBC: version 1.0";
int plugin_is_GPL_compatible;

// ODBC environment's global handle;
static SQLHENV hEnv = NULL;

// maximum size of a column header in bytes;
static const unsigned int DISPLAY_MAX = 1000;

// internally used to convert a column's index number to its string representation;
// there can be 10**(LOG_MAX_COLUMNS - 1) columns, should be plenty enough...;
static const unsigned int LOG_MAX_COLUMNS = 6;

// gawk default column separator;
// used when returning a list of column headers or values into a string;
static const WCHAR SUBSEP = '\x01c';

/*
structure to store information about a column;
internally used by cursors and partially exposed by get_col_array();
*/
typedef struct STR_BINDING {
    SQLSMALLINT         cDisplaySize;           // size of the displayed data, got from the dictionary, large enough to hold the column header;
    WCHAR               *wszBuffer;             // display buffer;
    SQLLEN              indPtr;                 // effective size of the fetched column data, or null;
    BOOL                fChar;                  // is it a character col (vs. a numeric one) ?
    WCHAR               *pColName;              // column name;
    struct STR_BINDING  *sNext;                 // linked list;
} BINDING;

// forward references;
static void say(const char *text);
static int get_odbc_connection_handle(void);
static int get_odbc_cursor_handle(void);
static awk_value_t *get_column_headers_str(int, awk_value_t *, struct awk_ext_func *);
static awk_value_t *get_column_headers_array(int, awk_value_t *, struct awk_ext_func *);
static void get_col_headers(unsigned int hCursor);
static void close_cursor(unsigned int hCursor);
static void disconnect(unsigned int hConnection);

static void HandleDiagnosticRecord (SQLHANDLE,
                                    SQLSMALLINT,
                                    RETCODE);
static void AllocateBindings(HSTMT,
                             SQLSMALLINT,
                             BINDING**,
                             unsigned int *);

/*
macro to call ODBC functions and report an error on failure;
takes handle, handle type, and stmt;
don't forget the Exit: and error processing code label at the end of the caller function (much like a catch()/except() exception handler statement);
*/
#define TRYODBC(h, ht, x)   {   RETCODE rc = (x);\
                                if (rc != SQL_SUCCESS) \
                                { \
                                    HandleDiagnosticRecord (h, ht, rc); \
                                } \
                                if (rc == SQL_ERROR) \
                                { \
                                    fprintf(stderr, "Error %d in %s\n", rc, __func__); \
                                    goto Exit;  \
                                }  \
                            }

// internal odbc interface stuff;
// opaque types ...;

// internally stores the handles for connections and statements (i.e. cursors) in array;
// the 0-based integer returned as a gawk's handle is the index into that array;
// that many connections are allowed;
#define MAX_ODBC_CONNECTIONS 100
static unsigned nb_odbc_connection_free_handles = MAX_ODBC_CONNECTIONS;
static SQLHDBC odbc_connection_handles[MAX_ODBC_CONNECTIONS];

// ditto for cursors;
// cursors are many-to-one associated to connections;
#define MAX_ODBC_CURSORS 100
static unsigned nb_odbc_cursor_free_handles = MAX_ODBC_CURSORS;
struct CONN_STMT_HANDLE {
   SQLHDBC  hDbc;
   SQLHSTMT hStmt;
   BINDING  *pBinding;
   unsigned int nb_cols;
   unsigned int header_length;     // total length of the column headers, without the separators;
   unsigned int row_length;        // total length of a data row, no separators, character (i.e. displayable) representation of columns;
} CONN_STMT_HANDLE;
struct CONN_STMT_HANDLE odbc_cursor_handles[MAX_ODBC_CURSORS];

// set to 1 after init_odbc() has been called, i.e. mainly after hEnv has been initialized, which happens automatically when the interface extension is loaded;
static unsigned short int bODBC_initialized = 0;

/*
ODBC_connect()
opens an odbc connection with parameters connection string, user name and password;
Returns a non-negative number if successful, -1 it not;
Usage from gawk:
   connection_string = "mymssqlserverdb" 
   hConnection = ODBC_connect(connection_string, user_name, password)
   if (-1 == hConnection) {
      printf("cannot connect using connection string %s\n", connection_string)
   else
      printf("received connection handle is %d\n", hConnection)
*/
awk_value_t *ODBC_connect(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t connectionString;
   awk_value_t user_name;
   awk_value_t password;
   int hConnection;

   assert(result != NULL);

   if (get_argument(0, AWK_STRING, &connectionString)) {
      hConnection = get_odbc_connection_handle();
      if (-1 == hConnection) {
         fprintf(stderr, "cannot open new connection to [%s]\n", connectionString.str_value.str); 
         return make_number(-1, result);
      }
      if (!get_argument(1, AWK_STRING, &user_name)) {
         fprintf(stderr, "missing user name parameter\n");
         return make_number(-1, result);
      }
      if (!get_argument(2, AWK_STRING, &password)) {
         fprintf(stderr, "missing password parameter\n");
         return make_number(-1, result);
      }
   }
   else {
      fprintf(stderr, "missing connection string parameter\n"); 
      return make_number(-1, result);
   }

   // Allocate a connection;
   SQLHDBC     hDbc = NULL;
   TRYODBC(hEnv,
           SQL_HANDLE_ENV,
           SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc));
   odbc_connection_handles[hConnection] = hDbc;

   TRYODBC(hDbc,
           SQL_HANDLE_DBC,
           SQLConnect(hDbc,
                      (SQLCHAR *) connectionString.str_value.str,
                      strlen(connectionString.str_value.str),
                      (SQLCHAR *) user_name.str_value.str,
                      strlen(user_name.str_value.str),
                      (SQLCHAR *) password.str_value.str,
                      strlen(password.str_value.str)
                     )
          );

    return make_number(hConnection, result);
Exit:
   return make_number(-1, result);
}

/*
ODBC_cursor()
allocates a cursor;
cursors are needed to execute an SQL statement (see ODBC_execute() below);
Returns a non-negative number if successful, -1 it not;
Usage from gawk:
   hCursor = ODBC_cursor(hConnection)
   if (-1 == hCursor) {
      printf("cannot connect get cursor for connection %d\n", hConnection)
   else
      print("received cursor: %d for connection %d\n", hCursor, hConnection)
*/
awk_value_t *ODBC_cursor(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hConnection;
   int hCursor;
   unsigned int connection_index;

   assert(result != NULL);

   if (get_argument(0, AWK_NUMBER, &hConnection)) {
      connection_index = hConnection.num_value;
      hCursor = get_odbc_cursor_handle();
      if (-1 == hCursor) {
         fprintf(stderr, "cannot get a new cursor for connection handle [%-d]\n", connection_index); 
         return make_number(-1, result);
      }
   }
   else {
      fprintf(stderr, "missing connection handle parameter\n"); 
      return make_number(-1, result);
   }

   // get a statement handle;
   SQLHSTMT hStmt = NULL;
   TRYODBC(odbc_connection_handles[connection_index],
           SQL_HANDLE_DBC,
           SQLAllocHandle(SQL_HANDLE_STMT,
                          odbc_connection_handles[connection_index],
                          &hStmt
                         )
          );
   odbc_cursor_handles[hCursor] = ((struct CONN_STMT_HANDLE) {odbc_connection_handles[connection_index], hStmt, NULL, 0, 0, 0});

   // try this; if it fails, ignore the error; it means this feature is not supported by the ODBC driver or the data source;
   // but watch out for possible subsequent errors;
   SQLSetStmtAttr(hStmt,
                  SQL_ATTR_CURSOR_SCROLLABLE,
                  SQL_SCROLLABLE,
                  0
                 );
   return make_number(hCursor, result);
Exit:
   return make_number(-1, result);
}

/*
ODBC_execute()
executes the given statement by the given cursor;
Returns 0 if OK, -1 if not;
Usage from gawk:
   statementStr = "SELECT c.country_name, c.country_id, l.country_id, l.street_address, l.city FROM countries c LEFT JOIN locations l ON l.country_id = c.country_id WHERE c.country_id IN ('US', 'UK', 'CN')"
   status = ODBC_execute(hCursor, statementStr)
   print("ODBC_execute status for statement [%s]: %s\n", statementStr, status)
*/
awk_value_t *ODBC_execute(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hCursor;
   awk_value_t statementStr;
   unsigned int cursor_index;

   assert(result != NULL);

   if (!get_argument(0, AWK_NUMBER, &hCursor)) {
      fprintf(stderr, "missing cursor handle\n"); 
      return make_number(-1, result);
   }
   else if (!get_argument(1, AWK_STRING, &statementStr)) {
      fprintf(stderr, "missing statement string for cursor with handle [%-.0g]\n", hCursor.num_value); 
      return make_number(-1, result);
   }
   cursor_index = hCursor.num_value;

   RETCODE     retCode;
   SQLSMALLINT cCols = 0;

   // execute the query;
   retCode = SQLExecDirect(odbc_cursor_handles[cursor_index].hStmt,
                           (SQLCHAR *) statementStr.str_value.str,
                           strlen(statementStr.str_value.str)
                          );

   switch(retCode) {
      case SQL_SUCCESS_WITH_INFO: {
         HandleDiagnosticRecord(odbc_cursor_handles[cursor_index].hStmt,
                                SQL_HANDLE_STMT,
                                retCode);
         break;
      }
      case SQL_SUCCESS: {
         TRYODBC(odbc_cursor_handles[cursor_index].hStmt,
                 SQL_HANDLE_STMT,
                 SQLNumResultCols(odbc_cursor_handles[cursor_index].hStmt,
                                  &cCols
                                 )
                );

         // if this is a row-returning query, allocate the column buffers and get the column headers;
         if (cCols > 0) {
            // allocate the column buffers for the result set;
            AllocateBindings(odbc_cursor_handles[cursor_index].hStmt,
                             cCols,
                             &odbc_cursor_handles[cursor_index].pBinding,
                             &odbc_cursor_handles[cursor_index].row_length);

            //get_col_headers(cursor_index, sNumResults);
            get_col_headers(cursor_index);
         }
         else {
            // cCols is actually the number of columns in the result set;
            // it is 0 for non SELECT statements, which are processed here;
            // SELECT statements' result set is accessed later using ODBC_fetch();
            SQLLEN cRowCount;
 
            TRYODBC(odbc_cursor_handles[cursor_index].hStmt,
                    SQL_HANDLE_STMT,
                    SQLRowCount(odbc_cursor_handles[cursor_index].hStmt,&cRowCount));
 
            if (cRowCount >= 0) {
               fprintf(stderr, "%ld %s affected\n",
                       cRowCount,
                       cRowCount == 1 ? "row" : "rows");
            }
         }
         break;
      }

      case SQL_ERROR: {
         HandleDiagnosticRecord(odbc_cursor_handles[cursor_index].hStmt, SQL_HANDLE_STMT, retCode);
         return make_number(-1, result);
      }

      default: {
         fprintf(stderr, "Unexpected return code %hd !\n", retCode);
         return make_number(-1, result);
      }
   }
   return make_number(cCols, result);
Exit:
   return make_number(-1, result);
}

/*
ODBC_get_column_headers()
dispatches the call to get_column_headers_str() if only a cursor as parameter, or to get_column_headers_array() if an array is received as the second parameter;
see those functions for usage;
*/
awk_value_t *ODBC_get_column_headers(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   assert(result != NULL);
   if (1 == nargs)
      return get_column_headers_str(nargs, result, unused);
   else
      return get_column_headers_array(nargs, result, unused);
}

/*
ODBC_fetch()
returns one row from the result set pointed to by hCursor, -1 if error;
columns are SUBSEP-separated (i.e. \x1c character) values, to be separated by the caller;
Usage from gawk:
   row_count = 0
   while(row = ODBC_fetch(hCursor) > -1) {
      row_count++
      nb_columns = split(row, cols, SUBSEP)
      printf("%d: ", row_count)
      for (i = 1; i <= nb_columns; i++)
         printf("%s%s", i > 1 ? "  " : "", cols[i])
      printf "\n"
   }
if a second parameter is present, the result set will be returned into an array with the following structure:
row[0] = nb_columns
row[i][col_name] = value
with 1 <= i <= nb_columns;
the array can be iterated in gawk as follows:
while(ODBC_fetch:(hCursor, row) > -1) {
   nb_columns = row_data[0]
   for (i = 1; i <= nb_columns; i++)
      for (col_name in row_data[i])
         print row_data[i][col_name]
}
this structure looks a bit odd because we had to enforce ordering using a numeric index as gawk returns the
values in an unpredictable order when iterating through an associative array with the for (index in array) statement;
we wish the chronological order of insertion were preserved...
*/
awk_value_t *ODBC_fetch(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hCursor;
   unsigned int cursor_index;

   assert(result != NULL);

   if (!get_argument(0, AWK_NUMBER, &hCursor)) {
      fprintf(stderr, "missing cursor handle parameter\n"); 
      return make_number(-1, result);
   }
   cursor_index = hCursor.num_value;

   awk_value_t array_param;
   awk_value_t value;
   awk_value_t index;
   awk_array_t col_array;

   if (2 == nargs)
      if (!get_argument(1, AWK_ARRAY, &array_param)) {
         fprintf(stderr, "could not access the array parameter\n"); 
         return make_number(-1, result);
      }
      else {
         col_array = array_param.array_cookie;
         clear_array(col_array);

         make_number(0, &index);
         make_number(odbc_cursor_handles[cursor_index].nb_cols, &value);
         if (!set_array_element(col_array, &index, &value)) {
            fprintf(stderr, "error in ODBC_fetch: set_array_element failed with index %d and value %d, expected [%d]\n",
                           index.num_value, value.num_value, odbc_cursor_handles[cursor_index].nb_cols);
            return make_number(-1, result);
         }
      }

   BINDING *pThisBinding;
   RETCODE retCode = SQL_SUCCESS;
   
   // fetch and return one row of data at each call;
   TRYODBC(odbc_cursor_handles[cursor_index].hStmt,
           SQL_HANDLE_STMT,
           retCode = SQLFetch(odbc_cursor_handles[cursor_index].hStmt
                             )
          );
   if (retCode != SQL_NO_DATA_FOUND) {
      // find the total length of the row' s data, excluding the separators;
      odbc_cursor_handles[cursor_index].row_length = 0;
      for (pThisBinding = odbc_cursor_handles[cursor_index].pBinding; pThisBinding; pThisBinding = pThisBinding->sNext)
         odbc_cursor_handles[cursor_index].row_length += pThisBinding -> indPtr;
      char output[odbc_cursor_handles[cursor_index].row_length + odbc_cursor_handles[cursor_index].nb_cols];
      output[0] = '\0';
      unsigned int iCol = 0;
      for (pThisBinding = odbc_cursor_handles[cursor_index].pBinding; pThisBinding; pThisBinding = pThisBinding->sNext) {
         iCol++;
         char temp[pThisBinding -> indPtr > 0 ? pThisBinding -> indPtr + 1 : 1]; temp[0] = '\0';
         if (pThisBinding != odbc_cursor_handles[cursor_index].pBinding) {
            sprintf(temp, "%c", SUBSEP);
            strcat(output, temp);
         }
         sprintf(temp, "%s", pThisBinding -> indPtr > 0 ? (char *) pThisBinding -> wszBuffer : "");
         strcat(output, temp);

         if (2 == nargs) {
            // populate the array_name array;
            awk_array_t subarray_names = create_array();
            value.val_type = AWK_ARRAY;
            value.array_cookie = subarray_names;

            clear_array(subarray_names);
            // !!! numerical array index must be converted to string or set_array_element will crash during memory allocation !!!!
            // allocate enough space for 10^LOG_MAX_COLUMNS columns;
            char str_index[LOG_MAX_COLUMNS + 1];
            unsigned int index_len = sprintf(str_index, "%d", iCol);
            make_const_string(str_index, index_len, &index);
            if (!set_array_element(col_array, &index, &value)) {
               fprintf(stderr, "error in ODBC_fetch: set_array_element for col_names failed\n");
               return make_number(-1, result);
            }
            subarray_names = value.array_cookie;
            make_const_string(pThisBinding -> pColName, strlen(pThisBinding -> pColName), &index);
            make_const_string(temp, strlen(temp), &value);
            if (!set_array_element(subarray_names, &index, &value)) {
               fprintf(stderr, "error in ODBC_fetch:: set_array_element failed in subarray_names with index %d and value %s, expected value %s\n",
                              index.num_value, value.str_value.str, pThisBinding -> pColName);
               return make_number(-1, result);
            }
         }
      }
      return make_const_string(output, strlen(output), result);
   }
   else
      return make_const_string("", 0, result);
Exit:
   return make_number(-1, result);
}

awk_value_t *ODBC_rewind(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hCursor;
   unsigned int cursor_index;

   assert(result != NULL);

   if (get_argument(0, AWK_NUMBER, &hCursor))
      cursor_index = hCursor.num_value;
   else {
      fprintf(stderr, "missing cursor parameter\n");
      return make_number(-1, result);
   }

   TRYODBC(odbc_cursor_handles[cursor_index].hStmt,
           SQL_HANDLE_STMT,
           SQLFetchScroll(odbc_cursor_handles[cursor_index].hStmt,
                          SQL_FETCH_FIRST,
                          0
                         )
          );
   TRYODBC(odbc_cursor_handles[cursor_index].hStmt,
           SQL_HANDLE_STMT,
           SQLFetchScroll(odbc_cursor_handles[cursor_index].hStmt,
                          SQL_FETCH_PRIOR,
                          0
                         )
          );
   return make_number(1, result);
Exit:
   return make_number(-1, result);
}

/*
ODBC_close_cursor()
closes the cursor with given handle;
returns 0 if OK, -1 otherwise;
Usage from gawk:
   if (-1 == ODBC_close_cursor(hCursor))
      printf("error while closing cursor %d\n", hCursor)
   else
      print("cursor %d closed: %s\n", hCursor)
*/
awk_value_t *ODBC_close_cursor(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hCursor;

   assert(result != NULL);

   if (!get_argument(0, AWK_NUMBER, &hCursor)) {
      fprintf(stderr, "missing cursor handle parameter\n"); 
      return make_number(-1, result);
   }

   close_cursor(hCursor.num_value);
   return make_number(0, result);
}

/*
ODBC_disconnect()
closes the connection with given handle;
returns 0 if OK, -1 otherwise;
Usage from gawk:
   if (-1 == ODBC_close_connection(hConnection))
      printf("error while disconnecting connection %d\n", hConnection)
   else
      print("connection %d disconnected\n", hConnection)
*/
awk_value_t *ODBC_disconnect(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hConnection;

   assert(result != NULL);

   if (!get_argument(0, AWK_STRING, &hConnection)) {
      fprintf(stderr, "missing connection handle parameter\n"); 
      return make_number(-1, result);
   }

   disconnect(hConnection.num_value);
   //printf("Disconnected from ODBC connection [%d]\n", (int) hConnection.num_value);
   return make_number(0, result);
}

// -----------------------------------------------------------------------------------------------
// ODBC support functions;

/*
init_odbc()
internal function;
initializes the odbc internal state, the odbc_connection_handles and odbc_cursor_handles arrays to null pointers and resets their respective counters;
automatically called at program start time;
returns awk_true if successfull, awk_false otherwise;
as an explicit call of init_odbc() crashes the program, it looks like gawk does not support this;
to prevent accidental explicit calls, the function has been removed from the symbol table;
if needed, a static awk_bool_t odbc_reinit() function could be implemented to freshen up the odbc environment;
*/
static awk_bool_t init_odbc() { //int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   if (bODBC_initialized) {
      // the odbc interface was already used: reset it;
      for (unsigned int i = 0; i < MAX_ODBC_CURSORS; i++)
         if (odbc_cursor_handles[i].hStmt)
            close_cursor(i);
      for (unsigned int i = 0; i < MAX_ODBC_CONNECTIONS; i++)
         if (odbc_connection_handles[i])
            disconnect(i);
      TRYODBC(hEnv,
              SQL_HANDLE_ENV,
              SQLFreeHandle(SQL_HANDLE_ENV, hEnv));
   }

   // allocate an environment;
   if (SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv) == SQL_ERROR) {
      fprintf(stderr, "Unable to allocate an environment handle\n");
      return awk_false;
   }

   // Register this as an application that expects 3.x behavior,
   // you must register something if you use AllocHandle
   TRYODBC(hEnv,
           SQL_HANDLE_ENV,
           SQLSetEnvAttr(hEnv,
                         SQL_ATTR_ODBC_VERSION,
                         (SQLPOINTER) SQL_OV_ODBC3,
                         0
                        )
          );
   for (unsigned i = 0; i < MAX_ODBC_CONNECTIONS; i++)
      odbc_connection_handles[i] = (SQLHDBC) NULL;
   nb_odbc_connection_free_handles = MAX_ODBC_CONNECTIONS;

   for (unsigned i = 0; i < MAX_ODBC_CURSORS; i++)
      odbc_cursor_handles[i] = ((struct CONN_STMT_HANDLE) {NULL, NULL, NULL, 0, 0, 0});
   nb_odbc_cursor_free_handles = MAX_ODBC_CURSORS;

   register_ext_version(ext_version);

   bODBC_initialized = 1;
   return awk_true;
Exit:
   return awk_false;
}

/*
get_odbc_connection_handle()
looks for a free slot in the odbc connection handle array;
returns its 0-based index if found, -1 otherwise;
*/
static int get_odbc_connection_handle(void) {
   if (0 == nb_odbc_connection_free_handles) {
      fprintf(stderr, "maximum of open connections [%d] reached, no free handles !\n", MAX_ODBC_CONNECTIONS);
      return -1;
   }
   for (unsigned int i = 0; i < MAX_ODBC_CONNECTIONS; i++)
      if ((SQLHDBC *) NULL == odbc_connection_handles[i]) {
         nb_odbc_connection_free_handles--;
         return i;
      }
   // should never come that far;
   return -1;
}

/*
get_odbc_cursor_handle()
looks for a free slot in the odbc cursor handle array;
returns its 0-based index if found, -1 otherwise;
*/
static int get_odbc_cursor_handle(void) {
   if (0 == nb_odbc_cursor_free_handles) {
      fprintf(stderr, "maximum of open cursors [%d] reached, no free handles !\n", MAX_ODBC_CURSORS);
      return -1;
   }
   for (unsigned i = 0; i < MAX_ODBC_CURSORS; i++)
      if (NULL == odbc_cursor_handles[i].hStmt) {
         nb_odbc_cursor_free_handles--;
         return i;
      }
   // should never come that far;
   return -1;
}

/*
get_column_headers_str()
returns the column headers as a string of SUBSEP-separated fields to be separated by the caller;
Usage from gawk:
   headers_str = get_column_headers_str(hCursor)
   nb_columns = split(headers_str, headers, SUBSEP)
   for (i = 1; i <= nb_columns; i++)
      printf("col %d: %s\n", i, headers[i])
   printf "\n"
*/
awk_value_t *get_column_headers_str(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hCursor;
   unsigned int cursor_index;

   assert(result != NULL);

   if (!get_argument(0, AWK_NUMBER, &hCursor)) {
      fprintf(stderr, "missing cursor handle\n"); 
      return make_number(-1, result);
   }
   cursor_index = hCursor.num_value;

   char output[odbc_cursor_handles[cursor_index].header_length + odbc_cursor_handles[cursor_index].nb_cols];
   output[0] = '\0';
   for (BINDING *pBinding = odbc_cursor_handles[cursor_index].pBinding; pBinding; pBinding = pBinding->sNext) {
      char temp[DISPLAY_MAX];
      if (pBinding != odbc_cursor_handles[cursor_index].pBinding) {
         sprintf(temp, "%c", SUBSEP);
         strcat(output, temp);
      }
      sprintf(temp, "%s", (char *) pBinding -> pColName);
      strcat(output, temp);
   }
   return make_const_string(output, strlen(output), result);
}

/*
get_column_headers_array()
create the array with given name attached to the given cursor;
returns the number of columns returned by the statement executed by the given cursor, or -1 if the call failed;
Usage from gawk;
   if (!(nb_cols = get_column_headers__array(my_cursor, columns))) {
      print "nb_columns:", columns["nb_cols"];
      for (i = 1; i <= nb_cols; i++)
         printf("%5d: %50s, %d\n", i, columns["col_names"][i], columns["col_widths"][i], columns["bis_char"][i])
   }
   else print "error while creating the array columns attached to cursor:", my_cursor
*/
awk_value_t *get_column_headers_array(int nargs, awk_value_t *result, struct awk_ext_func *unused) {
   awk_value_t hCursor;
   unsigned int cursor_index;
   awk_value_t array_name;
   awk_value_t array_param;

   assert(result != NULL);

   if (get_argument(0, AWK_NUMBER, &hCursor)) {
      cursor_index = hCursor.num_value;
      if (! get_argument(1, AWK_ARRAY, &array_param)) {
         fprintf(stderr, "missing array parameter\n");
         return make_number(-1, result);
      }
   }
   else {
     fprintf(stderr, "missing cursor handle\n");
     return make_number(-1, result);
   }

   awk_value_t value;
   awk_array_t col_array;
   col_array = array_param.array_cookie;
   clear_array(col_array);

   awk_value_t index;
   make_const_string("nb_cols", 7, &index);
   make_number(odbc_cursor_handles[cursor_index].nb_cols, &value);
   if (!set_array_element(col_array, &index, &value)) {
      fprintf(stderr, "error in get_col_array: set_array_element failed with index %s and value %d, expected [%d]\n",
                      index.str_value.str, value.num_value, odbc_cursor_handles[cursor_index].nb_cols);
      return make_number(-1, result);
   }

   awk_array_t subarray_names = create_array();
   make_const_string("col_names", 9, &index);
   value.val_type = AWK_ARRAY;
   value.array_cookie = subarray_names;
   if (!set_array_element(col_array, &index, &value)) {
      fprintf(stderr, "error in get_col_array: set_array_element for col_names failed\n");
      return make_number(-1, result);
   }
   subarray_names = value.array_cookie;

   awk_array_t subarray_widths = create_array();
   make_const_string("col_widths", 10, &index);
   value.val_type = AWK_ARRAY;
   value.array_cookie = subarray_widths;
   if (!set_array_element(col_array, &index, &value)) {
      fprintf(stderr, "error in get_col_array: set_array_element for col_widths failed\n");
      return make_number(-1, result);
   }
   subarray_widths = value.array_cookie;

   awk_array_t subarray_types = create_array();
   make_const_string("bis_char", 8, &index);
   value.val_type = AWK_ARRAY;
   value.array_cookie = subarray_types;
   if (!set_array_element(col_array, &index, &value)) {
      fprintf(stderr, "error in get_col_array: set_array_element for bis_char failed\n");
      return make_number(-1, result);
   }
   subarray_types = value.array_cookie;

   // indexes of array_name["col_names"], array_name["col_widths"] and array_name["bis_char"] are 1-based;
   unsigned int iCol = 0;
   for(BINDING *pBinding = odbc_cursor_handles[cursor_index].pBinding; pBinding; pBinding = pBinding -> sNext) {
      ++iCol;
      make_number(iCol, &index);
      make_const_string(pBinding -> pColName, strlen(pBinding -> pColName), &value);
      if (!set_array_element(subarray_names, &index, &value)) {
         fprintf(stderr, "error in get_col_array: set_array_element failed in subarray_names with index %d and value %s\n",
                         index.num_value, value.str_value.str);
         return make_number(-1, result);
      }

      make_number(pBinding -> cDisplaySize, &value);
      if (!set_array_element(subarray_widths, &index, &value)) {
         fprintf(stderr, "error in get_col_array: set_array_element failed in subarray_widths with index %d and value %d\n",
                         index.num_value, value.num_value);
         return make_number(-1, result);
      }

      make_number(pBinding -> fChar, &value);
      if (!set_array_element(subarray_types, &index, &value)) {
         fprintf(stderr, "error in get_col_array: set_array_element in subarray_types failed with index %d and value %d\n",
                         index.num_value, value.num_value);
         return make_number(-1, result);
      }
   }
   return make_number(iCol, result);
}

/*
get_col_headers()
populates the list of column headers;
automatically called by ODBC_execute();
*/
static void get_col_headers(unsigned int hCursor) {
   WCHAR       wszTitle[DISPLAY_MAX];
   SQLSMALLINT iCol = 0;
   SQLSMALLINT attributeLen;

   odbc_cursor_handles[hCursor].header_length = 0;
   for (BINDING *pBinding = odbc_cursor_handles[hCursor].pBinding; pBinding; pBinding = pBinding->sNext) {
      TRYODBC(odbc_cursor_handles[hCursor].hStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(odbc_cursor_handles[hCursor].hStmt,
                              iCol + 1,
                              SQL_DESC_NAME,
                              wszTitle,
                              sizeof(wszTitle), // Note count of bytes!
                              &attributeLen,
                              NULL
                             )
             );
      pBinding -> pColName = wcsdup(wszTitle);
      iCol++;
      odbc_cursor_handles[hCursor].header_length += attributeLen;
   }
   odbc_cursor_handles[hCursor].nb_cols = iCol;
Exit:
   return;
}

/*
close_cursor()
frees up the given cursor's used resources;
*/
static void close_cursor(unsigned int hCursor) {
   TRYODBC(odbc_cursor_handles[hCursor].hStmt,
           SQL_HANDLE_STMT,
           SQLFreeStmt(odbc_cursor_handles[hCursor].hStmt, SQL_CLOSE));
   if (odbc_cursor_handles[hCursor].hStmt)
      TRYODBC(odbc_cursor_handles[hCursor].hStmt,
              SQL_HANDLE_STMT,
              SQLFreeHandle(SQL_HANDLE_STMT, odbc_cursor_handles[hCursor].hStmt));
   BINDING *pNext;
   for (BINDING *pCur = odbc_cursor_handles[hCursor].pBinding; pCur; pCur = pNext) {
      pNext = pCur->sNext;
      free(pCur->wszBuffer);
      free(pCur->pColName);
      free(pCur);
   }
   odbc_cursor_handles[hCursor] = ((struct CONN_STMT_HANDLE) {NULL, NULL, NULL, 0, 0, 0});
   nb_odbc_cursor_free_handles++;
Exit:
   return;
}

/*
disconnect()
frees up the given connection's used resources;
the caller is responsible to make sure that the connection is no longer used by live cursors;
*/
static void disconnect(unsigned int hConnection) {
   if (odbc_connection_handles[hConnection]) {
      SQLDisconnect(odbc_connection_handles[hConnection]);
      SQLFreeHandle(SQL_HANDLE_DBC, odbc_connection_handles[hConnection]);
   }
   odbc_connection_handles[hConnection] = NULL;
   nb_odbc_connection_free_handles++;
}

/*
AllocateBindings()
get column information and allocate bindings for each column;
Parameters:
   hStmt       Statement handle
   cCols       Number of columns in the result set
   *lppBinding Binding pointer (returned)
*/
static void AllocateBindings(HSTMT       hStmt,
                             SQLSMALLINT cCols,
                             BINDING     **ppBinding,
                             unsigned int *row_length) {
   SQLSMALLINT     iCol;
   BINDING         *pThisBinding, *pLastBinding = NULL;
   SQLLEN          cchDisplay, ssType;
   SQLSMALLINT     cchColumnNameLength;

   *row_length = 0;
   for (iCol = 1; iCol <= cCols; iCol++) {
      pThisBinding = (BINDING *) (malloc(sizeof(BINDING)));
      if (!pThisBinding) {
         fprintf(stderr, "Out of memory in AllocateBindings for column %d\n", iCol);
         exit(-100);
      }

      if (iCol == 1)
         *ppBinding = pThisBinding;
      else
         pLastBinding->sNext = pThisBinding;
      pLastBinding = pThisBinding;

      // figure out the display length of the column (we will
      // bind to char since we are only displaying data, in general
      // you should bind to the appropriate C type if you are going
      // to manipulate data since it is much faster...)
      TRYODBC(hStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(hStmt,
                              iCol,
                              SQL_DESC_DISPLAY_SIZE,
                              NULL,
                              0,
                              NULL,
                              &cchDisplay)
             );

      // figure out if this is a character or numeric column; this is
      // used to determine if we want to display the data left- or right-
      // aligned.

      // SQL_DESC_CONCISE_TYPE maps to the 1.x SQL_COLUMN_TYPE.
      // This is what you must use if you want to work
      // against a 2.x driver.
      TRYODBC(hStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(hStmt,
                              iCol,
                              SQL_DESC_CONCISE_TYPE,
                              NULL,
                              0,
                              NULL,
                              &ssType
                             )
             );

      pThisBinding->fChar = (ssType == SQL_CHAR ||
                             ssType == SQL_VARCHAR ||
                             ssType == SQL_LONGVARCHAR);

      pThisBinding->sNext = NULL;

      // allocate a buffer big enough to hold the text representation
      // of the data.  Add one character for the null terminator
      if (NULL == (pThisBinding->wszBuffer = (WCHAR *) malloc((cchDisplay + 1) * sizeof(WCHAR)))) {
         fprintf(stderr, "Out of memory in AllocateBindings, could not allocate %d characters, i.e. %d bytes\n", cchDisplay + 1, (cchDisplay + 1) * sizeof(WCHAR));
         exit(-100);
      }

      // Map this buffer to the driver's buffer.   At Fetch time,
      // the driver will fill in this data.  Note that the size is
      // count of bytes (for Unicode).  All ODBC functions that take
      // SQLPOINTER use count of bytes; all functions that take only
      // strings use count of characters.
      TRYODBC(hStmt,
              SQL_HANDLE_STMT,
              SQLBindCol(hStmt,
                         iCol,
                         SQL_C_TCHAR,
                         (SQLPOINTER) pThisBinding->wszBuffer,
                         (cchDisplay + 1) * sizeof(WCHAR),
                         &pThisBinding->indPtr
                        )
             );

      // now set the display size that we will use to display
      // the data.   Figure out the length of the column name
      TRYODBC(hStmt,
              SQL_HANDLE_STMT,
              SQLColAttribute(hStmt,
                              iCol,
                              SQL_DESC_NAME,
                              NULL,
                              0,
                              &cchColumnNameLength,
                              NULL
                             )
             );

      // allow enough space for the column header too;
      // the final column width is the largest of the maximum data length and the header length;
      pThisBinding->cDisplaySize = (SQLSMALLINT)cchDisplay >= cchColumnNameLength ? (SQLSMALLINT)cchDisplay : cchColumnNameLength;
   }
   return;
Exit:
   return;
}

/*
HandleDiagnosticRecord()
display error/warning information;
Parameters:
     hHandle ODBC handle
     hType   Type of handle (HANDLE_STMT, HANDLE_ENV, HANDLE_DBC)
     retCode Return code of failing command
*/
static void HandleDiagnosticRecord(SQLHANDLE   hHandle,
                                   SQLSMALLINT hType,
                                   RETCODE     retCode) {
   SQLSMALLINT iRec = 0;
   SQLINTEGER  iError;
   const unsigned int MAX_MESSAGE_LENGTH = 1000;
   SQLCHAR       wszMessage[MAX_MESSAGE_LENGTH];
   SQLCHAR       wszState[SQL_SQLSTATE_SIZE+1];
   SQLSMALLINT textLengthPtr;

   if (retCode == SQL_INVALID_HANDLE) {
      fprintf(stderr, "Invalid handle!\n");
      return;
   }

   char output[MAX_MESSAGE_LENGTH];
   while(SQLGetDiagRec(hType,
                       hHandle,
                       ++iRec,
                       wszState,
                       &iError,
                       wszMessage,
                       MAX_MESSAGE_LENGTH,
                       &textLengthPtr) == SQL_SUCCESS) {
      if (strncmp((char *) wszState, "01004", 5)) {
         if (MAX_MESSAGE_LENGTH < textLengthPtr) {
            // if the error message is larger then available buffer, temporarily use a new one;
            SQLCHAR wszMessage[textLengthPtr + 1];
            SQLGetDiagRec(hType,
                          hHandle,
                          iRec,
                          wszState,
                          &iError,
                          wszMessage,
                          MAX_MESSAGE_LENGTH,
                          &textLengthPtr
                         );
            sprintf(output, "[%s]%s\nnative return code: %d\n", wszState, wszMessage, iError);
            say(output);
         }
         else {
            sprintf(output, "[%s]%s\nnative return code: %d\n", wszState, wszMessage, iError);
            say(output);
         }
      }
   }
}

static void say(const char *text) {
   if (bDebug)
      fprintf(stderr, "%s\n", text);
}

static awk_bool_t (*init_func)(void) = init_odbc;

/* these are the exported functions along with their min and max arities; */
static awk_ext_func_t func_table[] = {
                                        { "ODBC_connect",            ODBC_connect,            3, 3, awk_false, NULL},
                                        { "ODBC_cursor",             ODBC_cursor,             1, 1, awk_false, NULL},
                                        { "ODBC_execute",            ODBC_execute,            2, 2, awk_false, NULL},
                                        { "ODBC_get_column_headers", ODBC_get_column_headers, 2, 1, awk_false, NULL},
                                        { "ODBC_fetch",              ODBC_fetch,              2, 1, awk_false, NULL},
                                        { "ODBC_rewind",             ODBC_rewind,             1, 1, awk_false, NULL},
                                        { "ODBC_close_cursor",       ODBC_close_cursor,       1, 1, awk_false, NULL},
                                        { "ODBC_disconnect",         ODBC_disconnect,         0, 0, awk_false, NULL}
                                     };

/* define the dl_load function using the boilerplate macro */
dl_load_func(func_table, odbc, "")

