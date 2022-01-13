# test program for the odbc interface for gawk;
# cec@dbi-services, September 2021;
# the idea behind this script is to use it interactively and try successful and erroneous calls to odbc to see how it behaves;
# e.g.: instead of update ..., try an xupdate ... and check the returned status;

# load the ODBC interface extension;
@load "odbc"

BEGIN {
   target_names = sprintf("%s %s %s %s %s %s %s %s %s",
                          "mysqlitedb::",
                          "mypostgresqldb::",               # rewind() fails;
                          "mysqldb:debian:debian",
                          "myfbdb::", # "myfbdb_Devart::",
                          "OracleODBC-21::",                # rewind() fails;
                          "mymssqlserverdb:SA:admin2021!",
                          "myhsqldb::",                     # rewind() fails;
                          "mymongodb_Devart::",             # joins unsupported;
                          "CData-Excel-Source::")           # rewind() fails; spreadsheet file deleted when modified;
   #target_names = "myfbdb::"
   #target_names = "myfbdb_Devart::"
   nb_targets = split(target_names, targets, " ") 

   table_names = "regions countries locations departments jobs employees dependents"
   #table_names = "countries"
   nb_tables = split(table_names, tables, " ") 

   for (target = 1; target <= nb_targets; target++) {
      split(targets[target], infos, ":")
      print "Testing " infos[1]
      if (-1 == (connection = ODBC_connect(infos[1], infos[2], infos[3]))) {
         print "skipping connection to", infos[1]
         continue
      }
      for (t = 1; t <= nb_tables; t++)
         do_test(infos[1], connection, "select * from " tables[t] " order by 1")

      join_stmt = "SELECT c.country_name, c.country_id, l.country_id, l.street_address, l.city FROM countries c " \
                  "LEFT JOIN locations l ON l.country_id = c.country_id WHERE c.country_id IN ('US', 'UK', 'CN')"
      do_test(infos[1], connection, join_stmt)

      # test accentuated characters;
      # ignore the constraint violation errors after first successful execution;
      stmt = "INSERT INTO countries(country_id,country_name,region_id) VALUES ('RU','Россия',1)"
      do_test(infos[1], connection, stmt)

      stmt = "INSERT INTO countries(country_id,country_name,region_id) VALUES ('BI','Biélorussie',1)"
      do_test(infos[1], connection, stmt)

      stmt = "INSERT INTO countries(country_id,country_name,region_id) VALUES ('BH','Bahreïn',4)"
      do_test(infos[1], connection, stmt)

      stmt = "INSERT INTO countries(country_id,country_name,region_id) VALUES ('IS','Îles Salomon',2)"
      do_test(infos[1], connection, stmt)

      status = ODBC_disconnect(connection)
   }
}

function do_test(target_name, connection, stmt   , cursor, status, nb_cols, my_columns, max_width, i, line_str, headers_str, 
                                                   headers, max_col_widths, row_count, row, col_data, col_header_str, col_name) {
   printf "\n"
   print "target_name", target_name
   cursor = ODBC_cursor(connection)
   if (-1 == cursor) {
      print "could not get a cursor, exiting ..."
      return
   }

   print "executing [" stmt "]"
   status = ODBC_execute(cursor, stmt)

   if (0 == status) {
      print "no expected data"
      return
   }
   else if (-1 == status) {
      print "execution error"
      return
   }

   # data raw printing;
   # print the headers and initialize the header information used throughout the tests;
   printf "\n"
   print "raw printing the data from the SUBSEP string" 
   headers_str = ODBC_get_column_headers(cursor)
   nb_cols = split(headers_str, headers, SUBSEP)
   printf("%5s", "#")
   for (i = 1; i <= nb_cols; i++) {
      gsub(/ +$/, "", headers[i])
      printf(1 == my_columns["bis_char"][i] ? "  %-*s" : "  %*s", my_columns["col_widths"][i], headers[i])
      max_col_widths[i] = length(headers[i])           # to find the optimal (i.e. narrowest) width for this column;
   }
   printf "\n"
   # print the data;
   row_count = 0
   while ((row = ODBC_fetch(cursor)) > -1) {
      row_count++
      printf("%5d", row_count)
      nb_cols = split(row, col_data, SUBSEP)
      for (i = 1; i <= nb_cols; i++) {
         gsub(/ +$/, "", col_data[i])
         printf(1 == my_columns["bis_char"][i] ? "  %-*s" : "  %*s", my_columns["col_widths"][i], col_data[i])
         if (length(col_data[i]) > max_col_widths[i])  # column width must accomodate the data AND the header; 
            max_col_widths[i] = length(col_data[i])
      }
      printf "\n"
   }

   # print the column headers in a nice table;
   if ((nb_cols = ODBC_get_column_headers(cursor, my_columns)) > 0) {
      printf "\n"
      max_width = 0
      for (i = 1; i <= nb_cols; i++)
         if (length(my_columns["col_names"][i]) > max_width)
            max_width = length(my_columns["col_names"][i])
      line_str = ""
      line_str = line_str sprintf("+%s+%s+%s+%s+", repeat_char("-", 5), repeat_char("-", max_width), repeat_char("-", 6), repeat_char("-", 8))
      print line_str
      printf("|%5s|%-*s|%6s|%8s|\n", "#", max_width, "col names", "widths", "is char?")
      print line_str
      for (i = 1; i <= nb_cols; i++)
         printf("|%5d|%-*s|%6d|%8d|\n", i, max_width, my_columns["col_names"][i], my_columns["col_widths"][i], my_columns["bis_char"][i])
      print line_str
   }
   else {
      if (-1 == nb_cols)
         print "error while getting the column dimensions for cursor:", cursor
      ODBC_close_cursor(cursor)
      return
   }

   # pretty printing the data;
   # build the header string and print it;
   printf "\n"
   print "pretty printing the data from the SUBSEP string" 
   line_str = "+" repeat_char("-", 5)
   for (i = 1; i <= nb_cols; i++)
      line_str = line_str sprintf("+%s", repeat_char("-", max_col_widths[i]))
   line_str = line_str "+"
   print line_str
   col_header_str = sprintf("+%5s", "#")
   for (i = 1; i <= nb_cols; i++)
      col_header_str = col_header_str sprintf(1 == my_columns["bis_char"][i] ? "|%-*s" : "|%*s", max_col_widths[i], headers[i])
   col_header_str = col_header_str "|"
   print col_header_str
   print line_str
   # print the data;
   # as the result set has previously been entirely read, it must be rewound;
   # let' s first test closing and re-executing the same statement;
   #ODBC_close_cursor(cursor)
   #cursor = ODBC_cursor(connection)
   status = ODBC_execute(cursor, stmt)
   row_count = 0
   while ((row = ODBC_fetch(cursor)) > -1) {
      row_count++
      printf("|%5d", row_count)
      nb_cols = split(row, cols, SUBSEP)
      for (i = 1; i <= nb_cols; i++) {
         gsub(/ +$/, "", cols[i])
         printf(1 == my_columns["bis_char"][i] ? "|%-*s" : "|%*s", max_col_widths[i], cols[i])
      }
      printf "|\n"
   }
   print line_str

   # same pretty printing but using the array variant;
   # this time, let's try to rewind the result set in case it is supported;
   printf "\n"
   print "pretty printing the data from the received array with ODBC_rewind()" 
   if (ODBC_rewind(cursor) <= 0)
      print "ODBC_rewind() not supported" 
   else {
      print line_str
      print col_header_str
      print line_str
      row_count = 0
      row_data[0] = 0
      delete row_data
      while (ODBC_fetch(cursor, row_data) > -1) {
         row_count++
         printf("|%5d", row_count)
         nb_cols = row_data[0]
         for (i = 1; i <= nb_cols; i++)
            for (col_name in row_data[i]) {
               gsub(/ +$/, "", col_name)
               printf(1 == my_columns["bis_char"][i] ? "|%-*s" : "|%*s", max_col_widths[i], row_data[i][col_name])
            }
         printf "|\n"
      }
      print line_str
   }
   status = ODBC_close_cursor(cursor)
}

# do I need to explain ?
function repeat_char(ch, n   , s) {
   s = ""
   while (n-- > 0)
      s = s ch
   return s
}

# recursively walks the array with name "name" and prints its nodes;
# used for troublehooting;
function dumparray(name, array, i) {
   for (i in array)
      if (isarray(array[i]))
         dumparray(name "[\"" i "\"]", array[i])
      else
         printf("%s[\"%s\"] = %s\n", name, i, array[i])
}

