#include <stdio.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <errno.h>
#include <limits.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <regex.h>
#include <wchar.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <sql.h>

#include "gawkapi.h"

void main(unsigned int arc, char *argv[]) {
   printf("sizeof size_t: %d\n", sizeof(size_t));
   printf("sizeof WCHAR: %d\n", sizeof(WCHAR));
   printf("sizeof wchar_t: %d\n", sizeof(wchar_t));
   printf("sizeof char: %d\n", sizeof(char));
   printf("sizeof int: %d\n", sizeof(int));
   printf("sizeof short int: %d\n", sizeof(int));
   printf("sizeof short unsigned int: %d\n", sizeof(short unsigned int));

   printf("cast: %d\n", (int) 12.78345);
   printf("lf: %0.0lf\n", 12.78345);
   printf("f: %.0f\n", 12.78345);
   printf("g: %g\n", 12.78345);
   printf("g: %.0g\n", 12.78345);
   printf("%.0f\n", 12.0);
   printf("%.0f\n", 12);

   WCHAR *sw="hello";
   wchar_t *swt="hello";
   //printf("length of sw: %d\n", strlen(sw));
   //printf("length of sw: %d\n", strlen(swt));
   //printf("length of sw: %d\n", wcslen(sw));
   //printf("length of sw: %d\n", wcslen(swt));
   printf("%s\n\n\n", sw);
   //printf("%s\n", swt);
}

