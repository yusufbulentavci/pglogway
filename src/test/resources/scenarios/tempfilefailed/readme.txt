2022-07-27 12:13:54.450 +03,"postgres","tdbs",1478812,"[local]",62e10176.16909c,6,"idle",2022-07-27 12:12:22 +03,5/21740379,0,LOG,00000,"statement: select * from tdbs.gerceklesme where tarih > (now() - '1 year 4 months'::interval) order by olusturma_tarihi;",,,,,,,,"exec_simple_query, postgres.c:1045","psql"
2022-07-27 12:14:00.362 +03,"postgres","tdbs",1478812,"[local]",62e10176.16909c,7,"SELECT",2022-07-27 12:12:22 +03,5/21740379,0,LOG,00000,"temporary file: path ""base/pgsql_tmp/pgsql_tmp1478812.1"", size 106610688",,,,,,"select * from tdbs.gerceklesme where tarih > (now() - '1 year 4 months'::interval) order by olusturma_tarihi;",,"ReportTemporaryFileUsage, fd.c:1287","psql"
2022-07-27 12:14:00.362 +03,"postgres","tdbs",1478812,"[local]",62e10176.16909c,8,"SELECT",2022-07-27 12:12:22 +03,5/0,0,LOG,00000,"duration: 5912.916 ms",,,,,,,,"exec_simple_query, postgres.c:1289","psql"



2022-07-22 16:21:00.362 +03,"postgres","tdbs",1184909,"[local]",62daa3f2.12148d,35,"idle",2022-07-22 16:19:46 +03,32/5940756,0,LOG,00000,"statement: select * from tdbs.gerceklesme order by olusturma_tarihi;",,,,,,,,"exec_simple_query, postgres.c:1045","psql"
2022-07-22 16:22:10.688 +03,"postgres","tdbs",1184909,"[local]",62daa3f2.12148d,36,"SELECT",2022-07-22 16:19:46 +03,32/5940756,0,LOG,08006,"could not send data to client: Broken pipe",,,,,,"select * from tdbs.gerceklesme order by olusturma_tarihi;",,"internal_flush, pqcomm.c:1462","psql"
2022-07-22 16:22:10.688 +03,"postgres","tdbs",1184909,"[local]",62daa3f2.12148d,37,"SELECT",2022-07-22 16:19:46 +03,32/5940756,0,FATAL,08006,"connection to client lost",,,,,,"select * from tdbs.gerceklesme order by olusturma_tarihi;",,"ProcessInterrupts, postgres.c:3033","psql"
2022-07-22 16:22:10.723 +03,"postgres","tdbs",1184909,"[local]",62daa3f2.12148d,38,"SELECT",2022-07-22 16:19:46 +03,32/0,0,LOG,00000,"temporary file: path ""base/pgsql_tmp/pgsql_tmp1184909.0"", size 1073741824",,,,,,,,"ReportTemporaryFileUsage, fd.c:1287","psql"
2022-07-22 16:22:10.728 +03,"postgres","tdbs",1184909,"[local]",62daa3f2.12148d,39,"SELECT",2022-07-22 16:19:46 +03,32/0,0,LOG,00000,"temporary file: path ""base/pgsql_tmp/pgsql_tmp1184909.1"", size 43212800",,,,,,,,"ReportTemporaryFileUsage, fd.c:1287","psql"
2022-07-22 16:24:17.512 +03,,,1185079,,62daa4b0.121537,1,,2022-07-22 16:22:56 +03,32/5940792,0,FATAL,57P01,"terminating connection due to administrator command",,,,,,"select * from tdbs.gerceklesme order by olusturma_tarihi;",,"ProcessInterrupts, postgres.c:3023","psql"
2022-07-22 16:24:17.551 +03,,,1185079,,62daa4b0.121537,2,,2022-07-22 16:22:56 +03,32/0,0,LOG,00000,"temporary file: path ""base/pgsql_tmp/pgsql_tmp1185079.0"", size 1051099136",,,,,,,,"ReportTemporaryFileUsage, fd.c:1287","psql"


