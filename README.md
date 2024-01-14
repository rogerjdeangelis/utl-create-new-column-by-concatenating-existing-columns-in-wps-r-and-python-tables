# utl-create-new-column-by-concatenating-existing-columns-in-wps-r-and-python-tables
    %let pgm=utl-create-new-column-by-concatenating-existing-columns-in-wps-r-and-python-tables;

    Create new column by concatenating existing columns in wps r and python tables;

        SIX SOLUTIONS

          1  wps sql
          2  wps r sql
          3  wps python sql
          4  wps r loop
          5  wps r mutate
          6  wps python
          7  wps r base
             Gerald Cheves gic2@tc.columbia.edu

    github
    http://tinyurl.com/258r2m3f
    https://github.com/rogerjdeangelis/utl-create-new-column-by-concatenating-existing-columns-in-wps-r-and-python-tables

    Related repos

    http://tinyurl.com/ywdyh7j4
    https://github.com/rogerjdeangelis/utl-translating-converting-wps-datastep-and-sql-code-code-to-r

    http://tinyurl.com/526bbdwf
    https://github.com/rogerjdeangelis/utl-converting-common-wps-coding-to-r-and-python


    /**************************************************************************************************************************/
    /*                              |                                         |                                               */
    /*                              |                                         |                                               */
    /*       INPUT                  |          PROCESSES                      |              OUTPUT                           */
    /*                              |                                         |           CREATE NEW COLUMN                   */
    /*                              |                                         |                                               */
    /*                              |                                         |                                               */
    /* table SD1.HAVE total obs=5   |    SIX SOLUTIONS                        |                                 ADDED         */
    /*                              |                                         |                                 COLUMN        */
    /* OBS    COL1    COL2    COL3  |    WPS, R & Python sql                  |  OBS    COL1    COL2    COL3     DFS          */
    /*                              |    -------------------                  |                                               */
    /*  1      a       f       k    |    select                               |   1      a       f       k      a f k         */
    /*  2      b       g       l    |      *                                  |   2      b       g       l      b g l         */
    /*  3      c       h       m    |     ,col1||' '||col2||' '||col3 as dfs  |   3      c       h       m      c h m         */
    /*  4      d       i       n    |    from                                 |   4      d       i       n      d i n         */
    /*  5      e       j       o    |      have                               |   5      e       j       o      e j o         */
    /*                              |                                         |                                               */
    /*                              |    WPS R LOOP                           |                                               */
    /*                              |    ---------                            |                                               */
    /*                              |    have$DFS<-NaN;                       |                                               */
    /*                              |    for (i in 1:nrow(have)) {;           |                                               */
    /*                              |       have[i,4]<-paste(have[i,1],       |                                               */
    /*                              |           have[i,2], have[i,3]);        |                                               */
    /*                              |       };                                |                                               */
    /*                              |                                         |                                               */
    /*                              |    WPS R MUTATE                         |                                               */
    /*                              |    ------------                         |                                               */
    /*                              |    want<-have %>%                       |                                               */
    /*                              |      mutate(DFS=paste(COL1,COL2,COL3)); |                                               */
    /*                              |                                         |                                               */
    /*                              |    WPS Python                           |                                               */
    /*                              |    ----------                           |                                               */
    /*                              |    have['DFS'] =                        |                                               */
    /*                              |      have['COL1'] +'  + have['COL2']  + |                                               */
    /*                              |      + ' '  + have['COL3']              |                                               */
    /*                              |                                         |                                               */
    /*                              |    WPS R BASE                           |                                               */
    /*                              |    ==========                           |                                               */
    /*                              |    have$DFS<-do.call(paste,    |        |                                               */
    /*                              |      have[c("COL1","COL2","COL3")]);    |                                               */
    /*                              |                                         |                                               */
    /*                              |                                         |                                               */
    /**************************************************************************************************************************/

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */
    options validvarname=upcase;
    libname sd1 "d:/sd1";
    data sd1.have;
     informat col1-col3 $1.;
     input col1-col3;
    cards4;
    a f k
    b g l
    c h m
    d i n
    e j o
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.HAVE total obs=5                                                                                                   */
    /*                                                                                                                        */
    /* Obs    COL1    COL2    COL3                                                                                            */
    /*                                                                                                                        */
    /*  1      a       f       k                                                                                              */
    /*  2      b       g       l                                                                                              */
    /*  3      c       h       m                                                                                              */
    /*  4      d       i       n                                                                                              */
    /*  5      e       j       o                                                                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*                                  _
    / | __      ___ __  ___   ___  __ _| |
    | | \ \ /\ / / `_ \/ __| / __|/ _` | |
    | |  \ V  V /| |_) \__ \ \__ \ (_| | |
    |_|   \_/\_/ | .__/|___/ |___/\__, |_|
                 |_|                 |_|
    */

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    options validvarname=any;
    proc sql;
      create
        table sd1.want as
      select
        *
       ,catx(' ',col1,col2,col3) as dfs
      from
        sd1.have
    ;quit;
    proc print data=sd1.want;
    run;quit;
    ");


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS System                                                                                                         */
    /*                                                                                                                        */
    /* Obs    COL1    COL2    COL3     dfs                                                                                    */
    /*                                                                                                                        */
    /*  1      a       f       k      a f k                                                                                   */
    /*  2      b       g       l      b g l                                                                                   */
    /*  3      c       h       m      c h m                                                                                   */
    /*  4      d       i       n      d i n                                                                                   */
    /*  5      e       j       o      e j o                                                                                   */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                          _
    |___ \  __      ___ __  ___   _ __   ___  __ _| |
      __) | \ \ /\ / / `_ \/ __| | `__| / __|/ _` | |
     / __/   \ V  V /| |_) \__ \ | |    \__ \ (_| | |
    |_____|   \_/\_/ | .__/|___/ |_|    |___/\__, |_|
                     |_|                        |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    options validvarname=any;
    libname sd1 "d:/sd1";

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(sqldf);
    want<-sqldf("
      select
        *
       ,col1||` `||col2||` `||col3 as dfs
      from
        have
    ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* SD1.HAVE total obs=5 13MAY2022:08:52:34                                                                                */
    /*                                                                                                                        */
    /* Obs    COL1    COL2    COL3                                                                                            */
    /*                                                                                                                        */
    /*  1      a       f       k                                                                                              */
    /*  2      b       g       l                                                                                              */
    /*  3      c       h       m                                                                                              */
    /*  4      d       i       n                                                                                              */
    /*  5      e       j       o                                                                                              */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                    _   _                             _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __    ___  __ _| |
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \  / __|/ _` | |
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | | \__ \ (_| | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_| |___/\__, |_|
                     |_|         |_|    |___/                                |_|
    */


    %utlfkil(d:/xpt/want.xpt);

    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;
    proc datasets lib=work nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    options validvarname=any lrecl=32756;
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.have  python=have;
    submit;
    import pyreadstat as ps;
    from os import path;
    import pandas as pd;
    import numpy as np;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    want = pdsql('''
      select
        *
       ,col1||` `||col2||` `||col3 as dfs
      from
        have
    ''');
    print(want);
    ps.write_xport(want,'d:\\xpt\\want.xpt',table_name='want',file_format_version=5
    ,column_labels=['COL1','COL2','COL3','DFS']);
    endsubmit;
    ");

    /*--- handles long variable names by using the label to rename the variables  ----*/

    libname xpt xport "d:/xpt/want.xpt";
    proc contents data=xpt._all_;
    run;quit;

    data want_py_long_names;
      %utl_rens(xpt.want) ;
      set want;
    run;quit;
    libname xpt clear;

    proc print data=want_py_long_names;
    run;quit;


    /**************************************************************************************************************************/
    /*                           |                                                                                            */
    /*  The WPS Python System    |   WPS System                                                                               */
    /*                           |                                                                                            */
    /*  The PYTHON Procedure     |                                                                                            */
    /*                           |                                                                                            */
    /*    COL1 COL2 COL3    DFS  |    COL1    COL2    COL3     DFS                                                            */
    /*                           |                                                                                            */
    /*  0    a    f    k  a f k  |     a       f       k      a f k                                                           */
    /*  1    b    g    l  b g l  |     b       g       l      b g l                                                           */
    /*  2    c    h    m  c h m  |     c       h       m      c h m                                                           */
    /*  3    d    i    n  d i n  |     d       i       n      d i n                                                           */
    /*  4    e    j    o  e j o  |     e       j       o      e j o                                                           */
    /*                           |                                                                                            */
    /**************************************************************************************************************************/

    /*  _                                 _
    | || |   __      ___ __  ___   _ __  | | ___   ___  _ __
    | || |_  \ \ /\ / / `_ \/ __| | `__| | |/ _ \ / _ \| `_ \
    |__   _|  \ V  V /| |_) \__ \ | |    | | (_) | (_) | |_) |
       |_|     \_/\_/ | .__/|___/ |_|    |_|\___/ \___/| .__/
                      |_|                              |_|
    */
    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    have$DFS<-NaN;
    for (i in 1:nrow(have)) {;
       have[i,4]<-paste(have[i,1],
           have[i,2], have[i,3]);
       };
    have;
    endsubmit;
    import data=sd1.want r=have;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS R System                                                                                                       */
    /*                                                                                                                        */
    /*   COL1 COL2 COL3   DFS                                                                                                 */
    /* 1    a    f    k a f k                                                                                                 */
    /* 2    b    g    l b g l                                                                                                 */
    /* 3    c    h    m c h m                                                                                                 */
    /* 4    d    i    n d i n                                                                                                 */
    /* 5    e    j    o e j o                                                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                                _        _
    | ___|  __      ___ __  ___   _ __   _ __ ___  _   _| |_ __ _| |_ ___
    |___ \  \ \ /\ / / `_ \/ __| | `__| | `_ ` _ \| | | | __/ _` | __/ _ \
     ___) |  \ V  V /| |_) \__ \ | |    | | | | | | |_| | || (_| | ||  __/
    |____/    \_/\_/ | .__/|___/ |_|    |_| |_| |_|\__,_|\__\__,_|\__\___|
                     |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(dplyr);
    have$DFS<-NaN;
    want<-have %>%
        mutate(DFS=paste(COL1,COL2,COL3));
    want;;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;


    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS R System                                                                                                       */
    /*                                                                                                                        */
    /*   COL1 COL2 COL3   DFS                                                                                                 */
    /* 1    a    f    k a f k                                                                                                 */
    /* 2    b    g    l b g l                                                                                                 */
    /* 3    c    h    m c h m                                                                                                 */
    /* 4    d    i    n d i n                                                                                                 */
    /* 5    e    j    o e j o                                                                                                 */
    /*                                                                                                                        */
    /**************************************************************************************************************************/


    proc datasets lib=sd1 nolist mt=data mt=view nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    proc python;
    export data=sd1.have  python=have;
    submit;
    import numpy as np;
    import pandas as pd;
    have.info();
    print(have);
    have['DFS'] = have['COL1'] + ' ' + have['COL2'] + ' '  + have['COL3'];
    print(have);
    endsubmit;
    ");

     /**************************************************************************************************************************/
     /*                                                                                                                        */
     /*   COL1 COL2 COL3    DFS                                                                                                */
     /*                                                                                                                        */
     /* 0    a    f    k  a f k                                                                                                */
     /* 1    b    g    l  b g l                                                                                                */
     /* 2    c    h    m  c h m                                                                                                */
     /* 3    d    i    n  d i n                                                                                                */
     /* 4    e    j    o  e j o                                                                                                */
     /*                                                                                                                        */
     /**************************************************************************************************************************/


    /*____                               _
    |___  | __      ___ __  ___   _ __  | |__   __ _ ___  ___
       / /  \ \ /\ / / `_ \/ __| | `__| | `_ \ / _` / __|/ _ \
      / /    \ V  V /| |_) \__ \ | |    | |_) | (_| \__ \  __/
     /_/      \_/\_/ | .__/|___/ |_|    |_.__/ \__,_|___/\___|
                     |_|
    */


    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    have$DFS<-do.call(paste, have[c("COL1","COL2","COL3")]);
    have;
    endsubmit;
    import data=sd1.want r=have;
    run;quit;
    ');

    proc print data=sd1.want width=min;
    run;quit;

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The WPS R System                                                                                                       */
    /*                                                                                                                        */
    /*   COL1 COL2 COL3     DFS                                                                                               */
    /* 1    a    f    k   a f k                                                                                               */
    /* 2    b    g    l   b g l                                                                                               */
    /* 3    c    h    m   c h m                                                                                               */
    /* 4    d    i    n   d i n                                                                                               */
    /* 5    e    j    o   e j o                                                                                               */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
